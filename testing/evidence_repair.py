"""Collect C6 evidence while an authorized operator runs a second primary-loss scenario."""

import argparse
from pathlib import Path
import re
import sys
import time

from evidence_support import EvidenceError, now, read_jsonl, report, run, secret, utc
from evidence_topology import DIGEST, Probe, checksum, matched


EVENTS = ("node_disk_lost", "old_primary_fenced", "candidate_verified", "new_primary_ready", "guard_rebound")
SYSTEM_ID = re.compile(r"[1-9][0-9]{0,19}")
LSN = re.compile(r"[0-9A-Fa-f]{1,8}/[0-9A-Fa-f]{1,8}")


def wal_position(value):
    if not matched(value, LSN):
        raise EvidenceError("Cutover log contains an invalid WAL position.")
    high, low = value.split("/")
    return (int(high, 16) << 32) | int(low, 16)


def cutover_evidence(path, start, end, old, repaired):
    rows = []
    try:
        for row in read_jsonl(path):
            if len(rows) >= len(EVENTS) or row.get("event") != EVENTS[len(rows)]:
                raise EvidenceError("Unexpected or out-of-order cutover event.")
            event = {key: row[key] for key in ("event", "at", "node", "unit", "timeline", "system_id", "evidence_sha256")}
            if (not matched(event["node"]) or not matched(event["unit"]) or not matched(event["system_id"], SYSTEM_ID)
                    or not matched(event["evidence_sha256"], DIGEST) or type(event["timeline"]) is not int
                    or not 0 < event["timeline"] <= 0xFFFFFFFF):
                raise EvidenceError("Invalid cutover identity or source evidence digest.")
            if event["event"] == "old_primary_fenced":
                event["acknowledged_lsn"] = row["acknowledged_lsn"]
                wal_position(event["acknowledged_lsn"])
            elif event["event"] == "candidate_verified":
                event["replay_lsn"] = row["replay_lsn"]
                wal_position(event["replay_lsn"])
            elif event["event"] == "guard_rebound":
                event.update(previous_epoch=row["previous_epoch"], epoch=row["epoch"])
                if any(type(event[key]) is not int or event[key] < 1 for key in ("previous_epoch", "epoch")):
                    raise EvidenceError("Invalid guard epochs.")
            event["at"] = utc(event["at"]).isoformat()
            rows.append(event)
    except (KeyError, OSError, TypeError, ValueError):
        return False, rows
    if len(rows) != len(EVENTS):
        return False, rows
    lost, fenced, candidate, promoted, guard = rows
    timestamps = [utc(row["at"]) for row in rows]
    valid = (
        utc(start) <= timestamps[0] and timestamps[-1] <= utc(end)
        and all(left < right for left, right in zip(timestamps, timestamps[1:]))
        and all((row["node"], row["unit"]) == old for row in (lost, fenced))
        and all((row["node"], row["unit"]) == repaired for row in (candidate, promoted, guard))
        and len({row["system_id"] for row in rows}) == 1
        and lost["timeline"] == fenced["timeline"] == candidate["timeline"] < promoted["timeline"] == guard["timeline"]
        and 0 < wal_position(fenced["acknowledged_lsn"]) <= wal_position(candidate["replay_lsn"])
        and guard["epoch"] > guard["previous_epoch"]
    )
    return valid, rows


def audit_locations(probe, hazard, timeout):
    sample, body = probe.get("/internal/audit/location", {"hazard_id": hazard}, timeout, audit=True)
    copies, units = body.get("locations"), body.get("unavailable_units")
    valid = (sample["status"] == 200 and sample["elapsed_ms"] <= timeout * 1000 and body.get("hazard_id") == hazard
             and ((body.get("partial") is False and "unavailable_units" not in body)
                  or (body.get("partial") is True and isinstance(units, list) and bool(units)
                      and all(matched(unit) for unit in units) and len(set(units)) == len(units)))
             and isinstance(copies, list) and all(isinstance(copy, dict) and matched(copy.get("node"))
                 and matched(copy.get("unit")) and copy.get("role") in ("primary", "replica") for copy in copies))
    locations = {copy["unit"]: (copy["node"], copy["role"]) for copy in copies} if valid else {}
    if valid and len(locations) != len(copies):
        locations = {}
    sample["locations"] = locations
    return sample, locations


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for option in ("base-url", "audit-token-file", "hazard-id", "old-unit", "repaired-unit", "expected-checksum"):
        parser.add_argument("--" + option, required=True)
    parser.add_argument("--expected-count", required=True, type=int)
    parser.add_argument("--cutover-log", required=True, type=Path, help="fresh JSONL from independent platform and SQL observations")
    parser.add_argument("--watch-seconds", type=int, default=120)
    parser.add_argument("--timeout", type=int, default=5, help="per-request seconds")
    parser.add_argument("--output", required=True, type=Path, help="new JSON evidence file")
    args = parser.parse_args()
    if (not matched(args.old_unit) or not matched(args.repaired_unit) or args.old_unit == args.repaired_unit
            or not 1 <= len(args.hazard_id) <= 2048 or not matched(args.expected_checksum, DIGEST)
            or args.expected_count < 1 or not (1 <= args.timeout <= 30 and 3 * args.timeout + 2 <= args.watch_seconds <= 600)
            or args.output.exists()):
        parser.error("invalid unit, hazard, reference, timeout, watch duration or output")

    started = now()
    probe = Probe(args.base_url, secret(args.audit_token_file), "", 1)
    before, initial = audit_locations(probe, args.hazard_id, args.timeout)
    before_checksum = checksum(probe, args.expected_checksum, args.timeout)
    old_node, old_role = initial.get(args.old_unit, (None, None))
    new_node, new_role = initial.get(args.repaired_unit, (None, None))
    checks = {
        "old_primary_and_repaired_standby_observed": old_role == "primary" and new_role == "replica" and old_node != new_node
        and sum(role == "primary" for _, role in initial.values()) == 1,
        "baseline_matches_reference": before_checksum["matches_expected"] and before_checksum["count"] == args.expected_count,
    }
    observations = {"location_before": before, "checksum_before": before_checksum,
                    "expected_count": args.expected_count, "expected_checksum": args.expected_checksum}
    if not all(checks.values()):
        return report(args.output, "repaired_cutover", started, checks, observations)

    watch_start = now()
    deadline = time.monotonic() + args.watch_seconds
    print(f"WATCH READY at {watch_start}; operator window: {args.watch_seconds}s", file=sys.stderr, flush=True)
    samples = []
    while True:
        sample = checksum(probe, args.expected_checksum, args.timeout, deadline=deadline)
        if sample is None:
            break
        samples.append(sample)
    time.sleep(max(0, deadline - time.monotonic()))
    watch_end = now()
    valid_log, events = cutover_evidence(args.cutover_log, watch_start, watch_end,
                                         (old_node, args.old_unit), (new_node, args.repaired_unit))
    after, final = audit_locations(probe, args.hazard_id, args.timeout)
    after_checksum = checksum(probe, args.expected_checksum, args.timeout)
    checks.update(
        cutover_log_matches_observed_members_and_bounds=valid_log,
        checksums_bounded_and_consistent=bool(samples) and all(
            (sample["valid_partial"] and sample["count"] <= args.expected_count)
            or (sample["matches_expected"] and sample["count"] == args.expected_count)
            for sample in samples),
        loss_window_sampled=valid_log and any(utc(sample["at"]) < utc(events[3]["at"])
            and utc(sample["finished_at"]) >= utc(events[0]["at"]) for sample in samples),
        complete_read_after_guard_rebind=valid_log and any(sample["matches_expected"]
            and sample["count"] == args.expected_count and utc(sample["at"]) >= utc(events[-1]["at"]) for sample in samples),
        repaired_member_observed_as_primary=final.get(args.repaired_unit) == (new_node, "primary")
        and sum(role == "primary" for _, role in final.values()) == 1,
        recovered_checksum_matches_reference=after_checksum["matches_expected"] and after_checksum["count"] == args.expected_count,
    )
    observations.update(watch_start=watch_start, watch_end=watch_end, cutover_events=events,
                        checksum_samples=samples, location_after=after, checksum_after=after_checksum)
    return report(args.output, "repaired_cutover", started, checks, observations)


if __name__ == "__main__":
    raise SystemExit(run(main))
