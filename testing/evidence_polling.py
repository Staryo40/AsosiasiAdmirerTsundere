"""Evaluate bounded P2 polling/fencing evidence without contacting the system."""

import argparse
from collections import defaultdict
import hashlib
import math
from pathlib import Path
import re

from evidence_support import now, read_jsonl, report, run, utc


ENDPOINTS = ("seismic", "warnings", "volcanic")
SCENARIOS = ("death", "skew", "freeze")
FILES = ("faults", "ingress", "guard", "reconciliation")
UTC_STAMP = re.compile(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,6})?(?:Z|\+00:00)", re.ASCII)
FAULT_EVENTS = {
    "death": {"owner_died": {"owner"}},
    "skew": {"clock_shift": {"owner", "node", "delta_seconds"}},
    "freeze": {name: {"owner"} for name in ("frozen", "resumed", "refreshed")},
}
SCHEMA = """\
Supply four immutable UTF-8 JSONL exports from an authorized observation, not
examples or reconstructed successes. Every row has exactly the common fields
scenario (death, skew, or freeze) and observed_at (external observer UTC in
YYYY-MM-DDTHH:MM:SS[.ffffff]Z or +00:00). No guest clock can supply observed_at.
Additional fields by export:

--faults     event: window_start or window_end (no extra fields; each once per
             scenario). Windows must be disjoint and bound complete captures,
             with no dropped, sampled, or filtered polling/SQL events.
             Also include exactly:
             death: owner_died with owner (the active instance confirmed dead).
             skew: clock_shift twice with owner, node and delta_seconds (integer
             +45 then -30; same node and affected instance). These are applied
             clock adjustments, not merely commands or intended offsets.
             freeze: frozen, resumed, refreshed, all with the same active owner.
             refreshed is the first completed authority/status refresh by that
             instance after resuming, not the start of a refresh attempt.
--ingress    endpoint, period, owner, epoch, request_id, decision. Emit one row
             per actual upstream poll request arrival (accepted or rejected),
             with its ingress time, unique arrival request_id (not a reused
             poll correlation ID), correlated owner and positive integer epoch.
             accepted means admitted past the upstream
             request gate, not necessarily a completed response. Retries and
             rejected requests must also be captured. Durable receipt replay
             is not an upstream arrival. Mock completion logs alone are not
             evidence of ingress time or owner/period attribution.
--guard      endpoint, period, owner, epoch, decision. Capture all protected SQL
             mutation outcomes: committed (transaction durably committed),
             stale_rejected (protected SQL guard rejected stale/expired owner
             authority), or rejected (other failure). Use actual SQL decisions,
             not app-side checks or transaction starts. For mutations other
             than storing received poll results, receipts, or checkpoints tied
             to a poll period, use null for both endpoint and period. These
             still count in the freeze check. epoch is the positive integer
             submitted to SQL, not the current epoch looked up afterwards.
--reconciliation  One death row per endpoint: endpoint, before_at,
             checkpoint_before, checkpoint_after, required_checkpoint,
             source_refs, canonical_ids. before_at is the external snapshot
             time before death; observed_at is the external snapshot time
             after recovery. The checkpoint fields are UTC source event-time
             cursors, not observer times. required_checkpoint is the independent
             oracle's recover-through watermark for valid source data in scope.
             Each of source_refs and canonical_ids is an object with before,
             expected_after and observed_after arrays of nonempty strings.
             For a bounded, non-destructive cohort, use actual before/after
             durable snapshots and an independent expected-after list including
             retained identities and all valid input through the required
             checkpoint. expected_after is sorted and unique; do not dedupe
             actual lists or count physical replicas as separate effects.
             Each endpoint must have at least one expected new source reference.
             Reconcile warning source refs separately; their canonical IDs are
             the affected seismic hazards, not synthetic tsunami rows.

endpoint values are seismic (/seismic-events), warnings (/tsunami-warnings),
and volcanic (/volcanic-reports). Owner, node, request_id and period are nonempty
sanitized string IDs. Owner identifies an instance incarnation, not a reusable
name. Period is the declared common logical poll-period ID; use identical IDs
across observers and scenarios for the same real period, including delayed SQL
work. Do not assign periods from a skewed guest clock or regroup by arrival or
commit time. Capture activity before death/freeze and before and after both
skew steps. Timestamps tied to a marker cannot establish strict event order.
The checks cover only the supplied windows, not logger completeness,
authenticity, unobserved windows, downstream delivery, or same-owner retries.
"""


def require(condition):
    if not condition:
        raise ValueError("invalid normalized polling evidence")


def identifier(value):
    return isinstance(value, str) and bool(value.strip())


def stamp(value):
    require(isinstance(value, str) and UTC_STAMP.fullmatch(value))
    return utc(value)


def read_export(path, kind):
    rows = []
    seen_requests = set()
    for row in read_jsonl(path):
        scenario, at = row.pop("scenario"), stamp(row.pop("observed_at"))
        require(scenario in SCENARIOS)
        if kind == "faults":
            event = row["event"]
            fields = set() if event in ("window_start", "window_end") else FAULT_EVENTS[scenario][event]
            require(row.keys() == fields | {"event"})
            if "owner" in fields:
                require(identifier(row["owner"]))
            if event == "clock_shift":
                delta = row["delta_seconds"]
                require(identifier(row["node"]) and type(delta) is int and delta in (45, -30))
                row["event"] = "forward" if delta == 45 else "backward"
        elif kind == "reconciliation":
            checkpoints = ("before_at", "checkpoint_before", "checkpoint_after", "required_checkpoint")
            require(scenario == "death" and row.keys() == set(checkpoints) | {"endpoint", "source_refs", "canonical_ids"})
            require(row["endpoint"] in ENDPOINTS)
            for field in checkpoints:
                row[field] = stamp(row[field])
            for field in ("source_refs", "canonical_ids"):
                versions = row[field]
                require(isinstance(versions, dict) and versions.keys() == {"before", "expected_after", "observed_after"})
                for ids in versions.values():
                    require(isinstance(ids, list) and all(identifier(item) for item in ids))
                expected = versions["expected_after"]
                require(expected == sorted(set(expected)))
        else:
            fields = {"endpoint", "period", "owner", "epoch", "decision"}
            if kind == "ingress":
                fields.add("request_id")
            require(row.keys() == fields)
            endpoint, period, decision = row["endpoint"], row["period"], row["decision"]
            require(identifier(row["owner"]) and type(row["epoch"]) is int and row["epoch"] > 0)
            require((endpoint in ENDPOINTS and identifier(period)) or
                    (kind == "guard" and endpoint is None and period is None))
            if kind == "ingress":
                request = row["request_id"]
                require(decision in ("accepted", "rejected") and identifier(request) and request not in seen_requests)
                seen_requests.add(request)
            else:
                require(decision in ("committed", "stale_rejected", "rejected"))
        rows.append(dict(row, scenario=scenario, at=at))
    return rows


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(64 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def fault_sequence(rows, scenario, *events):
    matching = [row for row in rows if row["scenario"] == scenario]
    markers = {row["event"]: row for row in matching}
    order = ("window_start", *events, "window_end")
    if len(matching) != len(order) or markers.keys() != set(order):
        return None
    ordered = [markers[event] for event in order]
    if not all(left["at"] < right["at"] for left, right in zip(ordered, ordered[1:])):
        return None
    return ordered


def owner_periods(rows):
    owners = {endpoint: defaultdict(set) for endpoint in ENDPOINTS}
    for row in rows:
        if row["endpoint"] is not None:
            owners[row["endpoint"]][row["period"]].add(row["owner"])
    return owners


def successes(rows, start, end, owner=None):
    return [row for row in rows if start < row["at"] < end and (owner is None or row["owner"] == owner)]


def identity_differences(versions):
    before, expected, actual = (versions[field] for field in ("before", "expected_after", "observed_after"))
    prior, oracle, observed = set(before), set(expected), set(actual)
    return {
        "expected_new": len(oracle - prior),
        "missing_from_oracle": len(prior - oracle),
        "missing_after": len(oracle - observed),
        "unexpected_after": len(observed - oracle),
        "duplicate_before": len(before) - len(prior),
        "duplicate_after": len(actual) - len(observed),
    }


def analyze_death(sequence, ingress, guard, reconciliation):
    details = {endpoint: {} for endpoint in ENDPOINTS}
    checks = {"death_active_owner_covered": False, "replacement_ingress_within_60s": False,
              "replacement_commits_covered": False, "checkpoint_recovery_covered": False,
              "canonical_reconciliation_passed": False}
    if not sequence:
        return checks, details
    start, death, end = (marker["at"] for marker in sequence)
    old = sequence[1]["owner"]
    for endpoint, result in details.items():
        receipts, commits = ingress[endpoint], guard[endpoint]
        result["active_owner_covered"] = bool(successes(receipts, start, death, old) and
                                              successes(commits, start, death, old))
        first = min((row["at"] for row in receipts if death <= row["at"] < end and row["owner"] != old), default=None)
        result["replacement_ingress_seconds"] = (first - death).total_seconds() if first is not None else None
        committed = min((row["at"] for row in commits if first is not None and first < row["at"] < end and
                         row["owner"] != old), default=None)
        result["replacement_commit_observed"] = committed is not None
        snapshots = [row for row in reconciliation if row["endpoint"] == endpoint]
        result["reconciliation_records"] = len(snapshots)
        if len(snapshots) != 1:
            continue
        snapshot = snapshots[0]
        result["checkpoint_recovery"] = bool(
            committed is not None and start <= snapshot["before_at"] < death < committed < snapshot["at"] <= end
            and snapshot["checkpoint_before"] < snapshot["required_checkpoint"] <= snapshot["checkpoint_after"]
        )
        source = identity_differences(snapshot["source_refs"])
        canonical = identity_differences(snapshot["canonical_ids"])
        result.update(source_refs=source, canonical_ids=canonical)
        result["identities_match"] = (source["expected_new"] > 0 and bool(snapshot["canonical_ids"]["expected_after"])
                                      and not any(value for comparison in (source, canonical)
                                                  for name, value in comparison.items() if name != "expected_new"))
    checks.update(
        death_active_owner_covered=all(row["active_owner_covered"] for row in details.values()),
        replacement_ingress_within_60s=all(row["replacement_ingress_seconds"] is not None and
                                           0 < row["replacement_ingress_seconds"] <= 60 for row in details.values()),
        replacement_commits_covered=all(row["replacement_commit_observed"] for row in details.values()),
        checkpoint_recovery_covered=all(row.get("checkpoint_recovery", False) for row in details.values()),
        canonical_reconciliation_passed=all(row.get("identities_match", False) for row in details.values()),
    )
    return checks, details


def analyze_skew(sequence, ingress, guard):
    if not sequence:
        return {}
    start, forward, backward, end = (row["at"] for row in sequence)
    owner = sequence[1]["owner"]
    return {
        endpoint: {
            name: bool(successes(ingress[endpoint], left, right, target) and
                       successes(guard[endpoint], left, right, target))
            for name, left, right, target in (("baseline", start, forward, owner),
                                               ("after_plus_45", forward, backward, None),
                                               ("after_minus_30", backward, end, None))
        }
        for endpoint in ENDPOINTS
    }


def analyze_freeze(sequence, ingress, guard, decisions):
    details = {"seconds": None, "baseline_endpoints": 0, "stale_rejections": 0, "old_owner_commits": 0}
    if not sequence:
        return details
    start, frozen, resumed, refreshed, _ = (row["at"] for row in sequence)
    old = sequence[1]["owner"]
    baseline = {endpoint: successes(guard[endpoint], start, frozen, old) for endpoint in ENDPOINTS}
    details["seconds"] = (resumed - frozen).total_seconds()
    details["baseline_endpoints"] = sum(bool(baseline[endpoint] and successes(ingress[endpoint], start, frozen, old))
                                        for endpoint in ENDPOINTS)
    epochs = {row["epoch"] for rows in baseline.values() for row in rows}
    for row in decisions:
        if row["owner"] == old:
            if resumed < row["at"] < refreshed and row["decision"] == "stale_rejected" and row["epoch"] in epochs:
                details["stale_rejections"] += 1
            if resumed <= row["at"] <= refreshed and row["decision"] == "committed":
                details["old_owner_commits"] += 1
    return details


def by_endpoint(rows, scenario):
    result = {endpoint: [] for endpoint in ENDPOINTS}
    for row in rows:
        if row["scenario"] == scenario and row["endpoint"] is not None:
            result[row["endpoint"]].append(row)
    return result


def main():
    parser = argparse.ArgumentParser(
        description="Offline analyzer for Member 1 polling, failover and storage fencing.\n\n" + SCHEMA,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    for kind in FILES:
        parser.add_argument(f"--{kind}", required=True, type=Path, metavar="JSONL")
    parser.add_argument("--freeze-expiry-seconds", required=True, type=float, metavar="SECONDS",
                        help="positive declared owner expiry used during the observed freeze")
    parser.add_argument("--output", required=True, type=Path, metavar="JSON", help="new result path")
    args = parser.parse_args()
    started = now()
    files, checks = {}, {}
    observations = {"input_sha256": {}, "input_rows": {}}
    for kind in FILES:
        path = getattr(args, kind)
        try:
            observations["input_sha256"][kind] = sha256(path)
            files[kind] = read_export(path, kind)
        except (KeyError, OSError, TypeError, ValueError):
            checks[f"valid_{kind}_export"] = False
        else:
            observations["input_rows"][kind] = len(files[kind])
            checks[f"valid_{kind}_export"] = True
    expiry = args.freeze_expiry_seconds
    expiry_valid = math.isfinite(expiry) and expiry > 0
    checks["declared_expiry_valid"] = expiry_valid
    observations["declared_expiry_seconds"] = expiry if expiry_valid else None
    if len(files) != len(FILES):
        return report(args.output, "polling", started, checks, observations)

    sequences = {
        "death": fault_sequence(files["faults"], "death", "owner_died"),
        "skew": fault_sequence(files["faults"], "skew", "forward", "backward"),
        "freeze": fault_sequence(files["faults"], "freeze", "frozen", "resumed", "refreshed"),
    }
    skew, freeze = sequences["skew"], sequences["freeze"]
    checks["death_markers_ordered"] = sequences["death"] is not None
    checks["skew_markers_ordered_same_target"] = bool(skew and skew[1]["node"] == skew[2]["node"] and
                                                      skew[1]["owner"] == skew[2]["owner"])
    checks["freeze_markers_ordered_same_owner"] = bool(freeze and len({row["owner"] for row in freeze[1:-1]}) == 1)
    windows = sorted((sequence[0]["at"], sequence[-1]["at"]) for sequence in sequences.values() if sequence)
    checks["fault_windows_disjoint"] = (len(windows) == len(SCENARIOS) and
                                        all(left[1] < right[0] for left, right in zip(windows, windows[1:])))
    observations["fault_markers_utc"] = {
        scenario: {row["event"]: row["at"].isoformat() for row in sequence}
        for scenario, sequence in sequences.items() if sequence
    }
    outside = 0
    for kind in ("ingress", "guard", "reconciliation"):
        for row in files[kind]:
            sequence = sequences[row["scenario"]]
            outside += sequence is None or not sequence[0]["at"] <= row["at"] <= sequence[-1]["at"]
    checks["observations_within_fault_windows"] = outside == 0
    observations["records_outside_windows"] = outside

    ingress = [row for row in files["ingress"] if row["decision"] == "accepted"]
    guard = [row for row in files["guard"] if row["decision"] == "committed"]
    for name, rows in (("upstream", ingress), ("storage", guard)):
        owners = owner_periods(rows)
        periods = {endpoint: len(seen) for endpoint, seen in owners.items()}
        conflicts = {endpoint: sum(len(owner_set) > 1 for owner_set in seen.values())
                     for endpoint, seen in owners.items()}
        observations[f"{name}_observed_periods"] = periods
        observations[f"{name}_conflict_periods"] = conflicts
        checks[f"exclusive_{name}_owner_per_period"] = all(periods.values()) and not any(conflicts.values())

    for scenario, sequence in sequences.items():
        receipts, commits = by_endpoint(ingress, scenario), by_endpoint(guard, scenario)
        if scenario == "death":
            death_checks, observations[scenario] = analyze_death(sequence, receipts, commits, files["reconciliation"])
            checks.update(death_checks)
        elif scenario == "skew":
            details = analyze_skew(sequence, receipts, commits)
            observations[scenario] = details
            checks["skew_phases_covered"] = bool(details and all(all(phases.values()) for phases in details.values()))
        else:
            decisions = [row for row in files["guard"] if row["scenario"] == "freeze"]
            details = analyze_freeze(sequence, receipts, commits, decisions)
            observations[scenario] = details
            checks["freeze_active_owner_covered"] = details["baseline_endpoints"] == len(ENDPOINTS)
            checks["freeze_exceeded_declared_expiry"] = bool(expiry_valid and details["seconds"] is not None and
                                                            details["seconds"] > expiry)
            checks["freeze_stale_guard_rejection_before_refresh"] = details["stale_rejections"] > 0
            checks["freeze_no_old_owner_commit_before_refresh"] = bool(sequence and details["old_owner_commits"] == 0)
    return report(args.output, "polling", started, checks, observations)


if __name__ == "__main__":
    raise SystemExit(run(main))
