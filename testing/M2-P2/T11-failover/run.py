# T11: kill the polling Aggregator replica; another resumes polling within 60 s with no missed or duplicated event.
from __future__ import annotations

import sys
import time
from collections import Counter
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from m2lib import LogFollower, Target, failed_messages, finish, hazards, now, parse_time, parser, wait_for  # noqa: E402

LEASE = "aggregator-poller"


def main() -> None:
    options = parser("T11 Aggregator poller failover")
    options.add_argument("--warmup", type=int, default=30, help="seconds of normal polling before the kill")
    options.add_argument("--settle", type=int, default=60, help="seconds after recovery before checking stored events")
    args = options.parse_args()
    target, started = Target(args.target), now()
    target.require_k8s()
    pods_by_ip = target.pod_ips("aggregator")
    follower = LogFollower(target, "aggregator", started)
    time.sleep(args.warmup)

    victim = target.lease_holder(LEASE)
    if not victim:
        raise SystemExit(f"Lease {LEASE} has no holder; is BNPB_AGGREGATOR_LEASE_NAME set?")
    killed_at = now()
    target.kill("aggregator", victim)
    print(f"Killed {victim}; waiting for another replica to take over.")
    successor = wait_for("a new lease holder", lambda: (holder := target.lease_holder(LEASE)) and holder != victim and holder, 120, 1)
    lease_moved_at = now()

    def first_success():
        for record in sorted(target.logs("aggregator", killed_at, pod=successor), key=lambda item: item["_time"] or killed_at):
            if record.get("event") == "agency_poll" and not record.get("category") and record["_time"] and record["_time"] > killed_at:
                return record
        return None
    resumed = wait_for("the new holder's first successful poll", first_success, 120, 2)
    time.sleep(args.settle)
    records = follower.stop()
    pods_by_ip |= target.pod_ips("aggregator")

    successor_ips = {ip for ip, pod in pods_by_ip.items() if pod == successor}
    upstream = sorted((record for mock in ("bmkg", "pvmbg") for record in target.logs(mock, killed_at)
                       if record.get("event") == "http_request" and record.get("path") in ("/seismic-events", "/volcanic-reports")
                       and str(record.get("client_ip", "")).removeprefix("::ffff:") in successor_ips and record["_time"]),
                      key=lambda record: record["_time"])
    failover_seconds = (resumed["_time"] - killed_at).total_seconds()
    upstream_seconds = (upstream[0]["_time"] - killed_at).total_seconds() if upstream else None

    # Every BMKG event the mock produced around the kill must be stored exactly once (or parked with a reason).
    window_start = killed_at - timedelta(seconds=args.warmup)
    # Events from the last few seconds may not have been polled yet.
    cutoff = now() - timedelta(seconds=15)
    produced = {event["event_id"] for event in target.bmkg_events(window_start) if parse_time(event["occurred_at"]) <= cutoff}
    token = target.token("OPS")
    stored = Counter(item["source_ref_id"] for item in hazards(target, token, "BMKG"))
    parked = {item.get("source_ref_id") for item in failed_messages(target, token, "BMKG")}
    missing = sorted(produced - set(stored) - parked)
    doubled = sorted(key for key, count in stored.items() if count > 1)
    successes_by_pod = Counter(record["_pod"] for record in records if record.get("event") == "agency_poll" and not record.get("category"))

    checks = {
        "a_different_replica_took_over": successor != victim,
        "polling_resumed_within_60s": failover_seconds <= 60,
        "upstream_saw_the_new_poller_within_60s": upstream_seconds is not None and upstream_seconds <= 60,
        "no_event_missed_during_failover": not missing,
        "no_event_stored_twice": not doubled,
    }
    finish("M2-P2", "T11", target, started, checks, {
        "killed": {"pod": victim, "at": killed_at.isoformat()},
        "successor": {"pod": successor, "lease_moved_after_s": round((lease_moved_at - killed_at).total_seconds(), 1),
                      "first_successful_poll_after_s": round(failover_seconds, 1),
                      "first_upstream_request_after_s": None if upstream_seconds is None else round(upstream_seconds, 1),
                      "lease_generation": resumed.get("lease_generation")},
        "bmkg_events_in_window": len(produced), "missing": missing[:20], "stored_twice": doubled[:20],
        "parked_in_window": len(produced & parked), "successful_polls_by_pod": dict(successes_by_pod),
        "lease_events": [{"pod": record["_pod"], "event": record["event"], "time": str(record["_time"]),
                          "lease_generation": record.get("lease_generation")}
                         for record in records if record.get("event") in ("lease_acquired", "lease_released")][:20],
    })


if __name__ == "__main__":
    main()
