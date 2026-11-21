# T12: after a clock-skew or freeze run, each lease term had one holder and stale terms stored nothing.
from __future__ import annotations

import sys
from collections import Counter, defaultdict
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from m2lib import Target, finish, hazards, now, parse_time, parser  # noqa: E402

LEASE = "aggregator-poller"
REFUSED = ("lease_lost",)


def main() -> None:
    options = parser("T12 lease terms after clock skew or a frozen node")
    options.add_argument("--since", help="start of the run to analyse (ISO 8601); default 15 minutes ago")
    args = options.parse_args()
    target, started = Target(args.target), now()
    target.require_k8s()
    since = parse_time(args.since) if args.since else started - timedelta(minutes=15)
    records = target.logs("aggregator", since)
    if not records:
        raise SystemExit("No Aggregator logs in that window; pods restarted since then lose their logs.")

    # Log times come from each node's clock, which the scenario may have skewed, so nothing below orders pods by time.
    polls = [record for record in records if record.get("event") == "agency_poll" and "lease_generation" in record]
    holders_by_term: dict[int, set[str]] = defaultdict(set)
    successes_by_term: Counter = Counter()
    for record in polls:
        term = int(record["lease_generation"])
        holders_by_term[term].add(str(record.get("lease_holder")))
        if not record.get("category"):
            successes_by_term[term] += 1
    refused = [record for record in polls if record.get("category") in REFUSED]
    claim_failures = [record for record in records if record.get("event") == "poll_fence_claim_failed"]
    terms = [{"pod": record["_pod"], "event": record["event"], "lease_generation": record.get("lease_generation"),
              "pod_clock": str(record["_time"])}
             for record in records if record.get("event") in ("lease_acquired", "lease_released")]
    acquired_by_term: dict[int, set[str]] = defaultdict(set)
    for record in records:
        if record.get("event") == "lease_acquired" and record.get("lease_generation") is not None:
            acquired_by_term[int(record["lease_generation"])].add(record["_pod"])

    token = target.token("OPS")
    doubled = {}
    for source in ("BMKG", "PVMBG"):
        counts = Counter(item["source_ref_id"] for item in hazards(target, token, source))
        doubled[source] = sorted(key for key, count in counts.items() if count > 1)

    checks = {
        "every_term_had_one_holder": all(len(holders) == 1 for holders in holders_by_term.values())
                                     and all(len(pods) == 1 for pods in acquired_by_term.values()),
        "polls_ran_under_a_lease_term": bool(polls),
        "no_duplicate_hazard": not any(doubled.values()),
    }
    print(f"{len(terms)} lease events, {len(refused)} refused stale writes, {len(claim_failures)} refused claims.")
    finish("M2-P2", "T12", target, started, checks, {
        "window": {"since": since.isoformat(), "until": started.isoformat()},
        "current_holder": target.lease_holder(LEASE),
        "terms": sorted({int(term): sorted(holders) for term, holders in holders_by_term.items()}.items()),
        "successful_polls_by_term": dict(sorted(successes_by_term.items())),
        "refused_stale_writes": [{"pod": record["_pod"], "holder": record.get("lease_holder"),
                                  "lease_generation": record.get("lease_generation"), "endpoint": record.get("endpoint"),
                                  "pod_clock": str(record["_time"])} for record in refused[:50]],
        "refused_claims": [{"pod": record["_pod"], "lease_generation": record.get("lease_generation"),
                            "pod_clock": str(record["_time"])} for record in claim_failures[:20]],
        "lease_events": terms[:50],
        "duplicate_source_ref_ids": {source: keys[:20] for source, keys in doubled.items()},
        "note": "Pod clocks may be skewed in this run. The ordering guarantee (no write commits after a newer "
                "term claims the store) is proven by TestPollFenceTakeoverWaitsForAnInFlightWrite.",
    })


if __name__ == "__main__":
    main()
