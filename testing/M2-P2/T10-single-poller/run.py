# T10: only the aggregator-poller Lease holder polls BMKG and PVMBG, proven from the mocks' own access logs.
from __future__ import annotations

import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from m2lib import Target, finish, hazards, now, parser  # noqa: E402
from testlib import load_dotenv  # noqa: E402

LEASE = "aggregator-poller"
POLL_PATHS = {"bmkg": ("/seismic-events", "/tsunami-warnings"), "pvmbg": ("/volcanic-reports",)}


def seconds(value: str, fallback: float) -> float:
    units = {"ms": 0.001, "s": 1, "m": 60}
    for unit in ("ms", "s", "m"):
        if value.endswith(unit):
            try:
                return float(value[: -len(unit)]) * units[unit]
            except ValueError:
                return fallback
    return fallback


def poll_requests(target: Target, started, ended, pods_by_ip: dict[str, str]) -> list[dict]:
    requests = []
    for mock, paths in POLL_PATHS.items():
        for record in target.logs(mock, started):
            if record.get("event") != "http_request" or record.get("path") not in paths:
                continue
            if record["_time"] is None or not started <= record["_time"] <= ended:
                continue
            ip = str(record.get("client_ip", "")).removeprefix("::ffff:")
            requests.append({"mock": mock, "path": record["path"], "time": record["_time"], "ip": ip,
                             "pod": pods_by_ip.get(ip), "status": record.get("status")})
    return requests


def duplicates(items: list[dict]) -> list[str]:
    counts = Counter(item["source_ref_id"] for item in items)
    return sorted(key for key, count in counts.items() if count > 1)


def main() -> None:
    options = parser("T10 one Aggregator replica polls each source")
    options.add_argument("--minutes", type=float, default=2, help="how long to watch the mocks")
    args = options.parse_args()
    target, started = Target(args.target), now()
    target.require_k8s()
    config = load_dotenv(target.root / "k8s/config/aggregator.env")
    intervals = {"/seismic-events": seconds(config.get("BNPB_BMKG_POLL_INTERVAL", "3s"), 3),
                 "/tsunami-warnings": seconds(config.get("BNPB_BMKG_POLL_INTERVAL", "3s"), 3),
                 "/volcanic-reports": seconds(config.get("BNPB_PVMBG_POLL_INTERVAL", "3s"), 3)}

    pods_by_ip = target.pod_ips("aggregator")
    holder_before = target.lease_holder(LEASE)
    owners = {pod: target.ready(pod, 8083).get("poll_owner") for pod in pods_by_ip.values()}
    print(f"Lease holder {holder_before}; /ready poll_owner: {owners}. Watching for {args.minutes:g} min.")
    time.sleep(args.minutes * 60)
    ended = now()
    holder_after = target.lease_holder(LEASE)
    pods_by_ip |= target.pod_ips("aggregator")

    requests = poll_requests(target, started, ended, pods_by_ip)
    from_aggregator = [item for item in requests if item["pod"]]
    by_pod = Counter(item["pod"] for item in from_aggregator)
    window = (ended - started).total_seconds()
    per_path = Counter(item["path"] for item in from_aggregator)
    # One poller asks at most once per interval; three would ask about three times as often.
    rate_ok = {path: per_path[path] <= window / interval * 1.5 + 2 for path, interval in intervals.items()}
    token = target.token("OPS")
    stored = {source: hazards(target, token, source) for source in ("BMKG", "PVMBG")}
    doubled = {source: duplicates(items) for source, items in stored.items()}

    checks = {
        "lease_holder_unchanged": bool(holder_before) and holder_before == holder_after,
        "every_poll_came_from_the_lease_holder": bool(from_aggregator) and set(by_pod) == {holder_before},
        "request_rate_fits_one_poller": all(rate_ok.values()),
        "exactly_one_replica_reports_leader": list(owners.values()).count("leader") == 1
                                              and all(owner in ("leader", "standby") for owner in owners.values()),
        "no_duplicate_hazard": not any(doubled.values()),
    }
    finish("M2-P2", "T10", target, started, checks, {
        "window_seconds": round(window, 1), "lease_holder": {"before": holder_before, "after": holder_after},
        "poll_owner_by_pod": owners, "poll_requests_by_pod": dict(by_pod),
        "poll_requests_by_path": dict(per_path), "expected_max_per_path": {path: round(window / interval * 1.5 + 2)
                                                                           for path, interval in intervals.items()},
        "requests_from_other_clients": len(requests) - len(from_aggregator),
        "first_requests": [{key: str(value) for key, value in item.items()} for item in from_aggregator[:10]],
        "stored_hazards": {source: len(items) for source, items in stored.items()},
        "duplicate_source_ref_ids": {source: keys[:20] for source, keys in doubled.items()},
    })


if __name__ == "__main__":
    main()
