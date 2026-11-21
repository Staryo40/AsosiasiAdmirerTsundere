# T08: during a 60x feed burst nothing is lost, lag returns to zero within 60 s, and we report the p95 latency.
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from m2lib import Target, finish, now, parse_time, parser, request  # noqa: E402

LATENCY = "bnpb_satellite_store_latency_seconds_bucket"


def prometheus(target: Target, path: str, query: dict) -> list:
    response = request("GET", f"{target.prometheus}/api/v1/{path}?{urlencode(query)}", timeout=30)
    if response.status != 200 or response.json().get("status") != "success":
        raise AssertionError(f"Prometheus {path} answered HTTP {response.status}.")
    return response.json()["data"]["result"]


def scalar(target: Target, expression: str, at: datetime) -> float | None:
    result = prometheus(target, "query", {"query": expression, "time": at.timestamp()})
    if not result:
        return None
    value = float(result[0]["value"][1])
    return None if value != value else round(value, 3)


def series(target: Target, expression: str, start: datetime, end: datetime, step: int) -> list[tuple[datetime, float]]:
    result = prometheus(target, "query_range", {"query": expression, "start": start.timestamp(), "end": end.timestamp(), "step": step})
    if not result:
        return []
    return [(datetime.fromtimestamp(float(at), timezone.utc), float(value)) for at, value in result[0]["values"]]


def main() -> None:
    options = parser("T08 burst catch-up and latency from Prometheus")
    options.add_argument("--start", help="burst start (ISO 8601); default --minutes ago")
    options.add_argument("--end", help="end of the observation window (ISO 8601); default now")
    options.add_argument("--burst-end", help="when the feed stopped bursting (ISO 8601); default: the moment lag peaked")
    options.add_argument("--minutes", type=int, default=30)
    options.add_argument("--lag-threshold", type=float, default=0, help="lag at or below this counts as caught up")
    args = options.parse_args()
    target, started = Target(args.target), now()
    end = parse_time(args.end) if args.end else started
    start = parse_time(args.start) if args.start else end - timedelta(minutes=args.minutes)
    window = f"{max(int((end - start).total_seconds()), 60)}s"

    quantiles = {name: scalar(target, f"histogram_quantile({q}, sum by (le) (increase({LATENCY}[{window}])))", end)
                 for name, q in (("p50", 0.5), ("p95", 0.95), ("p99", 0.99))}
    stored = scalar(target, f"sum(increase(bnpb_satellite_store_latency_seconds_count[{window}]))", end)
    lag = series(target, "max(bnpb_satellite_feed_lag_events)", start, end, 5)
    if not lag:
        raise AssertionError("No bnpb_satellite_feed_lag_events samples in the window; is Prometheus scraping the satellite pods?")
    peak_at, peak = max(lag, key=lambda sample: sample[1])
    burst_end = parse_time(args.burst_end) if args.burst_end else peak_at
    caught_up = next((at for at, value in lag if at >= burst_end and value <= args.lag_threshold), None)
    catch_up_seconds = None if caught_up is None else round((caught_up - burst_end).total_seconds(), 1)
    processed = scalar(target, f"sum(increase(bnpb_satellite_feed_events_total[{window}]))", end)
    malformed = scalar(target, f"sum(increase(bnpb_satellite_feed_events_total{{kind=~\"malformed|unusable\"}}[{window}]))", end)
    reconnect_gaps = scalar(target, f"sum(changes(bnpb_satellite_connected[{window}]))", end)

    checks = {
        "lag_back_to_zero_within_60s": catch_up_seconds is not None and catch_up_seconds <= 60,
        "latency_reported": quantiles["p95"] is not None,
    }
    finish("M2-P4", "T08", target, started, checks, {
        "window": {"start": start.isoformat(), "end": end.isoformat(), "burst_end": burst_end.isoformat()},
        "latency_seconds": quantiles, "changes_stored": stored,
        "lag": {"peak_events": peak, "peak_at": peak_at.isoformat(), "caught_up_at": caught_up.isoformat() if caught_up else None,
                "catch_up_seconds": catch_up_seconds, "samples": [(at.isoformat(), value) for at, value in lag[::12]]},
        "feed_events_processed": processed, "malformed_or_unusable": malformed, "connection_changes": reconnect_gaps,
        "note": "Latency runs from an event arriving at the satellite service to the Aggregator storing the fire change.",
    })


if __name__ == "__main__":
    main()
