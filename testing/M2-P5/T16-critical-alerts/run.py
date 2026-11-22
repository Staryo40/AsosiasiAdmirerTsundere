# T16: every observation Sensor Pantai marks critical reaches the Field Team alert path,
# or is set aside with a reason; alert p95 stays within 5 s, reported against the SLO budget.
from __future__ import annotations

import base64
import os
import sys
from collections import Counter
from pathlib import Path
from urllib.parse import urlencode

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from m2lib import Target, failed_messages, finish, hazards, now, parse_time, parser, payload_json, request  # noqa: E402

P95_LIMIT_S = 5


def sensor_observations(base: str, token: str) -> list[dict]:
    items, cursor = [], None
    for _ in range(2000):
        query = {"limit": 500} | ({"cursor": cursor} if cursor else {})
        response = request("GET", f"{base}/v1/observations?{urlencode(query)}",
                           headers={"Authorization": f"Bearer {token}"}, timeout=30)
        if response.status != 200:
            raise AssertionError(f"Sensor Pantai /v1/observations answered HTTP {response.status}.")
        page = response.json()
        items.extend(page.get("items") or [])
        cursor = page.get("next_cursor")
        if not cursor or not page.get("items"):
            return items
    raise AssertionError("Sensor observation listing did not finish.")


def prometheus_scalar(target: Target, expression: str) -> float | None:
    response = request("GET", f"{target.prometheus}/api/v1/query?{urlencode({'query': expression})}", timeout=30)
    if response.status != 200 or response.json().get("status") != "success":
        raise AssertionError(f"Prometheus answered HTTP {response.status}.")
    result = response.json()["data"]["result"]
    if not result:
        return None
    value = float(result[0]["value"][1])
    return None if value != value else round(value, 3)


def rejected_observation_ids(items: list[dict]) -> dict[str, str]:
    rejected = {}
    for item in items:
        payload = payload_json(item)
        if isinstance(payload, dict) and payload.get("observation_id"):
            rejected[str(payload["observation_id"])] = item.get("reason") or ""
    return rejected


def main() -> None:
    options = parser("T16 critical Sensor Pantai observations reach the Field Team")
    options.add_argument("--sensor", default=os.getenv("PANTAI_BASE", "http://localhost:18080"),
                         help="Sensor Pantai, through kubectl port-forward svc/sensor-pantai 18080:8080")
    options.add_argument("--window", default="1h", help="Prometheus window for the alert p95, e.g. 30m or 1h")
    args = options.parse_args()
    target, started = Target(args.target), now()
    target.require_k8s()
    token = base64.b64decode(target.kubectl("get", "secret", "pantai-token", "-o", "jsonpath={.data.token}")).decode()

    observations = sensor_observations(args.sensor, token)
    critical = {str(item["observation_id"]): item for item in observations if item.get("critical") is True}
    ops = target.token("OPS")
    stored = {item["source_ref_id"]: item for item in hazards(target, ops, "SENSOR_PANTAI")}
    rejected = rejected_observation_ids(failed_messages(target, ops, "SENSOR_PANTAI"))

    # consumer-2 logs since its container started; older hazards cannot be checked from logs.
    pods = target.pods("consumer-2")
    container_start = min((pod["status"].get("startTime") for pod in pods if pod.get("status", {}).get("startTime")), default=None)
    log_start = parse_time(container_start) if container_start else started
    alerted = {record.get("hazard_id") for record in target.logs("consumer-2", log_start) if record.get("event") == "alert_sent"}

    outcome, rows = Counter(), []
    for observation_id, observation in sorted(critical.items()):
        hazard = stored.get(observation_id)
        if observation_id in rejected:
            state = "set_aside"
        elif hazard is None:
            state = "lost"
        elif hazard.get("severity") != "AWAS":
            state = "stored_not_critical"
        elif parse_time(hazard["ingested_at"]) < log_start:
            state = "before_log_window"
        elif hazard["hazard_id"] in alerted:
            state = "alerted"
        else:
            state = "stored_no_alert"
        outcome[state] += 1
        rows.append({"observation_id": observation_id, "station_id": observation.get("station_id"),
                     "state": state, "hazard_id": hazard and hazard["hazard_id"],
                     "reason": rejected.get(observation_id)})

    window = args.window
    p95 = prometheus_scalar(target, "histogram_quantile(0.95, sum by (le) (increase("
                                    f"bnpb_notification_latency_seconds_bucket{{source=\"SENSOR_PANTAI\"}}[{window}])))")
    alerts_in_window = prometheus_scalar(target, f"sum(increase(bnpb_notification_latency_seconds_count{{source=\"SENSOR_PANTAI\"}}[{window}]))")
    budget = prometheus_scalar(target, "bnpb:alert_error_budget_remaining:1d")
    checks = {
        "sensor_produced_critical_events": bool(critical),
        "no_critical_event_lost": outcome["lost"] == 0 and outcome["stored_no_alert"] == 0 and outcome["stored_not_critical"] == 0,
        "every_set_aside_event_has_a_reason": all(rejected[i] for i in critical if i in rejected),
        "critical_alert_p95_within_5s": p95 is not None and p95 <= P95_LIMIT_S,
    }
    finish("M2-P5", "T16", target, started, checks, {
        "sensor_observations": len(observations), "critical_observations": len(critical), "outcomes": dict(outcome),
        "consumer2_log_window_start": log_start.isoformat(), "alert_p95_seconds": p95, "prometheus_window": window,
        "sensor_alerts_in_window": alerts_in_window, "alert_path_budget_remaining_24h": budget,
        "critical": rows[:200],
        "note": "Matching is by observation_id, since the sensor's own clock runs at PANTAI_SPEED. A critical "
                "observation rejected for breaking the contract (for example stale data) counts as set aside, "
                "not lost. Hazards ingested before consumer-2 started cannot be checked from its logs.",
    })


if __name__ == "__main__":
    main()
