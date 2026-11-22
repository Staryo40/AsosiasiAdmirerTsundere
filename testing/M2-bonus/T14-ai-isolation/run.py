# T14: a slow, wrong, or dead AI provider, or a dead ai-summarizer, never disturbs the Field Team alert path.
from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from urllib.parse import urlencode

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from m2lib import Target, bearer, finish, now, parser, request, wait_for  # noqa: E402

FAULTS = ("", "wrong", "down", "slow")
# The API gives the summarizer 2 s; allow network overhead on top.
API_BUDGET_MS = 3000


def prometheus_scalar(target: Target, expression: str) -> float | None:
    response = request("GET", f"{target.prometheus}/api/v1/query?{urlencode({'query': expression})}", timeout=30)
    if response.status != 200 or response.json().get("status") != "success":
        raise AssertionError(f"Prometheus answered HTTP {response.status}.")
    result = response.json()["data"]["result"]
    if not result:
        return None
    value = float(result[0]["value"][1])
    return None if value != value else round(value, 3)


def main() -> None:
    options = parser("T14 the AI summarizer stays off the alert path")
    options.add_argument("--summarizer", default=os.getenv("SUMMARIZER_BASE", "http://localhost:8090"),
                         help="ai-summarizer address, through kubectl port-forward svc/ai-summarizer 8090")
    args = options.parse_args()
    target, started = Target(args.target), now()
    target.require_k8s()
    internal = {"X-BNPB-Internal-Key": target.value("BNPB_AI_SUMMARIZER_INTERNAL_KEY")}
    field = bearer(target.token("FIELD"))
    restarts_before = {name: target.restart_count(name) for name in ("consumer-2", "aggregator", "api")}

    phases = []
    first_provider = None
    for fault in FAULTS:
        assert request("POST", f"{args.summarizer}/internal/fault", headers=internal, body={"mode": fault}).status == 200
        forced = request("POST", f"{args.summarizer}/internal/summarize", headers=internal, timeout=60)
        through_api = request("GET", f"{target.api}/v1/situation-summary", headers=field, timeout=10)
        hazards = request("GET", f"{target.api}/v1/hazards?limit=5", headers=field, timeout=10)
        body = forced.json() if forced.status == 200 else {}
        if fault == "" and body.get("ai_generated"):
            first_provider = body.get("provider")
        phases.append({
            "fault": fault or "none", "provider": body.get("provider"), "ai_generated": body.get("ai_generated"),
            "event_count": body.get("event_count"),
            "attempts": body.get("last_attempts"), "summarize_ms": forced.duration_ms,
            "api_status": through_api.status, "api_ms": through_api.duration_ms, "hazards_status": hazards.status,
        })
        print(f"fault={fault or 'none'}: provider={body.get('provider')} api={through_api.status} in {through_api.duration_ms} ms")
    request("POST", f"{args.summarizer}/internal/fault", headers=internal, body={"mode": ""})

    target.stop("ai-summarizer")
    wait_for("ai-summarizer to stop", lambda: not target.pods("ai-summarizer"), 120, 2)
    dead = request("GET", f"{target.api}/v1/situation-summary", headers=field, timeout=10)
    dead_hazards = request("GET", f"{target.api}/v1/hazards?limit=5", headers=field, timeout=10)
    print(f"summarizer stopped: summary {dead.status} in {dead.duration_ms} ms, hazards {dead_hazards.status}")
    target.start("ai-summarizer")
    recovered = wait_for("the summary to come back", lambda: request(
        "GET", f"{target.api}/v1/situation-summary", headers=field, timeout=10).status == 200, 180, 5)

    window = max(int((now() - started).total_seconds()), 60)
    alerts = prometheus_scalar(target, f"sum(increase(bnpb_notification_latency_seconds_count[{window}s]))")
    p95 = prometheus_scalar(target, "histogram_quantile(0.95, sum by (le) "
                                    f"(increase(bnpb_notification_latency_seconds_bucket[{window}s])))")
    restarts_after = {name: target.restart_count(name) for name in restarts_before}
    faulty = [phase for phase in phases if phase["fault"] != "none"]
    checks = {
        # Without events the chain never calls a provider, and without a key there is nothing to break.
        "events_in_the_window": bool(phases[0]["event_count"]),
        "a_provider_answered_before_the_faults": first_provider is not None,
        "faulty_provider_was_not_used": bool(first_provider) and all(phase["provider"] != first_provider for phase in faulty),
        "a_summary_was_always_returned": all(phase["provider"] for phase in phases),
        "api_answered_within_budget": all(phase["api_status"] == 200 and phase["api_ms"] <= API_BUDGET_MS for phase in phases),
        "dead_summarizer_gives_fast_503": dead.status == 503 and dead.duration_ms <= API_BUDGET_MS,
        "hazards_unaffected": all(phase["hazards_status"] == 200 for phase in phases) and dead_hazards.status == 200,
        "summary_recovered": bool(recovered),
        "alert_p95_within_5s": p95 is None or p95 <= 5,
        "no_restarts_on_the_alert_path": restarts_after == restarts_before,
    }
    finish("M2-bonus", "T14", target, started, checks, {
        "first_provider": first_provider, "phases": phases,
        "summarizer_stopped": {"summary_status": dead.status, "summary_ms": dead.duration_ms, "hazards_status": dead_hazards.status},
        "alerts_during_test": alerts, "alert_p95_seconds": p95, "window_seconds": window,
        "restarts": {"before": restarts_before, "after": restarts_after},
        "note": "alert_p95 is measured only when consumer-2 recorded alerts during the run; "
                "run it while BMKG produces events, or with a Tester Agent scenario.",
    })


if __name__ == "__main__":
    main()
