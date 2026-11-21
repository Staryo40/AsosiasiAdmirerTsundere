# T02: broken PVMBG bodies are kept as retryable failures, crash nothing, and BMKG keeps flowing meanwhile.
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from m2lib import Target, assert_status, bearer, failed_messages, finish, now, parse_time, parser, request, wait_for  # noqa: E402

MODES = ("truncated", "deep_nesting", "invalid_utf8", "oversized")


def last_poll(target: Target, token: str, source: str):
    response = request("GET", f"{target.api}/v1/hazards?source={source}&limit=1", headers=bearer(token))
    assert_status(response, 200, f"{source} hazard listing")
    value = response.json().get("sources", {}).get(source, {}).get("last_successful_poll")
    return parse_time(value) if value else None


def open_responses(target: Target, token: str, since) -> list[dict]:
    return [item for item in failed_messages(target, token, "PVMBG")
            if item["kind"] == "response" and parse_time(item["last_seen_at"]) >= since]


def main() -> None:
    options = parser("T02 malformed PVMBG payloads")
    options.add_argument("--timeout", type=int, default=90, help="seconds to wait for each fault to be seen and cleared")
    args = options.parse_args()
    target, started = Target(args.target), now()
    token = target.token("OPS")
    restarts_before = target.restart_count("aggregator")
    modes = []
    try:
        for mode in MODES:
            target.pvmbg_admin("payload-fault", {"mode": mode})
            switched = now()
            seen = wait_for(f"a stored {mode} response", lambda: open_responses(target, token, switched), args.timeout)
            bmkg_during = last_poll(target, token, "BMKG")
            target.pvmbg_admin("payload-fault", {"mode": "none"})
            cleared = now()
            ids = {item["message_id"] for item in seen}
            resolved = True
            try:
                wait_for(f"the {mode} response to resolve", lambda: not ids & {item["message_id"] for item in
                                                                              failed_messages(target, token, "PVMBG")}, args.timeout)
            except AssertionError:
                resolved = False
            modes.append({"mode": mode, "reason": seen[0]["reason"], "status": seen[0]["status"], "retryable": seen[0]["retryable"],
                          "payload_bytes": seen[0]["payload_bytes"], "payload_truncated": seen[0]["payload_truncated"],
                          "bmkg_polled_during_fault": bool(bmkg_during and bmkg_during >= switched),
                          "resolved_after_recovery": resolved, "fault_seconds": round((cleared - switched).total_seconds(), 1)})
            print(f"  {mode}: {seen[0]['reason']} ({seen[0]['status']}), resolved={resolved}")
    finally:
        target.pvmbg_admin("payload-fault", {"mode": "none"})
    restarts_after = target.restart_count("aggregator")
    health = request("GET", f"{target.aggregator}/health")
    checks = {
        "aggregator_never_restarted": restarts_after == restarts_before,
        "aggregator_healthy": health.status == 200,
        "every_fault_stored_as_retryable": all(entry["retryable"] and entry["status"] == "retrying" for entry in modes),
        "every_fault_resolved_after_recovery": all(entry["resolved_after_recovery"] for entry in modes),
        "bmkg_kept_flowing": all(entry["bmkg_polled_during_fault"] for entry in modes),
    }
    finish("M2-P3", "T02", target, started, checks, {
        "modes": modes, "aggregator_restarts": {"before": restarts_before, "after": restarts_after},
    })


if __name__ == "__main__":
    main()
