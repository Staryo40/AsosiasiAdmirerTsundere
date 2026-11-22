# T02: broken PVMBG bodies are kept with their reason, crash nothing, and BMKG keeps flowing meanwhile.
# A body that cannot be read at all is a retryable response; one bad element inside a valid list is a parked record.
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


def new_failures(target: Target, token: str, since) -> list[dict]:
    return [item for item in failed_messages(target, token, "PVMBG") if parse_time(item["last_seen_at"]) >= since]


def classified(item: dict) -> bool:
    if item["kind"] == "response":
        return item["status"] == "retrying" and item["retryable"]
    return item["status"] == "parked" and not item["retryable"]


def main() -> None:
    options = parser("T02 malformed PVMBG payloads")
    options.add_argument("--timeout", type=int, default=90, help="seconds to wait for each fault to be seen and cleared")
    args = options.parse_args()
    target, started = Target(args.target), now()
    # Access tokens last 60 s, shorter than one run.
    ops = lambda: target.token("OPS")
    restarts_before = target.restart_count("aggregator")
    modes = []
    try:
        for mode in MODES:
            target.pvmbg_admin("payload-fault", {"mode": mode})
            switched = now()
            seen = wait_for(f"a stored {mode} failure", lambda: new_failures(target, ops(), switched), args.timeout)
            failure = seen[0]
            bmkg_during = last_poll(target, ops(), "BMKG")
            target.pvmbg_admin("payload-fault", {"mode": "none"})
            cleared = now()
            # Only a whole-response failure can heal; a parked record stays until Ops changes the system.
            resolved = None
            if failure["kind"] == "response":
                ids = {item["message_id"] for item in seen if item["kind"] == "response"}
                resolved = True
                try:
                    wait_for(f"the {mode} response to resolve", lambda: not ids & {item["message_id"] for item in
                                                                                  failed_messages(target, ops(), "PVMBG")}, args.timeout)
                except AssertionError:
                    resolved = False
            modes.append({"mode": mode, "kind": failure["kind"], "reason": failure["reason"], "status": failure["status"],
                          "retryable": failure["retryable"], "classified": classified(failure),
                          "payload_bytes": failure["payload_bytes"], "payload_truncated": failure["payload_truncated"],
                          "bmkg_polled_during_fault": bool(bmkg_during and bmkg_during >= switched),
                          "resolved_after_recovery": resolved, "fault_seconds": round((cleared - switched).total_seconds(), 1)})
            print(f"  {mode}: {failure['kind']} {failure['reason']} ({failure['status']}), resolved={resolved}")
    finally:
        target.pvmbg_admin("payload-fault", {"mode": "none"})
    restarts_after = target.restart_count("aggregator")
    health = request("GET", f"{target.aggregator}/health")
    checks = {
        "aggregator_never_restarted": restarts_after == restarts_before,
        "aggregator_healthy": health.status == 200,
        "every_fault_stored_with_matching_status": all(entry["classified"] for entry in modes),
        "every_response_fault_resolved_after_recovery": all(entry["resolved_after_recovery"] is not False for entry in modes),
        "bmkg_kept_flowing": all(entry["bmkg_polled_during_fault"] for entry in modes),
    }
    finish("M2-P3", "T02", target, started, checks, {
        "modes": modes, "aggregator_restarts": {"before": restarts_before, "after": restarts_after},
    })


if __name__ == "__main__":
    main()
