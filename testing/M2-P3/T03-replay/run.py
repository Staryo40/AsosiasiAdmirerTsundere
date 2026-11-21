# T03: PVMBG breaks its schema, Ops adapts the Aggregator, then replays every parked report without duplicates or reordering.
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from m2lib import (  # noqa: E402
    Target, assert_status, bearer, failed_messages, finish, hazards, now, parse_time, parser, payload_json, request,
)

STATE = Path(__file__).resolve().parents[2] / "results" / "M2-P3" / "T03-state.json"
ALIAS = "level:alert_level"


def parked_records(target: Target, token: str) -> list[dict]:
    return [item for item in failed_messages(target, token, "PVMBG") if item["status"] == "parked" and item["kind"] == "record"]


def newest_by_report(items: list[dict]) -> dict[str, dict]:
    newest: dict[str, dict] = {}
    for item in items:
        report = payload_json(item)
        if not isinstance(report, dict) or "report_id" not in report or "reported_at" not in report:
            continue
        current = newest.get(report["report_id"])
        if current is None or parse_time(report["reported_at"]) >= parse_time(current["reported_at"]):
            newest[report["report_id"]] = {"reported_at": report["reported_at"], "level": report.get("level")}
    return newest


def park(target: Target, args) -> None:
    token = target.token("OPS")
    target.pvmbg_admin("schema-version", {"version": 3})
    print(f"PVMBG now renames alert_level to level; collecting parked reports for {args.wait}s.")
    time.sleep(args.wait)
    items = parked_records(target, token)
    newest = newest_by_report(items)
    if not newest:
        raise SystemExit("No parked PVMBG records yet. Is BNPB_PVMBG_FIELD_ALIASES empty and the poller running?")
    STATE.parent.mkdir(parents=True, exist_ok=True)
    STATE.write_text(json.dumps({"parked_at": now().isoformat(), "message_ids": [item["message_id"] for item in items],
                                 "reasons": sorted({item["reason"] for item in items}), "newest": newest}, indent=2))
    print(f"{len(items)} parked records for {len(newest)} reports; reasons: {sorted({item['reason'] for item in items})}")
    print("\nOps now declares the new field. Set the alias, then restart the Aggregator:")
    if target.name == "k8s":
        print(f"  edit k8s/config/aggregator.env: BNPB_PVMBG_FIELD_ALIASES={ALIAS}\n  kubectl apply -k k8s   # the new ConfigMap rolls the Aggregator")
    else:
        print(f"  add to bnpb/.env: BNPB_PVMBG_FIELD_ALIASES={ALIAS}\n  docker compose up -d aggregator")
    print(f"\nThen run: python {Path(__file__).relative_to(target.root)} replay --target {target.name}")


def replay(target: Target, args) -> None:
    token = target.token("OPS")
    rounds, totals, token_continue = [], {"attempted": 0, "recovered": 0, "still_parked": 0, "skipped": 0}, None
    for _ in range(500):
        body = {"source": "PVMBG"} | ({"continue": token_continue} if token_continue else {})
        response = request("POST", f"{target.api}/v1/ops/failed-messages/replay", headers=bearer(token), body=body, timeout=30)
        if response.status == 401:
            token = target.token("OPS")
            continue
        assert_status(response, 200, "Ops replay")
        result = response.json()
        rounds.append(result)
        for key in totals:
            totals[key] += result[key]
        token_continue = result.get("continue")
        if not result["remaining"]:
            break
    state = json.loads(STATE.read_text())
    state["replay"] = {"rounds": len(rounds), "totals": totals}
    STATE.write_text(json.dumps(state, indent=2))
    print(f"Replay finished in {len(rounds)} calls: {totals}")
    print(f"Then run: python {Path(__file__).relative_to(target.root)} verify --target {target.name}")


def verify(target: Target, args) -> None:
    started = now()
    state = json.loads(STATE.read_text())
    token = target.token("OPS")
    try:
        still = {item["message_id"] for item in parked_records(target, token)} & set(state["message_ids"])
        stored: dict[str, list[dict]] = {}
        for item in hazards(target, token, "PVMBG"):
            stored.setdefault(item["source_ref_id"], []).append(item)
        missing, duplicated, out_of_order = [], [], []
        for report_id, newest in state["newest"].items():
            rows = stored.get(report_id, [])
            if not rows:
                missing.append(report_id)
                continue
            if len(rows) > 1:
                duplicated.append(report_id)
            hazard, reported = rows[0], parse_time(newest["reported_at"])
            occurred = parse_time(hazard["occurred_at"])
            # A live report newer than the parked ones may already have replaced it; an older one never may.
            if occurred < reported or (occurred == reported and newest["level"] and hazard["severity"] != newest["level"].upper()):
                out_of_order.append({"report_id": report_id, "parked_newest": newest, "stored": {
                    "occurred_at": hazard["occurred_at"], "severity": hazard["severity"]}})
    finally:
        target.pvmbg_admin("schema-version", {"version": 1})
    checks = {
        "no_replayed_message_still_parked": not still,
        "every_parked_report_is_a_hazard": not missing,
        "no_duplicate_hazard": not duplicated,
        "newest_report_wins_per_source_ref_id": not out_of_order,
    }
    finish("M2-P3", "T03", target, started, checks, {
        "parked": {"at": state["parked_at"], "records": len(state["message_ids"]), "reports": len(state["newest"]),
                   "reasons": state["reasons"]},
        "replay": state.get("replay"), "still_parked": sorted(still)[:20], "missing": missing[:20],
        "duplicated": duplicated[:20], "out_of_order": out_of_order[:20],
        "note": "PVMBG was set back to schema version 1; the alias can stay, version 1 does not use the renamed field.",
    })


def main() -> None:
    options = parser("T03 schema change, adaptation and replay")
    options.add_argument("step", choices=("park", "replay", "verify"))
    options.add_argument("--wait", type=int, default=60, help="seconds to let reports park under schema version 3")
    args = options.parse_args()
    {"park": park, "replay": replay, "verify": verify}[args.step](Target(args.target), args)


if __name__ == "__main__":
    main()
