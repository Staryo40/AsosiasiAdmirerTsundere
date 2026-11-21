# T01: PVMBG schema versions 1-2 are accepted automatically, 3-5 are parked with their reason (strict Aggregator).
from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from m2lib import Target, assert_status, bearer, failed_messages, finish, now, parse_time, parser, request  # noqa: E402

EXPECTED = {1: "accepted", 2: "accepted", 3: "parked", 4: "parked", 5: "parked"}


def last_pvmbg_poll(target: Target, token: str):
    response = request("GET", f"{target.api}/v1/hazards?source=PVMBG&limit=1", headers=bearer(token))
    assert_status(response, 200, "PVMBG hazard listing")
    value = response.json().get("sources", {}).get("PVMBG", {}).get("last_successful_poll")
    return parse_time(value) if value else None


def main() -> None:
    options = parser("T01 PVMBG schema change policy")
    options.add_argument("--wait", type=int, default=45, help="seconds per schema version")
    args = options.parse_args()
    target, started = Target(args.target), now()
    token = target.token("OPS")
    versions = []
    try:
        for version, expected in EXPECTED.items():
            target.pvmbg_admin("schema-version", {"version": version})
            switched = now()
            time.sleep(args.wait)
            parked = [item for item in failed_messages(target, token, "PVMBG") if item["kind"] == "record"
                      and item["status"] == "parked" and parse_time(item["last_seen_at"]) >= switched]
            polled = last_pvmbg_poll(target, token)
            observed = "parked" if parked else "accepted" if polled and polled >= switched else "no_poll"
            versions.append({"version": version, "expected": expected, "observed": observed, "parked_records": len(parked),
                             "reasons": sorted({item["reason"] for item in parked}),
                             "retryable": sorted({item["retryable"] for item in parked}),
                             "last_successful_poll": polled.isoformat() if polled else None})
            print(f"  version {version}: {observed} ({len(parked)} parked)")
    finally:
        target.pvmbg_admin("schema-version", {"version": 1})
    checks = {f"version_{entry['version']}_{entry['expected']}": entry["observed"] == entry["expected"] for entry in versions}
    checks["parked_records_are_not_retryable"] = all(entry["retryable"] in ([], [False]) for entry in versions)
    finish("M2-P3", "T01", target, started, checks, {
        "versions": versions,
        "precondition": "BNPB_PVMBG_FIELD_ALIASES, _NUMERIC_STRINGS and _MISSING_TIME_FALLBACK empty (strict policy).",
    })


if __name__ == "__main__":
    main()
