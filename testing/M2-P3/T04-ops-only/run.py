# T04: failed messages can be read and replayed only by BNPB Internal Ops.
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from m2lib import Target, finish, now, parser, request  # noqa: E402


def main() -> None:
    args = parser("T04 failed messages are Ops-only").parse_args()
    target, started = Target(args.target), now()
    tokens = {role: target.token(role) for role in ("MEDIA", "FIELD", "OPS")}
    cases = []

    def call(label: str, method: str, url: str, token: str | None, expected: tuple[int, ...], body=None) -> None:
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        response = request(method, url, headers=headers, body=body)
        cases.append({"case": label, "status": response.status, "expected": list(expected)})

    listing, replay = f"{target.api}/v1/ops/failed-messages?limit=1", f"{target.api}/v1/ops/failed-messages/replay"
    call("no_token_list", "GET", listing, None, (401,))
    call("media_list", "GET", listing, tokens["MEDIA"], (403,))
    call("field_list", "GET", listing, tokens["FIELD"], (403,))
    call("field_replay", "POST", replay, tokens["FIELD"], (403,), body={})
    call("ops_list", "GET", listing, tokens["OPS"], (200,))
    # The Aggregator's own route refuses a client token: only the API's internal key may call it.
    call("aggregator_route_with_ops_token", "GET", f"{target.aggregator}/internal/dlq/messages?limit=1", tokens["OPS"], (401, 403))
    checks = {case["case"]: case["status"] in case["expected"] for case in cases}
    finish("M2-P3", "T04", target, started, checks, {"cases": cases})


if __name__ == "__main__":
    main()
