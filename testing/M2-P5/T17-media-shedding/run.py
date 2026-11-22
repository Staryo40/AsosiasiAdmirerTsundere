# T17: under more load than the API admits, Media is refused with a controlled 429 while the Field Team is served.
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from m2lib import Target, finish, now, parser  # noqa: E402


def main() -> None:
    options = parser("T17 Media is shed before the Field Team under overload")
    options.add_argument("--vus", type=int, default=500, help="k6 virtual users; each sends one Media and one Field Team read")
    args = options.parse_args()
    target, started = Target(args.target), now()
    if not shutil.which("k6"):
        raise SystemExit("k6 is not on PATH; install it or run loadtest/k6-media-shed.js by hand.")
    summary = Path(tempfile.mkdtemp()) / "k6-summary.json"
    env = {**os.environ, "API_BASE": target.api, "VUS": str(args.vus),
           "FIELD_SECRET": target.value("BNPB_FIELD_CLIENT_SECRET"), "MEDIA_SECRET": target.value("BNPB_MEDIA_CLIENT_SECRET")}
    print(f"Running loadtest/k6-media-shed.js with {args.vus} VUs for 60 s.")
    subprocess.run(["k6", "run", "--quiet", f"--summary-export={summary}", "loadtest/k6-media-shed.js"],
                   cwd=target.root, env=env, check=False)
    metrics = json.loads(summary.read_text())["metrics"]

    def rate(name):
        return metrics.get(name, {}).get("value")
    field_ok, media_shed, media_unexpected = rate("field_ok"), rate("media_shed"), rate("media_unexpected")
    field_p95 = metrics.get("field_latency", {}).get("p(95)")
    checks = {
        "media_was_shed": bool(media_shed),
        "media_got_only_200_or_429": media_unexpected is not None and media_unexpected < 0.01,
        "field_team_stayed_served": field_ok is not None and field_ok >= 0.99,
    }
    finish("M2-P5", "T17", target, started, checks, {
        "vus": args.vus, "field_success_rate": field_ok, "field_p95_ms": field_p95,
        "media_shed_rate": media_shed, "media_unexpected_rate": media_unexpected,
        "requests": metrics.get("http_reqs", {}).get("count"),
        "note": "If media_was_shed fails, the load stayed under capacity; raise --vus. Each API pod admits 64 "
                "requests in flight and at most 16 from Media (BNPB_API_MAX_IN_FLIGHT, BNPB_API_MEDIA_MAX_IN_FLIGHT); "
                "Field Team and Ops are never refused.",
    })


if __name__ == "__main__":
    main()
