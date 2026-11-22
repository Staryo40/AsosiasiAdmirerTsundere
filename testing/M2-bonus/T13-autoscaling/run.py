# T13: under the k6 ramp, the api and aggregator HPAs add pods on their own and remove them once the load ends.
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from m2lib import Target, finish, now, parser  # noqa: E402

HPAS = ("api", "aggregator")
OVERLAY = "kubectl apply -f k8s/autoscaling/"


def sample(target: Target) -> dict[str, dict]:
    hpas = json.loads(target.kubectl("get", "hpa", *HPAS, "-o", "json"))["items"]
    deployments = json.loads(target.kubectl("get", "deployment", *HPAS, "-o", "json"))["items"]
    ready = {item["metadata"]["name"]: item.get("status", {}).get("readyReplicas", 0) for item in deployments}
    result = {}
    for hpa in hpas:
        name, status = hpa["metadata"]["name"], hpa.get("status", {})
        cpu = next((metric["resource"]["current"].get("averageUtilization") for metric in status.get("currentMetrics") or []
                    if metric.get("type") == "Resource"), None)
        result[name] = {"current": status.get("currentReplicas", 0), "desired": status.get("desiredReplicas", 0),
                        "ready": ready.get(name, 0), "cpu_percent": cpu}
    return result


def start_k6(target: Target, summary: Path, peak: int) -> subprocess.Popen | None:
    if not shutil.which("k6"):
        return None
    env = {**os.environ, "API_BASE": target.api, "PEAK_VUS": str(peak),
           "FIELD_ID": target.value("BNPB_FIELD_CLIENT_ID"), "FIELD_SECRET": target.value("BNPB_FIELD_CLIENT_SECRET")}
    return subprocess.Popen(["k6", "run", "--quiet", f"--summary-export={summary}", "loadtest/k6-api-ramp.js"],
                            cwd=target.root, env=env)


def main() -> None:
    options = parser("T13 api and aggregator scale with load and back")
    options.add_argument("--peak-vus", type=int, default=2000, help="k6 VUs at the top of the ramp")
    options.add_argument("--minutes", type=float, default=8, help="how long to record when k6 is run by hand")
    options.add_argument("--settle", type=int, default=120, help="extra seconds after k6 ends to wait for the scale-down")
    options.add_argument("--interval", type=float, default=5)
    args = options.parse_args()
    target, started = Target(args.target), now()
    target.require_k8s()
    if target.kubectl("get", "hpa", *HPAS, "-o", "name", check=False).count("horizontalpodautoscaler") != len(HPAS):
        raise SystemExit(f"The HPAs are missing; deploy autoscaling mode first: {OVERLAY}")
    minimum = {item["metadata"]["name"]: item["spec"]["minReplicas"]
               for item in json.loads(target.kubectl("get", "hpa", *HPAS, "-o", "json"))["items"]}

    summary = Path(tempfile.mkdtemp()) / "k6-summary.json"
    k6 = start_k6(target, summary, args.peak_vus)
    if k6:
        print(f"Running loadtest/k6-api-ramp.js with {args.peak_vus} peak VUs.")
    else:
        print(f"k6 is not on PATH. Run loadtest/k6-api-ramp.js yourself now; recording for {args.minutes:g} min.")

    timeline, k6_ended = [], None
    deadline = time.monotonic() + args.minutes * 60
    while True:
        point = {"t": round((now() - started).total_seconds()), **sample(target)}
        timeline.append(point)
        print(" ".join(f"{name}={point[name]['ready']}/{point[name]['desired']} cpu={point[name]['cpu_percent']}%"
                       for name in HPAS), flush=True)
        if k6 and k6_ended is None and k6.poll() is not None:
            k6_ended = time.monotonic()
        scaled = any(item[name]["desired"] > minimum[name] for item in timeline for name in HPAS)
        back = all(point[name]["current"] == minimum[name] for name in HPAS)
        if k6:
            if k6_ended is not None and ((scaled and back) or time.monotonic() > k6_ended + args.settle):
                break
        elif time.monotonic() > deadline:
            break
        time.sleep(args.interval)

    peak = {name: max(item[name]["ready"] for item in timeline) for name in HPAS}
    error_rate = None
    if summary.exists():
        error_rate = json.loads(summary.read_text())["metrics"].get("http_req_failed", {}).get("value")
    final = timeline[-1]
    checks = {
        "scaled_up_under_load": any(peak[name] > minimum[name] for name in HPAS),
        "scaled_back_to_minimum": all(final[name]["current"] == minimum[name] for name in HPAS),
        "aggregator_never_below_3": all(item["aggregator"]["ready"] >= 3 for item in timeline),
        "error_rate_below_1_percent": not k6 or (error_rate is not None and error_rate < 0.01),
    }
    finish("M2-bonus", "T13", target, started, checks, {
        "min_replicas": minimum, "peak_ready_replicas": peak, "k6_peak_vus": args.peak_vus if k6 else None,
        "k6_error_rate": error_rate, "k6_run_by_script": bool(k6), "samples": len(timeline), "timeline": timeline,
    })


if __name__ == "__main__":
    main()
