from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

COMMON = Path(__file__).resolve().parents[1] / "common"
sys.path.insert(0, str(COMMON))
from testlib import request, results_directory, timestamp  # noqa: E402


def main() -> int:
    started = timestamp(); api = os.getenv("API_BASE", "http://localhost:8080")
    ready = request("GET", f"{api}/ready")
    if ready.status != 200: raise AssertionError(f"P03 preflight failed: /ready returned HTTP {ready.status}.")
    root = Path(__file__).resolve().parent; result_root = results_directory(); os.environ["TEST_RESULTS_ROOT"] = str(result_root)
    scenarios = (
        ("T06", root / "T06-cross-domain-credentials/run.py"),
        ("T07", root / "T07-media-field-authorization/run.py"),
        ("T08", root / "T08-token-expiry-refresh/run.py"),
        ("T09", root / "T09-secret-hygiene/run.py"),
    )
    summary = []; failed = False
    for test_id, script in scenarios:
        print(f"\n=== {test_id} ===", flush=True)
        completed = subprocess.run([sys.executable, str(script)], check=False)
        passed = completed.returncode == 0; failed = failed or not passed
        summary.append({"test_id": test_id, "result": "PASS" if passed else "FAIL", "exit_code": completed.returncode})
    suite = {"suite": "P03", "started_at": started, "finished_at": timestamp(), "result": "FAIL" if failed else "PASS", "tests": summary}
    summary_path = result_root / "summary.json"; summary_path.write_text(json.dumps(suite, indent=2) + "\n", encoding="utf-8")
    print(f"\nP03 result: {suite['result']}\nResults: {result_root}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
