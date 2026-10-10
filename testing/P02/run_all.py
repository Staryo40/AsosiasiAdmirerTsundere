"""Run the P02 scenarios serially, restoring injection state between scenarios."""
import json
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "common"))
from testlib import results_directory, timestamp


def main() -> int:
    started = timestamp()
    result_root = results_directory("P02")
    environment = dict(os.environ, TEST_RESULTS_ROOT=str(result_root))
    scenarios = (
        ("T03", "T03-head-of-line"),
        ("T04", "T04-sustained-load"),
        ("T05", "T05-outage-recovery"),
    )
    results = []
    for test_id, directory in scenarios:
        print(f"\n=== {test_id} ===", flush=True)
        script = Path(__file__).resolve().parent / directory / "run.py"
        process = subprocess.run([sys.executable, str(script)], env=environment, check=False)
        results.append({"test_id": test_id, "result": "PASS" if process.returncode == 0 else "FAIL",
                        "exit_code": process.returncode, "evidence": f"{test_id}/result.json"})
    failed = any(test["result"] != "PASS" for test in results)
    summary = {"suite": "P02", "started_at": started, "finished_at": timestamp(),
               "result": "FAIL" if failed else "PASS", "tests": results}
    (result_root / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(f"\nP02 {summary['result']}: {result_root}")
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
