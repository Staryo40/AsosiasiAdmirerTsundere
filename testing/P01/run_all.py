from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

from acceptance import metadata, prepare_results, timestamp


def suite_outcomes(selected: str | None) -> list[dict]:
    scenarios = {"T01": "T01-mapping-boundaries", "T02": "T02-runtime-schema-evolution"}
    outcomes = []
    for test_id, folder in scenarios.items():
        if selected and selected != test_id:
            continue
        script = Path(__file__).resolve().parent / folder / "run.py"
        completed = subprocess.run([sys.executable, str(script)], check=False)
        outcomes.append({"test_id": test_id, "exit_code": completed.returncode,
                         "result": "PASS" if completed.returncode == 0 else "FAIL"})
    return outcomes


def main() -> int:
    parser = argparse.ArgumentParser(description="Run P1 data interoperability acceptance checks")
    parser.add_argument("--only", choices=["T01", "T02"])
    arguments = parser.parse_args()
    started = timestamp()
    directory = prepare_results()
    outcomes = suite_outcomes(arguments.only)
    passed = all(outcome["result"] == "PASS" for outcome in outcomes)
    summary = metadata("P01", started) | {"result": "PASS" if passed else "FAIL", "tests": outcomes,
                                          "formal_report": "MANUAL: P1-06 requires the final milestone report"}
    (directory / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(f"P01: {summary['result']}\nEvidence: {directory}")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
