from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from acceptance import ROOT, command_output, prepare_results, run_scenario

REQUIRED_TESTS = {"TestP1" + suffix for suffix in (
    "MappingContractFixtures", "MagnitudeBoundaries", "WarningOverridesMagnitude", "VolcanicLevelsAndConfidence",
    "InvalidCoreFields", "RejectsUnsupportedChanges", "PollingBoundaries", "TiedTimestampOrdering",
    "IntegrationSchemaCoexistence", "IntegrationLateWarningsAndDeduplication", "IntegrationInvalidRecordsAndCheckpoint",
    "IntegrationNetworkPolling", "IntegrationSourceIdentityAndOrphanWarning")}


def check_mapping(evidence: dict) -> None:
    command_output([sys.executable, "-m", "unittest", "discover", "-s", "testing/P01", "-p", "test_acceptance.py"])
    evidence["oracle_checks"] = "PASS"
    command_output(["docker", "compose", "--profile", "tests", "up", "-d", "--wait", "postgres-test"])
    command = ["docker", "compose", "--profile", "tests", "run", "--rm", "--no-deps", "tests",
               "go", "test", "-json", "-tags=integration", "-count=1", "-run", "^TestP1", "./internal/aggregator"]
    completed = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, timeout=300)
    (prepare_results() / "T01-go.ndjson").write_text(completed.stdout, encoding="utf-8")
    events = [json.loads(line) for line in completed.stdout.splitlines() if line.startswith("{")]
    cases = [{"case": event["Test"], "result": event["Action"].upper()}
             for event in events if "Test" in event and event["Action"] in {"pass", "fail", "skip"}]
    evidence.update({"command": command, "cases": cases, "exit_code": completed.returncode,
                     "database": "postgres-test (isolated test profile)", "raw_output": "T01-go.ndjson"})
    if completed.returncode or not REQUIRED_TESTS <= {case["case"] for case in cases} or any(case["result"] != "PASS" for case in cases):
        raise AssertionError("T01 Go acceptance failed or required tests did not run; see T01-go.ndjson")


if __name__ == "__main__":
    raise SystemExit(run_scenario("T01", check_mapping))
