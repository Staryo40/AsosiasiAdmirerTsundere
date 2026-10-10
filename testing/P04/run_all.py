from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
import sys
import uuid
from datetime import datetime
from pathlib import Path

from checks import (ROOT, cli, flexible_storage, independent_rebuild,
                    store_isolation, timestamp)

SCENARIOS = {"T10": independent_rebuild, "T11": store_isolation, "T12": flexible_storage}
FAILURES = (AssertionError, OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError)


def configure_project() -> str:
    project = "aat-p4-" + uuid.uuid4().hex[:12]
    folder = Path(__file__).resolve().parent
    os.environ.update(COMPOSE_PROJECT_NAME=project, COMPOSE_PATH_SEPARATOR=os.pathsep,
                      COMPOSE_FILE=os.pathsep.join(str(path) for path in (ROOT / "compose.yaml", folder / "compose.acceptance.yaml")))
    os.environ.pop("COMPOSE_PROFILES", None)
    return project


def configure_endpoints() -> dict[str, str]:
    endpoints = {}
    for service, port, variable in (("api", "8080", "API_BASE"), ("bmkg", "8081", "BMKG_BASE"),
                                    ("pvmbg", "8082", "PVMBG_BASE")):
        binding = cli(["docker", "compose", "port", service, port]).strip()
        if not binding.startswith("127.0.0.1:") or "\n" in binding:
            raise AssertionError(f"{service}: expected one loopback port binding")
        endpoints[variable] = "http://" + binding
    os.environ.update(endpoints)
    return endpoints


def evidence_directory() -> Path:
    directory = ROOT / "testing/results/P04" / datetime.now().strftime("%Y%m%d-%H%M%S-%f")
    directory.mkdir(parents=True)
    # Isolate P1's reused evidence helpers from a caller's earlier results folder.
    os.environ["TEST_RESULTS_ROOT"] = str(directory)
    return directory


def save_evidence(path: Path, evidence: dict) -> None:
    path.write_text(json.dumps(evidence, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def scenario_outcome(test_id: str, deadline: float, directory: Path) -> dict:
    evidence = {"test_id": test_id, "started_at": timestamp(), "result": "FAIL"}
    try:
        SCENARIOS[test_id](evidence, deadline)
        evidence["result"] = "PASS"
    except FAILURES as failure:
        evidence["failure"] = str(failure)
    finally:
        evidence["finished_at"] = timestamp()
        save_evidence(directory / f"{test_id}.json", evidence)
    print(f"{test_id}: {evidence['result']}", flush=True)
    return {key: evidence[key] for key in ("test_id", "result")}


def preserved_volumes(project: str) -> list[str]:
    return cli(["docker", "volume", "ls", "--filter", f"label=com.docker.compose.project={project}",
                "--format", "{{.Name}}"] ).splitlines()


def cleanup_project(summary: dict, arguments: argparse.Namespace) -> None:
    try:
        summary["containers_before_cleanup"] = container_diagnostics()
    finally:
        if arguments.keep_stack:
            summary["cleanup"] = "KEPT: debug with the explicit project/files recorded here"
        else:
            cli(["docker", "compose", "down", "--timeout", "15"])
            summary["cleanup"] = "Containers/networks removed; named volumes preserved"
    summary["preserved_volumes"] = preserved_volumes(summary["compose_project"])


def container_diagnostics() -> list[dict]:
    identities = cli(["docker", "compose", "ps", "--all", "--quiet"]).split()
    inspected = json.loads(cli(["docker", "inspect", *identities])) if identities else []
    return [{"service": container["Config"]["Labels"]["com.docker.compose.service"],
             "id": container["Id"], "image": container["Image"], "state": container["State"]["Status"],
             "exit_code": container["State"]["ExitCode"],
             "health": container["State"].get("Health", {}).get("Status")}
            for container in inspected]


def suite_summary(project: str, arguments: argparse.Namespace) -> dict:
    return {"suite": "P04", "result": "FAIL", "started_at": timestamp(), "command": sys.argv,
            "python_version": sys.version.split()[0],
            "compose_project": project, "compose_files": os.environ["COMPOSE_FILE"],
            "deadline_seconds": arguments.deadline, "tests": [],
            "manual_requirements": {"P4-05": "PENDING: service-boundary rationale and >=2 storage alternatives in the final report"}}


def run_suite(arguments: argparse.Namespace) -> int:
    directory = evidence_directory()
    project = configure_project()
    summary = suite_summary(project, arguments)
    print(f"P04 project: {project}\nEvidence: {directory}\nBuilding isolated stack...", flush=True)
    try:
        start_project(summary)
        selected = [arguments.only] if arguments.only else list(SCENARIOS)
        summary["tests"] = [scenario_outcome(test_id, arguments.deadline, directory) for test_id in selected]
        summary["result"] = "PASS" if all(test["result"] == "PASS" for test in summary["tests"]) else "FAIL"
    except FAILURES as failure:
        summary["failure"] = str(failure)
    finally:
        finish_suite(summary, arguments, directory)
    print(f"P04 runtime: {summary['result']} (P4-05 requires report review)\nEvidence: {directory}", flush=True)
    return 0 if summary["result"] == "PASS" else 1


def start_project(summary: dict) -> None:
    summary.update(git_commit=cli(["git", "rev-parse", "HEAD"]).strip(),
                   working_tree_dirty=bool(cli(["git", "status", "--porcelain"])),
                   compose_version=cli(["docker", "compose", "version"]).strip())
    cli(["docker", "compose", "config", "--quiet"])
    cli(["docker", "compose", "up", "-d", "--build", "--wait", "--wait-timeout", "180"], timeout=900)
    summary["endpoints"] = configure_endpoints()


def finish_suite(summary: dict, arguments: argparse.Namespace, directory: Path) -> None:
    try:
        cleanup_project(summary, arguments)
    except FAILURES as failure:
        summary["cleanup_failure"] = str(failure)
        summary["result"] = "FAIL"
    finally:
        summary["finished_at"] = timestamp()
        save_evidence(directory / "summary.json", summary)


def main() -> int:
    parser = argparse.ArgumentParser(description="Problem 4 acceptance on a fresh, isolated Docker Compose stack")
    parser.add_argument("--only", choices=list(SCENARIOS))
    parser.add_argument("--deadline", type=float, default=60, help="Seconds per eventual observation")
    parser.add_argument("--keep-stack", action="store_true", help="Keep isolated containers for diagnosis")
    arguments = parser.parse_args()
    if not math.isfinite(arguments.deadline) or arguments.deadline <= 0:
        parser.error("--deadline must be finite and positive")
    return run_suite(arguments)


if __name__ == "__main__":
    raise SystemExit(main())
