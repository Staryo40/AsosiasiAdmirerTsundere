from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from importlib.metadata import version
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlencode

from jsonschema import Draft202012Validator, FormatChecker

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "common"))
from testlib import (assert_status, bearer, bnpb_login, bnpb_refresh, load_dotenv,
                     metadata, pvmbg_login, repository_root, request,
                     require_value, sha256_text, timestamp, write_evidence)

ROOT = repository_root()


def prepare_results() -> Path:
    directory = ROOT / "testing/results/P01" / datetime.now().strftime("%Y%m%d-%H%M%S-%f")
    os.environ.setdefault("TEST_RESULTS_ROOT", str(directory))
    directory = Path(os.environ["TEST_RESULTS_ROOT"])
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def command_output(arguments: list[str]) -> str:
    completed = subprocess.run(arguments, cwd=ROOT, capture_output=True, text=True, timeout=300)
    if completed.returncode:
        raise AssertionError(f"Command failed with exit {completed.returncode}: {arguments[0]}")
    return completed.stdout


def canonical_validator() -> Draft202012Validator:
    schema = json.loads((ROOT / "bnpb/contracts/v1/schemas/hazard-event.schema.json").read_text())
    Draft202012Validator.check_schema(schema)
    checker = FormatChecker(formats=["date-time"])
    if checker.conforms("impossibleZ", "date-time"):
        raise AssertionError("date-time assertion is unavailable; install requirements.txt")
    return Draft202012Validator(schema, format_checker=checker)


def valid_hazard(hazard: dict[str, Any]) -> None:
    errors = list(canonical_validator().iter_errors(hazard))
    if errors:
        failure = errors[0]
        raise AssertionError(f"HazardEvent violates {failure.validator} at {list(failure.path)}")


def utc_instant(iso_timestamp: str) -> datetime:
    instant = datetime.fromisoformat(iso_timestamp.replace("Z", "+00:00"))
    if instant.tzinfo is None:
        raise AssertionError("Source timestamps require an explicit timezone")
    return instant.astimezone(timezone.utc)


def assert_equal(actual: Any, expected: Any, label: str) -> None:
    if not same_json(actual, expected):
        raise AssertionError(f"Mapping mismatch: {label}")


def same_json(actual: Any, expected: Any) -> bool:
    if isinstance(actual, bool) or isinstance(expected, bool):
        return type(actual) is type(expected) and actual == expected
    if isinstance(actual, dict) and isinstance(expected, dict):
        return actual.keys() == expected.keys() and all(same_json(actual[key], expected[key]) for key in actual)
    if isinstance(actual, list) and isinstance(expected, list):
        return len(actual) == len(expected) and all(same_json(left, right) for left, right in zip(actual, expected))
    return actual == expected


def assert_mapping(hazard: dict[str, Any], source_record: dict[str, Any],
                   warnings: list[dict[str, Any]]) -> None:
    valid_hazard(hazard)
    if hazard["source"] == "BMKG":
        assert_seismic(hazard, source_record, warnings)
    else:
        assert_volcanic(hazard, source_record)
    clock_field = "occurred_at" if hazard["source"] == "BMKG" else "reported_at"
    assert_equal(utc_instant(hazard["occurred_at"]), utc_instant(source_record[clock_field]), clock_field)
    if utc_instant(hazard["ingested_at"]) > datetime.now(timezone.utc):
        raise AssertionError("Receipt timestamp is in the future")


def source_attributes(source_record: dict[str, Any], mapped: set[str], reserved: set[str]) -> dict[str, Any]:
    attributes = {field: content for field, content in source_record.items() if field not in mapped}
    collisions = {field: attributes.pop(field) for field in reserved if field in attributes}
    if collisions:
        attributes["source_extras"] = collisions
    return attributes


def assert_seismic(hazard: dict[str, Any], event: dict[str, Any],
                   warnings: list[dict[str, Any]]) -> None:
    related = [warning for warning in warnings if warning["related_event_id"] == event["event_id"]]
    related.sort(key=lambda warning: (utc_instant(warning["issued_at"]), warning["warning_id"]))
    rank = {"Waspada": 1, "Siaga": 2, "Awas": 3}
    severity = max(related, key=lambda warning: rank[warning["threat_level"]])["threat_level"].upper() if related else (
        "NORMAL" if event["magnitude"] < 5 else "WASPADA" if event["magnitude"] < 6.5 else "SIAGA")
    expected = {"source_ref_id": event["event_id"], "hazard_type": "SEISMIC", "severity": severity,
                "area_name": event["region_name"], "latitude": event["epicenter_lat"], "longitude": event["epicenter_lon"]}
    for field, content in expected.items():
        assert_equal(hazard[field], content, field)
    attributes = source_attributes(event, {"event_id", "region_name", "epicenter_lat", "epicenter_lon", "occurred_at"},
                                   {"tsunami_warnings", "source_extras"})
    if related:
        attributes["tsunami_warnings"] = related
    assert_equal(hazard["attributes"], attributes, "seismic attributes and warning correlation")


def assert_volcanic(hazard: dict[str, Any], report: dict[str, Any]) -> None:
    reference = json.loads((ROOT / "bnpb/internal/aggregator/reference/volcanoes.json").read_text())
    volcano = reference[report["volcano_id"]]
    expected = {"source_ref_id": report["report_id"], "hazard_type": "VOLCANIC", "severity": report["alert_level"].upper(),
                "area_name": volcano["name"], "latitude": volcano["latitude"], "longitude": volcano["longitude"]}
    for field, content in expected.items():
        assert_equal(hazard[field], content, field)
    attributes = source_attributes(report, {"report_id", "reported_at", "alert_level"}, {"source_extras"})
    assert_equal(hazard["attributes"], attributes, "volcanic attributes, including confidence presence")


def wait_until(label: str, observation: Callable[[], Any], deadline: float) -> Any:
    expires = time.monotonic() + deadline
    while time.monotonic() < expires:
        observed = observation()
        if observed is not None:
            return observed
        time.sleep(0.5)
    raise AssertionError(f"Timed out waiting for {label} after {deadline}s")


def container_snapshot() -> dict[str, Any]:
    identities = command_output(["docker", "compose", "ps", "-q"]).split()
    if not identities:
        raise AssertionError("No running Compose containers")
    template = ('{"id":{{json .Id}},"service":{{json (index .Config.Labels "com.docker.compose.service")}},'
                '"started_at":{{json .State.StartedAt}},"running":{{json .State.Running}},'
                '"restart_count":{{json .RestartCount}},"image":{{json .Image}}}')
    rows = command_output(["docker", "inspect", "--format", template, *identities]).splitlines()
    snapshot = {row["service"]: row for row in map(json.loads, rows)}
    if not {"bmkg", "pvmbg", "aggregator", "api", "auth", "postgres"} <= snapshot.keys():
        raise AssertionError("Required Compose services are missing")
    return snapshot


def unchanged_containers(before: dict[str, Any], after: dict[str, Any]) -> None:
    assert_equal(after, before, "container identities/start times/restart counts/images")
    if not all(container["running"] for container in after.values()):
        raise AssertionError("A service stopped during schema evolution")


def poll_overlap_seconds() -> float:
    environment = json.loads(command_output(["docker", "inspect", "--format", "{{json .Config.Env}}",
                                            container_snapshot()["aggregator"]["id"]]))
    settings = dict(entry.split("=", 1) for entry in environment)
    duration = settings.get("BNPB_POLL_OVERLAP", "2s")
    units = {"ns": 1e-9, "us": 1e-6, "µs": 1e-6, "μs": 1e-6, "ms": .001, "s": 1, "m": 60, "h": 3600}
    parts = re.findall(r"(\d+(?:\.\d+)?|\.\d+)(ns|us|µs|μs|ms|s|m|h)", duration)
    if "".join(number + unit for number, unit in parts) != duration:
        raise AssertionError("Unsupported BNPB_POLL_OVERLAP duration")
    return sum(float(number) * units[unit] for number, unit in parts)


def source_fingerprints() -> dict[str, str]:
    paths = [path for folder in ("bnpb/internal", "bmkg/src", "pvmbg/src/main")
             for path in (ROOT / folder).rglob("*") if path.is_file() and not path.name.endswith("_test.go")]
    paths.append(ROOT / "bnpb/contracts/v1/schemas/hazard-event.schema.json")
    return {path.relative_to(ROOT).as_posix(): sha256_text(path.read_text(encoding="utf-8")) for path in paths}


def schema_fingerprint() -> str:
    identity = container_snapshot()["postgres"]["id"]
    environment = json.loads(command_output(["docker", "inspect", "--format", "{{json .Config.Env}}", identity]))
    settings = dict(entry.split("=", 1) for entry in environment)
    sql = """SELECT json_build_object(
        'migrations', (SELECT json_agg(m ORDER BY version) FROM schema_migrations m),
        'columns', (SELECT json_agg(c ORDER BY table_name, ordinal_position) FROM
            (SELECT table_name, ordinal_position, column_name, data_type, is_nullable, column_default
             FROM information_schema.columns WHERE table_schema='public') c))"""
    output = command_output(["docker", "compose", "exec", "-T", "postgres", "psql", "-X", "-v", "ON_ERROR_STOP=1",
                             "-U", settings["POSTGRES_USER"], "-d", settings["POSTGRES_DB"], "-Atc", sql])
    return sha256_text(output.strip())


class LiveClients:
    def __init__(self) -> None:
        self.api = os.getenv("API_BASE", "http://localhost:8080").rstrip("/")
        self.bmkg = os.getenv("BMKG_BASE", "http://localhost:8081").rstrip("/")
        self.pvmbg = os.getenv("PVMBG_BASE", "http://localhost:8082").rstrip("/")
        self.bn_env = load_dotenv(ROOT / "bnpb/.env")
        self.pv_env = load_dotenv(ROOT / "pvmbg/.env")
        self.bm_key = require_value(load_dotenv(ROOT / "bmkg/.env"), "BMKG_API_KEY")
        self.session = bnpb_login(self.api, require_value(self.bn_env, "BNPB_OPS_CLIENT_ID"),
                                  require_value(self.bn_env, "BNPB_OPS_CLIENT_SECRET"))
        self.reader = self.pv_login("READER")
        self.admin = self.pv_login("ADMIN")

    def pv_login(self, role: str) -> str:
        return pvmbg_login(self.pvmbg, require_value(self.pv_env, f"PVMBG_{role}_CLIENT_ID"),
                           require_value(self.pv_env, f"PVMBG_{role}_CLIENT_SECRET"))["access_token"]

    def api_json(self, path: str) -> dict[str, Any]:
        response = request("GET", self.api + path, headers=bearer(self.session["access_token"]))
        if response.status == 401:
            refreshed = bnpb_refresh(self.api, self.session["refresh_token"])
            assert_status(refreshed, 200, "OPS refresh")
            self.session = refreshed.json()
            response = request("GET", self.api + path, headers=bearer(self.session["access_token"]))
        assert_status(response, 200, path)
        return response.json()

    def reports(self, since: str) -> list[dict[str, Any]]:
        response = request("GET", self.pvmbg + "/volcanic-reports?" + urlencode({"since": since}), headers=bearer(self.reader))
        if response.status == 401:
            self.reader = self.pv_login("READER")
            response = request("GET", self.pvmbg + "/volcanic-reports?" + urlencode({"since": since}), headers=bearer(self.reader))
        assert_status(response, 200, "PVMBG reports")
        return response.json()

    def bmkg_records(self, path: str) -> list[dict[str, Any]]:
        response = request("GET", self.bmkg + path, headers={"X-BMKG-Key": self.bm_key})
        assert_status(response, 200, path)
        return response.json()

    def schema_version(self, version: int) -> None:
        response = request("POST", self.pvmbg + "/admin/schema-version", headers=bearer(self.admin), body={"version": version})
        if response.status == 401:
            self.admin = self.pv_login("ADMIN")
            response = request("POST", self.pvmbg + "/admin/schema-version", headers=bearer(self.admin), body={"version": version})
        assert_status(response, 204, "PVMBG schema toggle")

    def find_hazard(self, source: str, source_ref: str, occurred_at: str) -> dict[str, Any] | None:
        # Bound the witness window; P1 does not need a large historical snapshot under the read deadline.
        instant = utc_instant(occurred_at)
        filters = {"source": source, "view": "full", "limit": 100,
                   "occurred_from": instant.replace(microsecond=0).isoformat(), "occurred_to": instant.isoformat()}
        page = self.api_json("/v1/hazards?" + urlencode(filters))
        for hazard in page["items"]:
            if hazard["source_ref_id"] == source_ref:
                return hazard
        return None


def mapped_observation(clients: LiveClients, source: str, record: dict[str, Any],
                       warnings: list[dict[str, Any]]) -> dict[str, Any] | None:
    identity = record["event_id" if source == "BMKG" else "report_id"]
    clock_field = "occurred_at" if source == "BMKG" else "reported_at"
    hazard = clients.find_hazard(source, identity, record[clock_field])
    if hazard is None:
        return None
    if source == "PVMBG" and ("confidence_level" in hazard["attributes"]) != ("confidence_level" in record):
        return None
    if source == "BMKG":
        expected_ids = {warning["warning_id"] for warning in warnings if warning["related_event_id"] == identity}
        actual_ids = {warning["warning_id"] for warning in hazard["attributes"].get("tsunami_warnings", [])}
        if expected_ids != actual_ids:
            return None
    assert_mapping(hazard, record, warnings)
    return hazard


def fresh_report(clients: LiveClients, since: str) -> dict[str, Any] | None:
    reports = clients.reports(since)
    return reports[-1] if reports else None


def run_live(evidence: dict[str, Any], deadline: float) -> None:
    clients = LiveClients()
    baseline, original_version = evolution_baseline(clients, evidence, deadline)
    try:
        clients.schema_version(1)
        collect_live_phases(clients, evidence, deadline)
        evidence["containers_after"] = container_snapshot()
        unchanged_containers(baseline, evidence["containers_after"])
    finally:
        restore_evolution(clients, original_version, baseline, evidence)


def evolution_baseline(clients: LiveClients, evidence: dict[str, Any], deadline: float) -> tuple[dict[str, Any], int]:
    baseline = container_snapshot()
    evidence["containers_before"] = baseline
    evidence["source_fingerprints_before"] = source_fingerprints()
    evidence["schema_fingerprint_before"] = schema_fingerprint()
    recent = (datetime.now(timezone.utc).timestamp() - 120)
    since = datetime.fromtimestamp(recent, timezone.utc).isoformat()
    initial = wait_until("current PVMBG schema", lambda: fresh_report(clients, since), deadline)
    original_version = 2 if "confidence_level" in initial else 1
    evidence["original_schema_version"] = original_version
    return baseline, original_version


def restore_evolution(clients: LiveClients, original_version: int, baseline: dict[str, Any], evidence: dict[str, Any]) -> None:
    clients.schema_version(original_version)
    evidence["restored_schema_version"] = original_version
    evidence["containers_after_cleanup"] = container_snapshot()
    unchanged_containers(baseline, evidence["containers_after_cleanup"])
    evidence["source_fingerprints_after"] = source_fingerprints()
    evidence["schema_fingerprint_after"] = schema_fingerprint()
    assert_equal(evidence["source_fingerprints_after"], evidence["source_fingerprints_before"], "source code unchanged")
    assert_equal(evidence["schema_fingerprint_after"], evidence["schema_fingerprint_before"], "schema and migration history unchanged")


def collect_live_phases(clients: LiveClients, evidence: dict[str, Any], deadline: float) -> None:
    before_time = datetime.now(timezone.utc).isoformat()
    old_report = wait_until("new V1 report", lambda: fresh_report(clients, before_time), deadline)
    evidence["v1_source"] = old_report
    if "confidence_level" in old_report:
        raise AssertionError("Schema V1 still includes confidence_level")
    old_hazard = wait_until("V1 ingest", lambda: mapped_observation(clients, "PVMBG", old_report, []), deadline)
    seismic = clients.bmkg_records("/seismic-events")[-1]
    warnings = clients.bmkg_records("/tsunami-warnings")
    seismic_hazard = wait_until("BMKG mapping", lambda: mapped_observation(clients, "BMKG", seismic, warnings), deadline)
    evidence["before"] = {"pvmbg": old_report, "volcanic_hazard": old_hazard, "bmkg": seismic,
                          "related_warnings": [warning for warning in warnings if warning["related_event_id"] == seismic["event_id"]], "seismic_hazard": seismic_hazard}
    preserve_old_record(clients, old_report, evidence, deadline)
    evidence["toggle_at"] = timestamp()
    clients.schema_version(2)
    collect_new_schema(clients, evidence, deadline, old_hazard)


def preserve_old_record(clients: LiveClients, old_report: dict[str, Any], evidence: dict[str, Any], deadline: float) -> None:
    # Advance beyond overlap so the toggle cannot legitimately re-fetch and enrich the V1 witness.
    overlap = poll_overlap_seconds()
    boundary = utc_instant(old_report["reported_at"]).timestamp() + overlap
    since = datetime.fromtimestamp(boundary, timezone.utc).isoformat()
    witness = wait_until("V1 checkpoint beyond overlap", lambda: fresh_report(clients, since), deadline)
    wait_until("V1 witness ingest", lambda: mapped_observation(clients, "PVMBG", witness, []), deadline)
    evidence["poll_overlap_seconds"] = overlap
    evidence["v1_checkpoint_witness"] = witness


def collect_new_schema(clients: LiveClients, evidence: dict[str, Any], deadline: float,
                       old_hazard: dict[str, Any]) -> None:
    since = datetime.now(timezone.utc).isoformat()
    new_report = wait_until("new V2 report", lambda: fresh_report(clients, since), deadline)
    evidence["v2_source"] = new_report
    if "confidence_level" not in new_report:
        raise AssertionError("Schema V2 lacks confidence_level")
    new_hazard = wait_until("V2 ingest", lambda: mapped_observation(clients, "PVMBG", new_report, []), deadline)
    old_detail = clients.api_json(f"/v1/hazards/{old_hazard['hazard_id']}?view=full")["item"]
    assert_equal(old_detail, old_hazard, "V1 record remains readable and unchanged after V2")
    detail = clients.api_json(f"/v1/hazards/{new_hazard['hazard_id']}?view=full")["item"]
    assert_equal(detail, new_hazard, "list/detail canonical consistency")
    evidence["after"] = {"pvmbg": new_report, "volcanic_hazard": new_hazard, "old_hazard": old_detail}


def run_scenario(test_id: str, scenario: Callable[[dict[str, Any]], None]) -> int:
    prepare_results()
    started = timestamp()
    evidence: dict[str, Any] = {"result": "FAIL", "command": sys.argv, "python_version": sys.version.split()[0],
                               "validator_version": version("jsonschema"),
                               "compose_project": os.getenv("COMPOSE_PROJECT_NAME", "default"),
                               "compose_files": os.getenv("COMPOSE_FILE", "compose.yaml + automatic override"),
                               "working_tree_dirty": bool(command_output(["git", "status", "--porcelain"])), "cases": []}
    try:
        scenario(evidence)
        evidence["result"] = "PASS"
    except (AssertionError, OSError, ValueError, subprocess.SubprocessError) as failure:
        evidence["failure"] = str(failure)
    finally:
        write_evidence(test_id, metadata(test_id, started) | evidence)
    print(f"{test_id}: {evidence['result']}")
    if "failure" in evidence:
        print(evidence["failure"])
    return 0 if evidence["result"] == "PASS" else 1
