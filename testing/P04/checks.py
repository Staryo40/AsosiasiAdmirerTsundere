from __future__ import annotations

import json
import subprocess
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Reuse the P1 source/mapping oracle rather than define a second schema policy.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from testing.P01.acceptance import (LiveClients, assert_equal, assert_status, assert_volcanic,
                                   bearer, container_snapshot, evolution_baseline, fresh_report,
                                   poll_overlap_seconds, repository_root, request, restore_evolution,
                                   sha256_text, timestamp, unchanged_containers, utc_instant,
                                   valid_hazard, wait_until)

ROOT = repository_root()
REQUIRED = {"bmkg", "pvmbg", "aggregator", "api", "auth", "postgres",
            "rabbitmq", "consumer-1", "consumer-2"}


def cli(arguments: list[str], timeout: float = 300, stdin: str | None = None) -> str:
    completed = subprocess.run(arguments, cwd=ROOT, input=stdin, capture_output=True,
                               text=True, encoding="utf-8", errors="replace", timeout=timeout)
    if completed.returncode:
        raise AssertionError(f"{' '.join(arguments[:3])} failed: exit {completed.returncode}")
    return completed.stdout


def containers() -> dict[str, Any]:
    identities = cli(["docker", "compose", "ps", "--all", "--quiet"]).split()
    if not identities:
        raise AssertionError("No containers in the selected Compose project")
    inspected = json.loads(cli(["docker", "inspect", *identities]))
    by_service = {container["Config"]["Labels"]["com.docker.compose.service"]: container
                  for container in inspected}
    if len(by_service) != len(inspected):
        raise AssertionError("P4 expects one container per service, not scaled replicas")
    if not REQUIRED <= by_service.keys():
        raise AssertionError(f"Missing services: {sorted(REQUIRED - by_service.keys())}")
    return by_service


def snapshot(inspected: dict[str, Any]) -> dict[str, Any]:
    return {service: {"id": container["Id"], "image": container["Image"],
                      "started_at": container["State"]["StartedAt"],
                      "running": container["State"]["Running"],
                      "restart_count": container["RestartCount"]}
            for service, container in inspected.items()}


def ready_containers(inspected: dict[str, Any]) -> None:
    for service in REQUIRED:
        state = inspected[service]["State"]
        if not state["Running"] or state.get("Health", {}).get("Status") != "healthy":
            raise AssertionError(f"{service} must be running and healthy")
    if len({inspected[service]["Id"] for service in REQUIRED}) != len(REQUIRED):
        raise AssertionError("Components do not have distinct containers")


def unchanged_peers(before: dict[str, Any], after: dict[str, Any]) -> None:
    peers_before = {service: identity for service, identity in before.items() if service != "consumer-1"}
    peers_after = {service: identity for service, identity in after.items() if service != "consumer-1"}
    assert_equal(peers_after, peers_before, "all non-target containers remain unchanged")


def seismic_sample(clients: LiveClients, phase: str) -> dict[str, Any]:
    started = time.monotonic()
    page = clients.api_json("/v1/hazards?source=BMKG&view=full&limit=5")
    hazards = page["items"]
    if not hazards or any(hazard["source"] != "BMKG" for hazard in hazards):
        raise AssertionError("Independent BMKG request must return stored BMKG hazards")
    return {"phase": phase, "at": timestamp(), "status": 200,
            "duration_ms": round((time.monotonic() - started) * 1000, 2),
            "hazard_ids": [hazard["hazard_id"] for hazard in hazards]}


def monitored_command(arguments: list[str], clients: LiveClients,
                      evidence: dict[str, Any], phase: str) -> None:
    action = {"command": arguments, "started_at": timestamp(), "phase": phase}
    evidence.setdefault("actions", []).append(action)
    with ThreadPoolExecutor(max_workers=1) as executor:
        pending = executor.submit(cli, arguments)
        while not pending.done():
            evidence["requests"].append(seismic_sample(clients, phase))
            time.sleep(0.1)
        pending.result()
    action["finished_at"] = timestamp()
    if not any(sample["phase"] == phase for sample in evidence["requests"]):
        raise AssertionError(f"No HTTP observation during {phase}")


def independent_rebuild(evidence: dict[str, Any], deadline: float) -> None:
    clients = LiveClients()
    before = containers()
    ready_containers(before)
    evidence["before"] = snapshot(before)
    evidence["requests"] = [seismic_sample(clients, "baseline")]
    try:
        rebuild_consumer(clients, evidence, deadline)
    finally:
        cli(["docker", "compose", "up", "-d", "--no-deps", "--no-build", "consumer-1"])
        evidence["after_cleanup"] = snapshot(containers())
        unchanged_peers(evidence["before"], evidence["after_cleanup"])


def rebuild_consumer(clients: LiveClients, evidence: dict[str, Any], deadline: float) -> None:
    monitored_command(["docker", "compose", "stop", "consumer-1"], clients, evidence, "stop")
    stopped = containers()
    evidence["stopped"] = snapshot(stopped)
    if stopped["consumer-1"]["State"]["Running"]:
        raise AssertionError("Target consumer did not stop")
    unchanged_peers(evidence["before"], evidence["stopped"])
    for _ in range(3):
        evidence["requests"].append(seismic_sample(clients, "offline"))
    monitored_command(["docker", "compose", "build", "consumer-1"], clients, evidence, "build")
    monitored_command(["docker", "compose", "up", "-d", "--no-deps", "--no-build",
                       "--force-recreate", "consumer-1"], clients, evidence, "start")
    after = wait_until("consumer ready", healthy_snapshot, deadline)
    evidence["after"] = snapshot(after)
    assert_recreated(evidence["before"], evidence["after"])
    assert_equal(after["consumer-1"]["Mounts"], stopped["consumer-1"]["Mounts"], "consumer volume preserved")


def healthy_snapshot() -> dict[str, Any] | None:
    inspected = containers()
    state = inspected["consumer-1"]["State"]
    return inspected if state["Running"] and state.get("Health", {}).get("Status") == "healthy" else None


def assert_recreated(before: dict[str, Any], after: dict[str, Any]) -> None:
    unchanged_peers(before, after)
    if after["consumer-1"]["id"] == before["consumer-1"]["id"]:
        raise AssertionError("Target container was not recreated")
    if after["consumer-1"]["started_at"] == before["consumer-1"]["started_at"]:
        raise AssertionError("Target start time did not change")


def environment(container: dict[str, Any]) -> dict[str, str]:
    return dict(entry.split("=", 1) for entry in container["Config"]["Env"] if "=" in entry)


def assert_private_store(inspected: dict[str, Any]) -> dict[str, Any]:
    postgres = inspected["postgres"]
    networks = postgres["NetworkSettings"]["Networks"]
    if len(networks) != 1:
        raise AssertionError("Canonical Store must have exactly one private network")
    if postgres["HostConfig"].get("PortBindings"):
        raise AssertionError("Canonical Store publishes a host port")
    network_name = next(iter(networks))
    owners = {service for service, container in inspected.items()
              if network_name in container["NetworkSettings"]["Networks"]}
    if owners != {"postgres", "aggregator"}:
        raise AssertionError(f"Unexpected Canonical Store network members: {sorted(owners)}")
    assert_store_credentials(inspected)
    return {"network": network_name, "owners": sorted(owners),
            "host_port_bindings": {}, "database_mounts": postgres["Mounts"]}


def assert_store_credentials(inspected: dict[str, Any]) -> None:
    postgres = inspected["postgres"]
    password = environment(postgres)["POSTGRES_PASSWORD"]
    store_sources = {mount["Source"] for mount in postgres["Mounts"]}
    for service, container in inspected.items():
        if service in {"postgres", "aggregator"}:
            continue
        for name, setting in environment(container).items():
            if "DATABASE" in name or name.startswith(("POSTGRES_", "PGPASSWORD", "PGHOST")):
                raise AssertionError(f"{service} has a database setting: {name}")
            if password and password in setting:
                raise AssertionError(f"{service} has the canonical password in {name}")
        if store_sources & {mount["Source"] for mount in container["Mounts"]}:
            raise AssertionError(f"{service} mounts Canonical Store files")
        if any("docker.sock" in mount["Destination"] for mount in container["Mounts"]):
            raise AssertionError(f"{service} can bypass isolation through the Docker socket")


def probe_exit(arguments: list[str]) -> int:
    completed = subprocess.run(arguments, cwd=ROOT, capture_output=True, timeout=15)
    # pg_isready: 0=accepting, 1=rejecting, 2=no response, 3=invalid invocation.
    if completed.returncode not in {0, 1, 2}:
        raise AssertionError(f"Database probe could not execute: exit {completed.returncode}")
    return completed.returncode


def store_probe(container: dict[str, Any], postgres: dict[str, Any], address: str) -> dict[str, Any]:
    arguments = ["docker", "run", "--rm", "--network", f"container:{container['Id']}",
                 "--entrypoint", "pg_isready", postgres["Image"], "-h", address, "-p", "5432", "-t", "2"]
    return {"address": address, "exit_code": probe_exit(arguments), "at": timestamp()}


def store_isolation(evidence: dict[str, Any], deadline: float) -> None:
    inspected = containers()
    ready_containers(inspected)
    evidence["containers"] = snapshot(inspected)
    evidence["isolation"] = assert_private_store(inspected)
    network = json.loads(cli(["docker", "network", "inspect", evidence["isolation"]["network"]]))[0]
    if not network["Internal"] or set(network["Containers"]) != {inspected[name]["Id"] for name in ("postgres", "aggregator")}:
        raise AssertionError("Runtime store network is not internal or contains an outsider")
    addresses = ["postgres", next(iter(inspected["postgres"]["NetworkSettings"]["Networks"].values()))["IPAddress"]]
    evidence["probes"] = {}
    for service in sorted(REQUIRED - {"postgres"}):
        probes = [store_probe(inspected[service], inspected["postgres"], address) for address in addresses]
        evidence["probes"][service] = probes
        expected = 0 if service == "aggregator" else 2
        if any(probe["exit_code"] != expected for probe in probes):
            raise AssertionError(f"{service}: expected pg_isready exit {expected} for DNS and direct IP")
    assert_equal(store_probe(inspected["aggregator"], inspected["postgres"], addresses[1])["exit_code"], 0,
                 "positive control after negative probes")
    evidence["api_trace"] = aggregator_trace(LiveClients(), deadline)


def aggregator_trace(clients: LiveClients, deadline: float) -> dict[str, Any]:
    correlation_id = "p4-" + uuid.uuid4().hex
    headers = bearer(clients.session["access_token"]) | {"X-Correlation-ID": correlation_id}
    response = request("GET", clients.api + "/v1/hazards?source=BMKG&view=full&limit=1", headers=headers)
    assert_status(response, 200, "Client-Facing API through Aggregator")
    if not response.json()["items"]:
        raise AssertionError("API trace must include a stored hazard")
    logs = wait_until("correlated API -> Aggregator network call",
                      lambda: correlated_logs(correlation_id), deadline)
    return {"correlation_id": correlation_id, "status": response.status, "logs": logs}


def correlated_logs(correlation_id: str) -> dict[str, Any] | None:
    matching = {}
    for service in ("api", "aggregator"):
        output = cli(["docker", "compose", "logs", "--no-color", "--no-log-prefix", "--since", "2m", service])
        rows = [json.loads(line) for line in output.splitlines() if line.startswith("{")]
        selected = [row for row in rows if row.get("correlation_id") == correlation_id]
        matching[service] = [{key: row[key] for key in ("event", "path", "url", "status", "duration_ms") if key in row}
                             for row in selected]
    outgoing = [row for row in matching["api"] if row.get("event") == "outbound_http"
                and "/internal/hazards" in row.get("url", "") and row.get("status") == 200]
    incoming = [row for row in matching["aggregator"] if row.get("event") == "http_request"
                and row.get("path") == "/internal/hazards" and row.get("status") == 200]
    return matching if outgoing and incoming else None


def sql_json(statement: str, variables: dict[str, str] | None = None) -> Any:
    settings = environment(containers()["postgres"])
    arguments = ["docker", "compose", "exec", "-T", "postgres", "psql", "-X", "-qAt",
                 "-v", "ON_ERROR_STOP=1", "-U", settings["POSTGRES_USER"], "-d", settings["POSTGRES_DB"]]
    for name, setting in (variables or {}).items():
        arguments.extend(["-v", f"{name}={setting}"])
    return json.loads(cli(arguments, stdin="SET default_transaction_read_only=on;\n" + statement))


def schema_definition() -> dict[str, Any]:
    return sql_json("""SELECT json_build_object(
        'migrations', (SELECT json_agg(m ORDER BY version) FROM schema_migrations m),
        'columns', (SELECT json_agg(c ORDER BY table_name, ordinal_position) FROM
            (SELECT table_name, ordinal_position, column_name, udt_name, is_nullable, column_default
             FROM information_schema.columns WHERE table_schema='public') c),
        'constraints', (SELECT json_agg(c ORDER BY relation, conname) FROM
            (SELECT conrelid::regclass::text AS relation, conname, pg_get_constraintdef(oid) AS definition
             FROM pg_constraint WHERE connamespace='public'::regnamespace) c),
        'indexes', (SELECT json_agg(i ORDER BY tablename, indexname) FROM
            (SELECT tablename, indexname, indexdef FROM pg_indexes WHERE schemaname='public') i),
        'triggers', (SELECT json_agg(t ORDER BY relation, tgname) FROM
            (SELECT tgrelid::regclass::text AS relation, tgname, pg_get_triggerdef(oid) AS definition
             FROM pg_trigger WHERE NOT tgisinternal AND tgrelid IN
             (SELECT oid FROM pg_class WHERE relnamespace='public'::regnamespace)) t),
        'functions', (SELECT json_agg(f ORDER BY definition) FROM
            (SELECT pg_get_functiondef(oid) AS definition FROM pg_proc
             WHERE pronamespace='public'::regnamespace AND prokind='f') f));""")


def stored_witnesses(old_id: str, new_id: str) -> list[dict[str, Any]]:
    return sql_json("""SELECT COALESCE(json_agg(h ORDER BY hazard_id), '[]'::json) FROM
        (SELECT hazard_id, source, source_ref_id, attributes FROM hazard_events
         WHERE hazard_id IN (:'old_id', :'new_id')) h;""", {"old_id": old_id, "new_id": new_id})


def assert_witnesses(stored: list[dict[str, Any]], old: dict[str, Any], new: dict[str, Any]) -> None:
    # Missing witnesses, duplicate identities, absent/zero/null confidence must differ.
    by_id = {hazard["hazard_id"]: hazard for hazard in stored}
    if old["hazard_id"] == new["hazard_id"] or len(stored) != 2 or len(by_id) != 2:
        raise AssertionError("Expected two distinct persisted V1/V2 hazards")
    for witness in (old, new):
        expected = {key: witness[key] for key in ("hazard_id", "source", "source_ref_id", "attributes")}
        assert_equal(by_id.get(witness["hazard_id"]), expected, "SQL row and API witness match")
    if "confidence_level" in old["attributes"] or "confidence_level" not in new["attributes"]:
        raise AssertionError("V1 lacks confidence and V2 retains it")
    confidence = new["attributes"]["confidence_level"]
    if isinstance(confidence, bool) or not isinstance(confidence, (int, float)) or not 0 <= confidence <= 1:
        raise AssertionError("Stored confidence must be a number in [0, 1]")


def flexible_storage(evidence: dict[str, Any], deadline: float) -> None:
    clients = LiveClients()
    before = schema_definition()
    evidence["schema_before"] = before
    baseline, original_version = evolution_baseline(clients, evidence, deadline)
    try:
        clients.schema_version(1)
        collect_storage_evidence(clients, evidence, deadline)
        assert_storage_structure(before)
    finally:
        try:
            restore_evolution(clients, original_version, baseline, evidence)
        finally:
            evidence["schema_after"] = schema_definition()
            assert_equal(evidence["schema_after"], before, "columns/constraints/indexes/triggers/functions/migrations unchanged")
            evidence["schema_sha256"] = sha256_text(json.dumps(before, sort_keys=True))


def collect_storage_evidence(clients: LiveClients, evidence: dict[str, Any], deadline: float) -> None:
    collect_storage_phases(clients, evidence, deadline)
    old, new = evidence["after"]["old_hazard"], evidence["after"]["volcanic_hazard"]
    # Cleanup re-renders overlapping source records as V1; inspect the V2 state first.
    evidence["stored_witnesses"] = stored_witnesses(old["hazard_id"], new["hazard_id"])
    evidence["storage_observed_at"] = timestamp()
    assert_witnesses(evidence["stored_witnesses"], old, new)
    evidence["containers_after"] = container_snapshot()
    unchanged_containers(evidence["containers_before"], evidence["containers_after"])


def observed_report(clients: LiveClients, report: dict[str, Any]) -> dict[str, Any] | None:
    hazard = clients.find_hazard("PVMBG", report["report_id"], report["reported_at"])
    if hazard is None or ("confidence_level" in hazard["attributes"]) != ("confidence_level" in report):
        return None
    valid_hazard(hazard)
    assert_volcanic(hazard, report)
    assert_equal(utc_instant(hazard["occurred_at"]), utc_instant(report["reported_at"]), "source timestamp")
    observed_clock = sql_json("SELECT to_json(clock_timestamp());")
    assert_receipt_clock(hazard, observed_clock)
    return hazard


def assert_receipt_clock(hazard: dict[str, Any], observed_clock: str) -> None:
    # Docker services share the store's clock; the Windows observer may be skewed.
    if utc_instant(hazard["ingested_at"]) > utc_instant(observed_clock):
        raise AssertionError("Receipt timestamp exceeds the observed Canonical Store clock")


def collect_storage_phases(clients: LiveClients, evidence: dict[str, Any], deadline: float) -> None:
    since = datetime.now(timezone.utc).isoformat()
    old_report = wait_until("new stored V1 report", lambda: fresh_report(clients, since), deadline)
    if "confidence_level" in old_report:
        raise AssertionError("Schema V1 still includes confidence_level")
    old_hazard = wait_until("V1 round trip", lambda: observed_report(clients, old_report), deadline)
    evidence["before"] = {"pvmbg": old_report, "volcanic_hazard": old_hazard}
    advance_old_checkpoint(clients, old_report, evidence, deadline)
    evidence["toggle_at"] = timestamp()
    clients.schema_version(2)
    collect_new_storage(clients, old_hazard, evidence, deadline)


def advance_old_checkpoint(clients: LiveClients, report: dict[str, Any],
                           evidence: dict[str, Any], deadline: float) -> None:
    overlap = poll_overlap_seconds()
    boundary = utc_instant(report["reported_at"]).timestamp() + overlap
    since = datetime.fromtimestamp(boundary, timezone.utc).isoformat()
    witness = wait_until("V1 checkpoint beyond overlap", lambda: fresh_report(clients, since), deadline)
    wait_until("V1 checkpoint ingest", lambda: observed_report(clients, witness), deadline)
    evidence["poll_overlap_seconds"] = overlap
    evidence["v1_checkpoint_witness"] = witness


def collect_new_storage(clients: LiveClients, old_hazard: dict[str, Any],
                        evidence: dict[str, Any], deadline: float) -> None:
    since = datetime.now(timezone.utc).isoformat()
    report = wait_until("new stored V2 report", lambda: fresh_report(clients, since), deadline)
    if "confidence_level" not in report:
        raise AssertionError("Schema V2 lacks confidence_level")
    new_hazard = wait_until("V2 round trip", lambda: observed_report(clients, report), deadline)
    old_detail = clients.api_json(f"/v1/hazards/{old_hazard['hazard_id']}?view=full")["item"]
    new_detail = clients.api_json(f"/v1/hazards/{new_hazard['hazard_id']}?view=full")["item"]
    assert_equal(old_detail, old_hazard, "V1 remains readable and unchanged")
    assert_equal(new_detail, new_hazard, "V2 list/detail consistency")
    evidence["after"] = {"pvmbg": report, "volcanic_hazard": new_detail, "old_hazard": old_detail}
    evidence["receipt_clock"] = {"basis": "Canonical Store clock_timestamp", "observed_at": sql_json("SELECT to_json(clock_timestamp());")}


def assert_storage_structure(definition: dict[str, Any]) -> None:
    columns = {column["column_name"]: column for column in definition["columns"]
               if column["table_name"] == "hazard_events"}
    if columns["attributes"]["udt_name"] != "jsonb" or "confidence_level" in columns:
        raise AssertionError("Dynamic attributes must use JSONB without a confidence column")
    constraints = [constraint["definition"] for constraint in definition["constraints"]
                   if constraint["relation"] == "hazard_events"]
    if "UNIQUE (source, source_ref_id)" not in constraints:
        raise AssertionError("Source identity must be unique")
    indexes = [index["indexdef"] for index in definition["indexes"] if index["tablename"] == "hazard_events"]
    if not any("source, occurred_at DESC" in index for index in indexes):
        raise AssertionError("Source/time pagination index is missing")
    mounts = containers()["postgres"]["Mounts"]
    if not any(mount["Type"] == "volume" and mount["Destination"] == "/var/lib/postgresql" for mount in mounts):
        raise AssertionError("Canonical Store lacks a persistent named volume")
