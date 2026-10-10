from __future__ import annotations

import copy
import subprocess
import unittest
from unittest.mock import patch

import checks
import run_all


def identities() -> dict:
    return {service: {"id": service, "image": "image", "started_at": "before", "running": True,
                      "restart_count": 0} for service in checks.REQUIRED}


def isolated_containers() -> dict:
    inspected = {service: {"Config": {"Env": []}, "NetworkSettings": {"Networks": {"public": {}}},
                           "HostConfig": {"PortBindings": {}}, "Mounts": []}
                 for service in checks.REQUIRED}
    for service in ("postgres", "aggregator"):
        inspected[service]["NetworkSettings"]["Networks"] = {"private": {}}
    inspected["postgres"]["Config"]["Env"] = ["POSTGRES_PASSWORD=unique-private-password"]
    inspected["postgres"]["Mounts"] = [{"Source": "canonical-volume", "Destination": "/var/lib/postgresql"}]
    return inspected


class ContainerOracleTests(unittest.TestCase):
    def test_rebuild_requires_target_recreation_and_unchanged_peers(self) -> None:
        before, after = identities(), identities()
        with self.assertRaises(AssertionError):
            checks.assert_recreated(before, after)
        after["consumer-1"].update(id="recreated", started_at="after")
        checks.assert_recreated(before, after)
        for field, changed in (("id", "other"), ("image", "new-image"), ("started_at", "after"),
                               ("restart_count", 1), ("running", False)):
            with self.subTest(field=field):
                broken = copy.deepcopy(after)
                broken["api"][field] = changed
                with self.assertRaises(AssertionError):
                    checks.assert_recreated(before, broken)

    def test_store_rejects_network_port_credential_and_mount_bypasses(self) -> None:
        checks.assert_private_store(isolated_containers())
        for bypass in ("network", "port", "credential", "password", "mount", "socket"):
            with self.subTest(bypass=bypass):
                inspected = isolated_containers()
                self.add_bypass(inspected, bypass)
                with self.assertRaises(AssertionError):
                    checks.assert_private_store(inspected)

    def add_bypass(self, inspected: dict, bypass: str) -> None:
        if bypass == "network":
            inspected["api"]["NetworkSettings"]["Networks"]["private"] = {}
        elif bypass == "port":
            inspected["postgres"]["HostConfig"]["PortBindings"] = {"5432/tcp": [{}]}
        elif bypass == "credential":
            inspected["api"]["Config"]["Env"] = ["BNPB_CANONICAL_DATABASE_URL=postgres://db"]
        elif bypass == "password":
            inspected["api"]["Config"]["Env"] = ["UNEXPECTED_SECRET=unique-private-password"]
        else:
            inspected["api"]["Mounts"] = [{"Source": "canonical-volume" if bypass == "mount" else "socket",
                                           "Destination": "/db" if bypass == "mount" else "/var/run/docker.sock"}]

    def test_probe_execution_failure_cannot_prove_isolation(self) -> None:
        for exit_code in (0, 1, 2, 3, 125, 127):
            with self.subTest(exit_code=exit_code), patch("checks.subprocess.run", return_value=
                    subprocess.CompletedProcess(["docker"], exit_code)):
                if exit_code in {0, 1, 2}:
                    self.assertEqual(checks.probe_exit(["docker"]), exit_code)
                else:
                    with self.assertRaises(AssertionError):
                        checks.probe_exit(["docker"])

    def test_failed_rebuild_restores_only_target(self) -> None:
        with patch("checks.LiveClients"), patch("checks.containers", return_value={}), \
                patch("checks.ready_containers"), patch("checks.snapshot", return_value=identities()), \
                patch("checks.seismic_sample", return_value={}), patch("checks.cli") as command, \
                patch("checks.rebuild_consumer", side_effect=AssertionError("build failure")):
            with self.assertRaisesRegex(AssertionError, "build failure"):
                checks.independent_rebuild({}, 1)
        command.assert_called_once_with(["docker", "compose", "up", "-d", "--no-deps", "--no-build", "consumer-1"])


class StorageOracleTests(unittest.TestCase):
    def test_sql_is_observed_before_restoring_source_schema(self) -> None:
        old = {"hazard_id": "old", "source": "PVMBG", "source_ref_id": "r1", "attributes": {}}
        new = {"hazard_id": "new", "source": "PVMBG", "source_ref_id": "r2", "attributes": {"confidence_level": .8}}
        phases = []
        evidence = {"containers_before": {}, "after": {"old_hazard": old, "volcanic_hazard": new}}

        def observed_rows(*_arguments: str) -> list:
            self.assertNotIn("restore", phases)
            phases.append("sql")
            return [old, new]

        with patch("checks.LiveClients"), patch("checks.schema_definition", return_value={}), \
                patch("checks.evolution_baseline", return_value=({}, 1)), \
                patch("checks.collect_storage_phases"), patch("checks.container_snapshot", return_value={}), \
                patch("checks.assert_storage_structure"), patch("checks.stored_witnesses", side_effect=observed_rows), \
                patch("checks.restore_evolution", side_effect=lambda *_arguments: phases.append("restore")):
            checks.flexible_storage(evidence, 1)
        self.assertEqual(phases, ["sql", "restore"])

    def test_receipt_is_checked_against_the_storage_clock(self) -> None:
        hazard = {"ingested_at": "2026-10-09T12:00:00.600Z"}
        checks.assert_receipt_clock(hazard, "2026-10-09T12:00:00.601Z")
        checks.assert_receipt_clock(hazard, "2026-10-09T12:00:00.600Z")
        with self.assertRaises(AssertionError):
            checks.assert_receipt_clock(hazard, "2026-10-09T12:00:00.599Z")

    def test_missing_duplicate_and_malformed_witnesses_fail(self) -> None:
        old = {"hazard_id": "old", "source": "PVMBG", "source_ref_id": "r1", "attributes": {}}
        new = {"hazard_id": "new", "source": "PVMBG", "source_ref_id": "r2", "attributes": {"confidence_level": 0}}
        for confidence in (0, .8, 1):
            supported = new | {"attributes": {"confidence_level": confidence}}
            checks.assert_witnesses([old, supported], old, supported)
        for stored in ([], [old], [old, old], [new, new], [old, new, new]):
            with self.subTest(stored=stored), self.assertRaises(AssertionError):
                checks.assert_witnesses(stored, old, new)
        for confidence in (None, True, "0", -0.1, 1.1, float("nan")):
            malformed = new | {"attributes": {"confidence_level": confidence}}
            with self.subTest(confidence=confidence), self.assertRaises(AssertionError):
                checks.assert_witnesses([old, malformed], old, malformed)
        missing = new | {"attributes": {}}
        with self.assertRaises(AssertionError):
            checks.assert_witnesses([old, missing], old, missing)

    def test_api_only_success_cannot_hide_missing_sql_attribute(self) -> None:
        old = {"hazard_id": "old", "source": "PVMBG", "source_ref_id": "r1", "attributes": {}}
        new = {"hazard_id": "new", "source": "PVMBG", "source_ref_id": "r2", "attributes": {"confidence_level": .8}}
        with self.assertRaises(AssertionError):
            checks.assert_witnesses([old, new | {"attributes": {}}], old, new)

    def test_startup_failure_still_tears_down_partial_project(self) -> None:
        summary = {"compose_project": "aat-p4-test"}
        with patch("run_all.cli", return_value="") as command:
            run_all.cleanup_project(summary, type("Options", (), {"keep_stack": False})())
        self.assertIn(unittest.mock.call(["docker", "compose", "down", "--timeout", "15"]), command.call_args_list)


if __name__ == "__main__":
    unittest.main()
