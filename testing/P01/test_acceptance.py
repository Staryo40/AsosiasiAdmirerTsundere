from __future__ import annotations

import copy
import json
import unittest

from acceptance import ROOT, assert_equal, assert_mapping, unchanged_containers, valid_hazard


class AcceptanceOracleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.fixtures = json.loads((ROOT / "bnpb/internal/aggregator/testdata/mapping.json").read_text())

    def test_golden_payloads_satisfy_schema_and_mapping(self) -> None:
        for fixture in self.fixtures:
            with self.subTest(fixture=fixture["name"]):
                assert_mapping(fixture["expected"], fixture["input"], fixture.get("warnings", []))

    def test_schema_rejects_bad_fields_and_datetime(self) -> None:
        for field, replacement in (("latitude", True), ("severity", "Unknown"), ("occurred_at", "impossibleZ"),
                                   ("attributes", None), ("unexpected", "extra")):
            with self.subTest(field=field):
                hazard = copy.deepcopy(self.fixtures[0]["expected"])
                hazard[field] = replacement
                with self.assertRaises(AssertionError):
                    valid_hazard(hazard)

    def test_mapping_rejects_field_loss_and_warning_max_magnitude(self) -> None:
        fixture = self.fixtures[1]
        hazard = copy.deepcopy(fixture["expected"])
        hazard["severity"] = "SIAGA"
        with self.assertRaises(AssertionError):
            assert_mapping(hazard, fixture["input"], fixture["warnings"])
        fixture = self.fixtures[3]
        hazard = copy.deepcopy(fixture["expected"])
        del hazard["attributes"]["confidence_level"]
        with self.assertRaises(AssertionError):
            assert_mapping(hazard, fixture["input"], [])

    def test_boolean_and_number_are_distinct_json_values(self) -> None:
        with self.assertRaises(AssertionError):
            assert_equal({"nested": [False]}, {"nested": [0]}, "boolean preservation")
        assert_equal({"measurement": 1}, {"measurement": 1.0}, "JSON numeric equality")

    def test_container_restart_and_recreate_are_detected(self) -> None:
        before = {"aggregator": {"id": "a", "started_at": "first", "restart_count": 0, "running": True}}
        for field, replacement in (("id", "b"), ("started_at", "later"), ("restart_count", 1), ("running", False)):
            with self.subTest(field=field):
                after = copy.deepcopy(before)
                after["aggregator"][field] = replacement
                with self.assertRaises(AssertionError):
                    unchanged_containers(before, after)


if __name__ == "__main__":
    unittest.main()
