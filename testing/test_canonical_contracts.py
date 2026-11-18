"""Isolated Step 3a schema checks; no running service or deployment required."""
import copy
import json
import unittest
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource

ROOT = Path(__file__).resolve().parents[1]
SCHEMAS = ROOT / "bnpb/contracts/v1/schemas"
PAIRS = {"BMKG": "SEISMIC", "PVMBG": "VOLCANIC", "SATELIT": "WILDFIRE", "SENSOR_PANTAI": "COASTAL"}


class CanonicalM2ContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.schemas = {}
        resources = {}
        for name in ("hazard-event", "hazard-summary", "hazard-notification"):
            path = (SCHEMAS / f"{name}.schema.json").resolve()
            schema = json.loads(path.read_text(encoding="utf-8"))
            Draft202012Validator.check_schema(schema)
            cls.schemas[name] = path.as_uri()
            resources[path.as_uri()] = Resource.from_contents(schema)
        registry = Registry().with_resources(resources.items())
        cls.validators = {
            name: Draft202012Validator({"$ref": uri}, registry=registry, format_checker=FormatChecker())
            for name, uri in cls.schemas.items()
        }

    def payloads(self, source, hazard_type):
        event = {
            "hazard_id": "haz-test", "source": source, "source_ref_id": "stable-test",
            "hazard_type": hazard_type, "severity": "SIAGA", "area_name": "Test Area",
            "latitude": -2.51234, "longitude": 113.8045,
            "occurred_at": "2026-11-12T02:43:00Z", "ingested_at": "2026-11-12T02:51:08Z",
            "attributes": {"nested": {"precise_raw": "private"}},
        }
        summary = {key: value for key, value in event.items() if key not in ("source_ref_id", "latitude", "longitude", "attributes")}
        notification = {
            "schema_version": 1, "message_id": "msg-test", "hazard_id": "haz-test", "revision": 1,
            "correlation_id": "contract-test", "event_type": "hazard.created",
            "produced_at": "2026-11-12T02:51:08Z", "hazard_event": event,
        }
        return {"hazard-event": event, "hazard-summary": summary, "hazard-notification": notification}

    def test_all_pair_combinations_at_every_wire_boundary(self):
        for source in (*PAIRS, "UNKNOWN"):
            for hazard_type in (*PAIRS.values(), "TSUNAMI", "UNKNOWN"):
                for name, payload in self.payloads(source, hazard_type).items():
                    with self.subTest(source=source, hazard_type=hazard_type, schema=name):
                        self.assertEqual(self.validators[name].is_valid(payload), PAIRS.get(source) == hazard_type)

    def test_media_summary_still_rejects_each_raw_field(self):
        for source, hazard_type in PAIRS.items():
            payloads = self.payloads(source, hazard_type)
            for raw_field in ("source_ref_id", "latitude", "longitude", "attributes"):
                with self.subTest(source=source, raw_field=raw_field):
                    summary = copy.deepcopy(payloads["hazard-summary"])
                    summary[raw_field] = payloads["hazard-event"][raw_field]
                    self.assertFalse(self.validators["hazard-summary"].is_valid(summary))

    def test_updated_notification_uses_same_expanded_event_schema(self):
        for source, hazard_type in PAIRS.items():
            notification = self.payloads(source, hazard_type)["hazard-notification"]
            notification.update(revision=2, event_type="hazard.updated")
            with self.subTest(source=source):
                self.validators["hazard-notification"].validate(notification)


if __name__ == "__main__":
    unittest.main()
