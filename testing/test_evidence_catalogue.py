import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from evidence_catalogue import complete_audit, load_artifact, load_submission, summarize_submission, verify_population


AS_OF = "2026-11-20T01:02:03.123456789Z"


def sha256(raw):
    return hashlib.sha256(raw).hexdigest()


def audit(count, checksum):
    return dict(source="BMKG", count=count, checksum=checksum, algorithm="sha256",
                as_of=AS_OF, partial=False, unavailable_units=[])


class CatalogueEvidenceTest(unittest.TestCase):
    def test_checksum_envelope_requires_all_exact_fields(self):
        valid = audit(0, sha256(b""))
        self.assertEqual(complete_audit(valid).as_of, AS_OF)
        self.assertEqual(complete_audit(dict(valid, as_of="2026-11-20T01:02:03+00:00")).count, 0)
        for key in valid:
            with self.subTest(missing=key), self.assertRaises(ValueError):
                complete_audit({name: field for name, field in valid.items() if name != key})
        for key, value in (("unavailable_units", None), ("unavailable_units", ["store-a"]),
                           ("unavailable_units", {}), ("partial", 0), ("partial", True),
                           ("count", False), ("count", -1), ("count", 0.0), ("algorithm", "SHA256"),
                           ("source", None), ("source", "PVMBG"), ("checksum", "A" * 64),
                           ("as_of", None), ("as_of", "2026-11-20T01:02:03+07:00"),
                           ("as_of", "2026-11-20T01:02:03"), ("as_of", "2026-11-31T01:02:03Z"),
                           ("extra", None)):
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                complete_audit(dict(valid, **{key: value}))

    def test_artifact_population_is_required_even_when_every_row_was_acknowledged(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            historical = root / "historical.jsonl"
            burst = root / "burst.jsonl"
            output = root / "run.json"
            historical.write_bytes('{"event_id":"é"}\n{"event_id":"a "}\n'.encode("utf-8"))
            burst.write_bytes(b'{"event_id":"z"}\n{"event_id":"a "}\n{"event_id":"z"}\n')
            baseline_hash = sha256(historical.read_bytes())
            checksum = sha256("a \nz\né".encode("utf-8"))
            baseline = load_artifact(historical)
            artifact = load_artifact(burst)
            for label, changes, passed in (
                ("resume with duplicates and overlapping IDs", {}, True),
                ("acknowledged but unchanged incomplete population", {
                    "audit_before": audit(2, sha256("a \né".encode("utf-8"))),
                    "audit_after": audit(2, sha256("a \né".encode("utf-8"))),
                    "reference_checksum": sha256("a \né".encode("utf-8")),
                    "expected_population": {"count": 2, "checksum": sha256("a \né".encode("utf-8"))},
                }, False),
                ("correct checksum and wrong count", {"audit_after": audit(4, checksum)}, False),
                ("correct count and wrong checksum", {"audit_after": audit(3, sha256(b"wrong"))}, False),
                ("wrong baseline bytes", {"baseline_artifact_sha256": "a" * 64}, False),
                ("wrong upload bytes", {"artifact_sha256": "a" * 64}, False),
                ("wrong acknowledged row count", {"rows": 2}, False),
                ("wrong supplied reference", {"reference_checksum": "a" * 64}, False),
            ):
                with self.subTest(label=label):
                    document = dict(
                        dataset_available=True, rows=3, artifact_sha256=sha256(burst.read_bytes()),
                        baseline_artifact_sha256=baseline_hash, expected_population={"count": 3, "checksum": checksum},
                        reference_checksum=checksum, audit_before=audit(3, checksum), audit_after=audit(3, checksum),
                        submission_started_at="2026-11-20T01:00:00Z", submission_finished_at="2026-11-20T01:01:00Z",
                        resources_manifest={}, measurements_before={}, measurements_after={},
                    )
                    document.update(changes)
                    output.write_text(json.dumps(document), encoding="utf-8")
                    submission = load_submission(output)
                    self.assertEqual(verify_population(submission, artifact, baseline), passed)
                    summary = summarize_submission(submission)
                    self.assertEqual(summary["audit_before"]["as_of"], AS_OF)
                    self.assertEqual(summary["audit_after"]["as_of"], AS_OF)
                    self.assertEqual(summary["audit_count_delta"], document["audit_after"]["count"] - document["audit_before"]["count"])

            output.write_text(output.read_text(encoding="utf-8").replace('"partial": false', '"partial": true, "partial": false', 1), encoding="utf-8")
            with self.assertRaises(ValueError):
                load_submission(output)


if __name__ == "__main__":
    unittest.main()
