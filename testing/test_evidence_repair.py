import json
from pathlib import Path
import tempfile
import unittest

from evidence_repair import EVENTS, cutover_evidence


class CutoverEvidenceTest(unittest.TestCase):
    def test_requires_ordered_evidence_on_the_same_wal_history(self):
        old, repaired = ("node-a", "store-a"), ("node-b", "store-b")
        for name, update in (
            ("complete", None),
            ("insufficient replay", (2, "replay_lsn", "0/FFFFFFFF")),
            ("invalid WAL", (1, "acknowledged_lsn", "1/0junk")),
            ("wrong timeline", (2, "timeline", 2)),
            ("wrong history", (2, "system_id", "934")),
            ("wrong member", (3, "unit", "other-standby")),
            ("promotion before fence", (3, "at", "2026-11-20T00:00:01.5Z")),
            ("outside watch", (4, "at", "2026-11-20T00:00:10Z")),
            ("stale guard epoch", (4, "epoch", 6)),
            ("missing source digest", (0, "evidence_sha256", None)),
        ):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                rows = []
                for index, event in enumerate(EVENTS):
                    node, unit = old if index < 2 else repaired
                    rows.append(dict(event=event, at=f"2026-11-20T00:00:0{index+1}Z", node=node, unit=unit,
                                     timeline=1 if index < 3 else 2, system_id="123", evidence_sha256="a" * 64))
                rows[1]["acknowledged_lsn"] = "1/0"
                rows[2]["replay_lsn"] = "1/20"
                rows[4].update(previous_epoch=6, epoch=7)
                if update:
                    index, key, value = update
                    rows[index][key] = value
                path = Path(directory) / "cutover.jsonl"
                path.write_text("\n".join(map(json.dumps, rows)), encoding="utf-8")
                accepted, _ = cutover_evidence(path, "2026-11-20T00:00:00Z", "2026-11-20T00:00:09Z", old, repaired)
                self.assertEqual(accepted, update is None)


if __name__ == "__main__":
    unittest.main()
