# T07: compare our fires with the course assistants' reference result, matching fires by their detection keys.
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from m2lib import Target, now, parser, write_evidence, metadata  # noqa: E402

KEY_FIELDS = ("detection_keys", "detections", "keys")


def reference_fires(path: Path) -> list[dict]:
    document = json.loads(path.read_text(encoding="utf-8-sig"))
    if isinstance(document, dict):
        document = next((document[name] for name in ("fires", "items", "events", "hazards") if name in document), None)
    if not isinstance(document, list):
        raise SystemExit("Reference must be a JSON list of fires, or an object with a fires/items list.")
    fires = []
    for position, item in enumerate(document):
        attributes = item.get("attributes", {}) if isinstance(item.get("attributes"), dict) else {}
        keys = next((source[name] for source in (item, attributes) for name in KEY_FIELDS if name in source), None)
        if not isinstance(keys, list):
            raise SystemExit(f"Reference fire {position} has no detection key list ({', '.join(KEY_FIELDS)}).")
        fires.append({"id": item.get("id") or item.get("source_ref_id") or f"ref-{position}",
                      "severity": item.get("severity"), "keys": frozenset(map(str, keys))})
    return fires


def main() -> None:
    options = parser("T07 comparison with the assistants' reference")
    options.add_argument("reference", type=Path, help="reference result file from the assistants")
    options.add_argument("--ours", type=Path, help="saved bnpb-satellite -verify output; default: run it on the leader")
    args = options.parse_args()
    target, started = Target(args.target), now()
    ours_report = json.loads(args.ours.read_text()) if args.ours else target.satellite_verify()
    ours = [{"id": fire["id"], "severity": fire["severity"], "keys": frozenset(fire["detection_keys"])} for fire in ours_report["fires"]]
    reference = reference_fires(args.reference)

    by_key = {key: fire for fire in ours for key in fire["keys"]}
    exact, partial, missing, severity_differs, claimed = [], [], [], [], set()
    for ref in reference:
        if any(other["keys"] == ref["keys"] for other in ours):
            match = next(other for other in ours if other["keys"] == ref["keys"])
            exact.append(ref["id"])
            claimed.add(match["id"])
            if ref["severity"] and ref["severity"] != match["severity"]:
                severity_differs.append({"reference": ref["id"], "ours": match["id"], "reference_severity": ref["severity"],
                                         "our_severity": match["severity"]})
            continue
        overlapping = {by_key[key]["id"]: by_key[key] for key in ref["keys"] if key in by_key}
        if not overlapping:
            missing.append({"reference": ref["id"], "detections": len(ref["keys"])})
            continue
        claimed.update(overlapping)
        ours_keys = frozenset().union(*(fire["keys"] for fire in overlapping.values()))
        partial.append({"reference": ref["id"], "ours": sorted(overlapping), "reference_detections": len(ref["keys"]),
                        "our_detections": len(ours_keys), "only_reference": sorted(ref["keys"] - ours_keys)[:10],
                        "only_ours": sorted(ours_keys - ref["keys"])[:10],
                        "shape": "split" if len(overlapping) > 1 else "differs"})
    extra = [{"ours": fire["id"], "detections": len(fire["keys"])} for fire in ours if fire["id"] not in claimed]
    summary = {"reference_fires": len(reference), "our_fires": len(ours), "exact": len(exact), "partial": len(partial),
               "missing": len(missing), "extra": len(extra), "severity_differs": len(severity_differs)}
    write_evidence("M2-P4", "T07", metadata("T07", target, started) | {
        "summary": summary, "partial": partial[:50], "missing": missing[:50], "extra": extra[:50],
        "severity_differs": severity_differs[:50], "our_next_offset": ours_report.get("next_offset"),
        "likely_causes": ["late detections past the 360-minute watermark are counted but not clustered",
                          "low-confidence detections are excluded from fires",
                          "single detections are not published as fires",
                          "the reference may cover a different offset range than our log"],
    })
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
