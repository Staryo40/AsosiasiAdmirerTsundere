"""Verify existing A2 catalogue evidence without contacting or changing services."""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
import math
from pathlib import Path
import re
from statistics import mean
import unicodedata

from evidence_support import now, read_json, read_jsonl, report, run, utc


SHARDS = (0, 1, 2)
ROLES = ("primary", "replica")
TABLES = ("hazard_events", "source_seismic", "source_warnings", "outbox", "handled_inputs")
STRATEGIES = ("fixed_hash", "longitude_band")
COUNTERS = ("logical_rows", "input_bytes", "writes")
METRICS = (
    "cpu_percent", "disk_read_bytes_per_second", "disk_write_bytes_per_second",
    "network_receive_bytes_per_second", "network_transmit_bytes_per_second", "disk_write_latency_ms",
)
DIGEST = re.compile(r"[0-9a-f]{64}")
UTC_TIMESTAMP = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?(?:Z|\+00:00)")
NODE_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")
EMPTY_CHECKSUM = hashlib.sha256(b"").hexdigest()


@dataclass(frozen=True)
class Audit:
    count: int
    checksum: str
    as_of: str

    @property
    def identity(self):
        return self.count, self.checksum


@dataclass(frozen=True)
class Artifact:
    sha256: str
    rows: int
    refs: set[bytes]


@dataclass(frozen=True)
class Submission:
    started: datetime
    finished: datetime
    artifact_sha256: str
    rows: int
    baseline_artifact_sha256: str | None
    expected_population: tuple[int, str]
    reference_checksum: str
    audit_before: Audit
    audit_after: Audit
    resources_manifest: dict
    measurements_before: dict
    measurements_after: dict


@dataclass(frozen=True)
class ShardCopy:
    shard_id: int
    node: str
    role: str


@dataclass(frozen=True)
class PlacementSnapshot:
    observed_at: datetime
    revision: int
    copies: dict[str, ShardCopy]


@dataclass(frozen=True)
class CopyMeasurement:
    bmkg_rows: int
    canonical_rows: int
    relation_bytes: int
    canonical_revisions: int


@dataclass
class StageEvidence:
    checks: dict
    observations: dict
    submission: Submission | None = None
    placement: PlacementSnapshot | None = None
    before: dict[str, CopyMeasurement] | None = None
    after: dict[str, CopyMeasurement] | None = None


def need(condition):
    if not condition:
        raise ValueError("Invalid or incomplete evidence input.")


def integer(number):
    need(type(number) is int and 0 <= number <= 2**63 - 1)
    return number


def digest(checksum):
    need(isinstance(checksum, str) and DIGEST.fullmatch(checksum) is not None)
    return checksum


def complete_audit(audit):
    need(isinstance(audit, dict) and audit.keys() == {
        "source", "count", "checksum", "algorithm", "as_of", "partial", "unavailable_units",
    })
    need(audit["source"] == "BMKG" and audit["algorithm"] == "sha256" and audit["partial"] is False)
    need(audit["unavailable_units"] == [])
    as_of = audit["as_of"]
    need(isinstance(as_of, str) and UTC_TIMESTAMP.fullmatch(as_of) is not None)
    utc(as_of)
    count, checksum = integer(audit["count"]), digest(audit["checksum"])
    need((count == 0) == (checksum == EMPTY_CHECKSUM))
    return Audit(count, checksum, as_of)


def unique_object(pairs):
    document = dict(pairs)
    need(len(document) == len(pairs))
    return document


def load_artifact(path):
    file_hash = hashlib.sha256()
    rows, refs = 0, set()
    with Path(path).open("rb") as stream:
        for raw in iter(lambda: stream.readline((128 << 10) + 1), b""):
            need(len(raw) <= 128 << 10)
            file_hash.update(raw)
            line = raw.decode("utf-8").strip()
            if not line:
                continue
            event = json.loads(line, object_pairs_hook=unique_object)
            need(isinstance(event, dict))
            ref = event["event_id"]
            need(isinstance(ref, str) and bool(ref.strip()) and not any(unicodedata.category(char) == "Cc" for char in ref))
            ref_bytes = ref.encode("utf-8")
            need(len(ref_bytes) <= 2048)
            refs.add(ref_bytes)
            rows += 1
    need(rows > 0)
    return Artifact(file_hash.hexdigest(), rows, refs)


def verify_population(submission, artifact, baseline=None):
    if submission is None or artifact is None:
        return False
    baseline_sha256 = baseline.sha256 if baseline else None
    if (artifact.sha256 != submission.artifact_sha256 or artifact.rows != submission.rows
            or baseline_sha256 != submission.baseline_artifact_sha256):
        return False
    refs = baseline.refs | artifact.refs if baseline else artifact.refs
    expected = (len(refs), hashlib.sha256(b"\n".join(sorted(refs))).hexdigest())
    return submission.audit_after.identity == submission.expected_population == expected and submission.reference_checksum == expected[1]


def load_submission(path):
    document = json.loads(Path(path).read_text(encoding="utf-8-sig"), object_pairs_hook=unique_object)
    started = utc(document["submission_started_at"])
    finished = utc(document["submission_finished_at"])
    need(document["dataset_available"] is True and started < finished)
    baseline = digest(document["baseline_artifact_sha256"]) if "baseline_artifact_sha256" in document else None
    expected = document["expected_population"]
    return Submission(
        started, finished, digest(document["artifact_sha256"]), integer(document["rows"]),
        baseline, (integer(expected["count"]), digest(expected["checksum"])),
        digest(document["reference_checksum"]), complete_audit(document["audit_before"]),
        complete_audit(document["audit_after"]), document["resources_manifest"],
        document["measurements_before"], document["measurements_after"],
    )


def load_resources(path):
    raw = Path(path).read_bytes()
    manifest = json.loads(raw.decode("utf-8"))
    need(isinstance(manifest["resources"], dict) and bool(manifest["resources"]))
    return manifest, hashlib.sha256(raw).hexdigest()


def node_name(name):
    need(isinstance(name, str) and len(name) <= 253)
    need(all(NODE_LABEL.fullmatch(label) is not None for label in name.split(".")))
    return name


def placement_snapshot(snapshot, nodes):
    copies = {}
    for physical in snapshot["copies"]:
        unit, node, role = physical["unit"], node_name(physical["node"]), physical["role"]
        shard = integer(physical["shard_id"])
        need(isinstance(unit, str) and bool(unit.strip()) and unit not in copies)
        need(node in nodes and shard in SHARDS and role in ROLES and physical["state"] == "ready")
        copies[unit] = ShardCopy(shard, node, role)
    for shard in SHARDS:
        members = [copy for copy in copies.values() if copy.shard_id == shard]
        need(len(members) == 2 and {copy.role for copy in members} == set(ROLES))
        need(members[0].node != members[1].node)
    need(all(sum(copy.node == node for copy in copies.values()) < len(SHARDS) for node in nodes))
    revision = integer(snapshot["revision"])
    need(revision > 0)  # Revision zero is the empty control-store baseline, not observed RF2.
    return PlacementSnapshot(utc(snapshot["observed_at"]), revision, copies)


def load_placement(path):
    document = read_json(path)
    need(isinstance(document["nodes"], list))
    nodes = sorted(map(node_name, document["nodes"]))
    need(len(nodes) >= 3 and len(set(nodes)) == len(nodes))
    return nodes, document


def stable_placement(stage, nodes, submission):
    before = placement_snapshot(stage["before"], nodes)
    after = placement_snapshot(stage["after"], nodes)
    need(before.observed_at <= submission.started < submission.finished <= after.observed_at)
    need(before.revision == after.revision and before.copies == after.copies)
    return after


def measure_copy(physical):
    tables = {}
    for relation in physical["tables"]:
        name = relation["name"]
        need(name in TABLES and name not in tables and relation["stats_approximate"] is True)
        integer(relation["stats_inserted"])
        integer(relation["stats_updated"])
        tables[name] = integer(relation["row_count"]), integer(relation["relation_bytes"])
    need(set(tables) == set(TABLES))
    revisions = integer(physical["canonical_revisions"])
    need(revisions == tables["outbox"][0])
    return CopyMeasurement(tables["source_seismic"][0], tables["hazard_events"][0],
                           sum(size for _, size in tables.values()), revisions)


def measurements(snapshot, placement):
    need(snapshot["partial"] is False and snapshot["missing_settings"] in (None, []))
    need(snapshot["unavailable_units"] in (None, []))
    copies, connections = {}, set()
    for physical in snapshot["items"]:
        unit = physical["unit"]
        expected = placement.copies[unit]
        connection = integer(physical["connection_index"])
        need(unit not in copies and connection not in connections and physical["partial"] is False)
        need(physical["settings_complete"] is True and physical["in_recovery"] is (expected.role == "replica"))
        need(physical["node"] == expected.node and physical["role"] == expected.role)
        copies[unit] = measure_copy(physical)
        connections.add(connection)
    need(copies.keys() == placement.copies.keys())
    return copies


def analytical_scenario(scenario, input_rows):
    need(integer(scenario["logical_rows"]) == input_rows and len(scenario["shards"]) == len(SHARDS))
    shards = [{metric: integer(shard[metric]) for metric in COUNTERS} for shard in scenario["shards"]]
    need(all(shard["writes"] == shard["logical_rows"] for shard in shards))
    need(all((shard["input_bytes"] > 0) == (shard["logical_rows"] > 0) for shard in shards))
    need(sum(shard["logical_rows"] for shard in shards) == input_rows)
    skew = {}
    for metric in COUNTERS:
        values = [shard[metric] for shard in shards]
        maximum, average = max(values), sum(values) / len(SHARDS)
        skew[metric] = {"max": maximum, "mean": average, "max_to_mean": maximum / average if average else 0}
        for key, computed in skew[metric].items():
            need(math.isclose(metric_number(scenario["skew"][metric][key]), computed, rel_tol=1e-12))
    return {"logical_rows": input_rows, "shards": shards, "skew": skew}


def compare_strategies(comparison, rows):
    strategies = {strategy: analytical_scenario(comparison[strategy], rows) for strategy in STRATEGIES}
    need(len({sum(shard["input_bytes"] for shard in scenario["shards"]) for scenario in strategies.values()}) == 1)
    return strategies


def read_comparison(path, submission, resources, stage):
    document = read_json(path)
    manifest, manifest_sha = resources
    need(document["algorithm"] == "sha256-32be-v1; longitude-bands-120deg-v1")
    need(document["counters_are_analytical"] is True and document["resources_sha256"] == manifest_sha)
    need(submission.resources_manifest == manifest and document["input_sha256"] == submission.artifact_sha256)
    declared_reference = manifest.get("reference_checksum")
    if declared_reference is not None:
        need(isinstance(declared_reference, str))
        need(not declared_reference.strip() or declared_reference.strip().lower() == submission.reference_checksum)
    need(integer(document["input_rows"]) == submission.rows)
    strategies = compare_strategies(document["natural"], submission.rows)
    summary = {"resources_sha256": manifest_sha, "strategies": strategies}
    if stage == "burst":
        area = document["burst_area"]
        need(isinstance(area, str) and bool(area.strip()))
        need(compare_strategies(document["region_burst"], submission.rows) == strategies)
        summary["all_rows_in_burst_area"] = True
        summary["burst_area_sha256"] = hashlib.sha256(area.encode("utf-8")).hexdigest()
    return summary


def metric_number(number):
    need(type(number) in (int, float) and math.isfinite(number) and number >= 0)
    return number


def metric_samples(path, nodes, submission):
    samples = {node: {} for node in nodes}
    for sample in read_jsonl(path):
        measured_at = utc(sample["timestamp"])
        if not submission.started <= measured_at <= submission.finished:
            continue
        by_time = samples[sample["node"]]
        need(measured_at not in by_time)
        readings = {metric: metric_number(sample[metric]) for metric in METRICS}
        need(readings["cpu_percent"] <= 100)
        by_time[measured_at] = readings
    return {node: summarize_samples(by_time) for node, by_time in samples.items()}


def summarize_samples(by_time):
    if not by_time:
        return {"sample_count": 0}
    metrics = {}
    for metric in METRICS:
        numbers = [readings[metric] for readings in by_time.values()]
        metrics[metric] = {"min": min(numbers), "sample_mean": mean(numbers), "max": max(numbers)}
    return {"sample_count": len(by_time), "first_sample_at": min(by_time).isoformat(),
            "last_sample_at": max(by_time).isoformat(), "metrics": metrics}


def storage_by_node(nodes, placement, before, after):
    storage = {}
    for node in nodes:
        units = [unit for unit, copy in placement.copies.items() if copy.node == node]
        primaries = [unit for unit in units if placement.copies[unit].role == "primary"]
        storage[node] = {
            "primary_shards": sorted(placement.copies[unit].shard_id for unit in primaries),
            "replica_shards": sorted(placement.copies[unit].shard_id for unit in units if unit not in primaries),
            "before": storage_totals(before, units, primaries), "after": storage_totals(after, units, primaries),
            "primary_revision_delta": sum(after[unit].canonical_revisions - before[unit].canonical_revisions
                                          for unit in primaries),
        }
    return storage


def storage_totals(measured, units, primaries):
    total_bytes = sum(measured[unit].relation_bytes for unit in units)
    primary_bytes = sum(measured[unit].relation_bytes for unit in primaries)
    return {
        "bmkg_logical_rows_present": sum(measured[unit].bmkg_rows for unit in units),
        "canonical_logical_rows_present": sum(measured[unit].canonical_rows for unit in units),
        "primary_bmkg_logical_rows": sum(measured[unit].bmkg_rows for unit in primaries),
        "all_copy_relation_bytes": total_bytes, "primary_relation_bytes": primary_bytes,
        "replica_relation_bytes": total_bytes - primary_bytes,
    }


def verify_storage(stage, nodes, comparison):
    placement, before, after = stage.placement, stage.before, stage.after
    primaries = {copy.shard_id: unit for unit, copy in placement.copies.items() if copy.role == "primary"}
    replicas = {copy.shard_id: unit for unit, copy in placement.copies.items() if copy.role == "replica"}
    stage.checks["rf2_matching_row_counts"] = all(
        row_counters(snapshot[primaries[shard]]) == row_counters(snapshot[replicas[shard]])
        for snapshot in (before, after) for shard in SHARDS
    )
    stage.checks["primary_bmkg_rows_match_audit"] = all(
        sum(snapshot[unit].bmkg_rows for unit in primaries.values()) == audit.count
        for snapshot, audit in ((before, stage.submission.audit_before), (after, stage.submission.audit_after))
    )
    stage.checks["primary_deltas_match_fixed_hash"] = comparison is not None and all(
        measured_delta(before[unit], after[unit]) == (shard["logical_rows"], shard["logical_rows"], shard["writes"])
        for unit, shard in zip((primaries[shard] for shard in SHARDS), comparison["strategies"]["fixed_hash"]["shards"])
    )
    storage = storage_by_node(nodes, placement, before, after)
    writing_nodes = [node for node, counts in storage.items() if counts["primary_revision_delta"] > 0]
    stage.checks["primary_revisions_on_multiple_nodes"] = len(writing_nodes) > 1
    stage.observations.update(storage_by_node=storage, nodes_with_primary_revision_increases=writing_nodes)


def row_counters(measurement):
    return measurement.bmkg_rows, measurement.canonical_rows, measurement.canonical_revisions


def measured_delta(before, after):
    return tuple(end - start for start, end in zip(row_counters(before), row_counters(after)))


def read_capture(sources, name, reader, *args):
    try:
        captured = reader(*args)
    except FileNotFoundError:
        sources[name] = "absent"
    except (KeyError, TypeError, ValueError, OSError, OverflowError, RecursionError):
        sources[name] = "invalid_or_incomplete"
    else:
        sources[name] = "usable"
        return captured
    return None


def summarize_submission(submission):
    elapsed = (submission.finished - submission.started).total_seconds()
    return {
        "artifact_sha256": submission.artifact_sha256, "acknowledged_rows": submission.rows,
        "baseline_artifact_sha256": submission.baseline_artifact_sha256,
        "expected_population": dict(zip(("count", "checksum"), submission.expected_population)),
        "audit_before": {"count": submission.audit_before.count, "checksum": submission.audit_before.checksum,
                         "as_of": submission.audit_before.as_of},
        "audit_after": {"count": submission.audit_after.count, "checksum": submission.audit_after.checksum,
                        "as_of": submission.audit_after.as_of},
        "audit_count_delta": submission.audit_after.count - submission.audit_before.count,
        "submission_started_at": submission.started.isoformat(), "submission_finished_at": submission.finished.isoformat(),
        "submission_elapsed_seconds": elapsed, "mean_acknowledged_rows_per_second": submission.rows / elapsed,
    }


def inspect_stage(args, name, resources, placement_input):
    sources = {}
    checks = dict.fromkeys(("complete_loader_report", "audit_matches_expected_population", "analytical_comparison_matches_input",
                            "confirmed_stable_placement", "complete_rf2_measurements", "rf2_matching_row_counts",
                            "primary_bmkg_rows_match_audit", "primary_deltas_match_fixed_hash",
                            "primary_revisions_on_multiple_nodes", "node_samples_in_submission_window"), False)
    stage = StageEvidence(checks, {"inputs": sources})
    stage.submission = submission = read_capture(sources, "loader_report", load_submission, getattr(args, f"{name}_report"))
    if submission is None:
        return stage
    stage.observations["submission"] = summarize_submission(submission)
    checks["complete_loader_report"] = True
    compare_path = getattr(args, f"{name}_compare")
    comparison = read_capture(sources, "comparison", read_comparison, compare_path, submission, resources, name) if resources else None
    checks["analytical_comparison_matches_input"] = comparison is not None
    if comparison is not None:
        stage.observations["analytical_comparison"] = comparison
    if placement_input is None:
        return stage
    attach_node_evidence(stage, name, placement_input, getattr(args, f"{name}_metrics"))
    if checks["complete_rf2_measurements"]:
        verify_storage(stage, placement_input[0], comparison)
    return stage


def attach_node_evidence(stage, name, placement_input, metrics_path):
    nodes, placement_document = placement_input
    sources = stage.observations["inputs"]
    samples = read_capture(sources, "node_timeseries", metric_samples, metrics_path, nodes, stage.submission)
    if samples is not None:
        missing = [node for node, summary in samples.items() if not summary["sample_count"]]
        stage.observations.update(measured_node_timeseries=samples, nodes_without_in_window_samples=missing)
        stage.checks["node_samples_in_submission_window"] = not missing
    stage.placement = read_capture(sources, "placement_window", lambda: stable_placement(
        placement_document[name], nodes, stage.submission))
    if stage.placement is not None:
        stage.checks["confirmed_stable_placement"] = True
        stage.observations["placement_revision"] = stage.placement.revision
        stage.before = read_capture(sources, "measurements_before", measurements, stage.submission.measurements_before, stage.placement)
        stage.after = read_capture(sources, "measurements_after", measurements, stage.submission.measurements_after, stage.placement)
        stage.checks["complete_rf2_measurements"] = stage.before is not None and stage.after is not None


def check_continuity(historical, burst, reference):
    first, second = historical.submission, burst.submission
    first_placement, second_placement = historical.placement, burst.placement
    return {
        "historical_audit_matches_independent_reference": bool(
            first and reference and historical.checks["audit_matches_expected_population"] and first.reference_checksum == reference),
        "historical_bmkg_baseline_empty": bool(first and first.audit_before.identity == (0, EMPTY_CHECKSUM)),
        "burst_uses_distinct_artifact": bool(first and second and first.artifact_sha256 != second.artifact_sha256),
        "burst_starts_at_historical_audit_baseline": bool(
            first and second and first.finished <= second.started and first.audit_after.identity == second.audit_before.identity),
        "placement_unchanged_between_stages": bool(first_placement and second_placement
            and first_placement.copies == second_placement.copies and first_placement.revision == second_placement.revision),
        "burst_starts_at_historical_storage_baseline": bool(historical.after and burst.before
            and historical.after.keys() == burst.before.keys()
            and all(row_counters(previous) == row_counters(burst.before[unit]) for unit, previous in historical.after.items())),
        "no_node_contains_all_historical_bmkg_rows": bool(first and first.audit_after.count > 0
            and historical.checks["primary_bmkg_rows_match_audit"] and historical.checks["rf2_matching_row_counts"]
            and all(counts["after"]["bmkg_logical_rows_present"] < first.audit_after.count
                    for counts in historical.observations["storage_by_node"].values())),
    }


def arguments():
    parser = argparse.ArgumentParser(
        description="Read-only verification of historical assistant catalogue and distinct same-region burst evidence.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""No assistant dataset or reference is bundled. Use real A2 exports and the existing Go loader separately.
For --placement use {nodes:[Kubernetes node names],historical:{before:snapshot,after:snapshot},burst:{before:snapshot,after:snapshot}}.
Each snapshot is {observed_at,revision,copies}; each copy is {shard_id,unit,node,role,state}. Use three ready RF2 shards,
actual observed units/nodes and an unchanged placement revision bracketing each submission.
Metrics JSONL keys: timestamp (ISO 8601 with offset), node, cpu_percent (0–100 of node capacity), disk_read_bytes_per_second,
disk_write_bytes_per_second, network_receive_bytes_per_second, network_transmit_bytes_per_second, disk_write_latency_ms
(mean disk-write latency in that sample). Use measured node-wide values, distinct timestamps per node, and samples in each actual window.
""",
    )
    for stage in ("historical", "burst"):
        parser.add_argument(f"--{stage}-artifact", required=True, metavar="JSONL", help=f"exact {stage} JSONL input shared by loader and comparator")
        parser.add_argument(f"--{stage}-report", required=True, metavar="JSON", help=f"{stage} cmd/catalog-load run report")
        parser.add_argument(f"--{stage}-compare", required=True, metavar="JSON", help=f"{stage} cmd/placement-compare stdout JSON" + (
            " (use -burst-area matching every row in the burst artifact)" if stage == "burst" else ""))
        parser.add_argument(f"--{stage}-metrics", required=True, metavar="JSONL", help=f"normalized A2 {stage} node measurements")
    parser.add_argument("--reference", required=True, metavar="SHA256", help="independent assistant BMKG Audit SHA-256, not artifact SHA-256")
    parser.add_argument("--resources", required=True, metavar="JSON", help="original historical resource manifest (also used for burst unless overridden)")
    parser.add_argument("--burst-resources", metavar="JSON", help="original burst manifest if different bytes, with the same resources object")
    parser.add_argument("--placement", required=True, metavar="JSON", help="A2 actual placement evidence for both submission windows")
    parser.add_argument("--out", required=True, metavar="JSON", help="new sanitized evidence report path; must not exist")
    return parser.parse_args()


def main():
    args, started = arguments(), now()
    sources = {}
    reference = read_capture(sources, "assistant_reference", digest, args.reference.strip().lower())
    resources = read_capture(sources, "resources", load_resources, args.resources)
    burst_resources = resources
    if args.burst_resources is not None:
        burst_resources = read_capture(sources, "burst_resources", load_resources, args.burst_resources)
    placement = read_capture(sources, "placement", load_placement, args.placement)
    historical = inspect_stage(args, "historical", resources, placement)
    burst = inspect_stage(args, "burst", burst_resources, placement)
    historical_artifact = read_capture(historical.observations["inputs"], "artifact", load_artifact, args.historical_artifact)
    burst_artifact = read_capture(burst.observations["inputs"], "artifact", load_artifact, args.burst_artifact)
    historical.checks["audit_matches_expected_population"] = verify_population(historical.submission, historical_artifact)
    burst.checks["audit_matches_expected_population"] = historical_artifact is not None and verify_population(
        burst.submission, burst_artifact, historical_artifact)
    checks = check_continuity(historical, burst, reference)
    checks["resource_envelope_unchanged"] = bool(
        resources and burst_resources and resources[0]["resources"] == burst_resources[0]["resources"]
    )
    for name, stage in (("historical", historical), ("burst", burst)):
        checks.update({f"{name}_{check}": passed for check, passed in stage.checks.items()})
    observations = {
        "inputs": sources, "assistant_reference_sha256": reference,
        "expected_nodes": placement[0] if placement else [],
        "historical": historical.observations, "burst": burst.observations,
        "measurement_scope": {
            "analytical_comparison": "Shard indices 0–2; rows, JSONL record bytes and modeled writes, not runtime I/O or replicas.",
            "storage_by_node": "Measured PostgreSQL rows and total relation sizes for the five owned tables on each copy; not WAL, filesystem bytes or physical I/O.",
            "primary_revision_delta": "Primary outbox row-count delta only; replica tuple statistics and WAL replay are not logical writes.",
            "measured_node_timeseries": "A2 supplied samples inside the loader submission window; sample means are unweighted; disk write latency is not end-to-end event latency.",
            "provenance": "Assumes genuine operator exports from an isolated run; offline checks do not authenticate their origin or prove failover.",
        },
    }
    return report(args.out, "assistant_catalogue_and_burst", started, checks, observations)


if __name__ == "__main__":
    raise SystemExit(run(main))
