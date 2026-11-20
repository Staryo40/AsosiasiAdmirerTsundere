"""Collect read-only live topology, failover, and BMKG pagination evidence."""

import argparse
from collections import Counter
import json
from pathlib import Path, PurePosixPath, PureWindowsPath
import re
import subprocess
import sys
import time
from urllib.parse import urlencode

from evidence_support import get_json, now, read_json, read_jsonl, report, run, secret, utc


NAMESPACE = "sistem-informasi-kebencanaan-bnpb"
FLAG = "topology"
SHARDS = {"shard-0", "shard-1", "shard-2"}
FAULT_EVENTS = ("node_disk_lost", "old_primary_fenced", "new_primary_ready")
NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,252}")
DIGEST = re.compile(r"[a-f0-9]{64}")
NODE_INFO = re.compile(r"[A-Za-z0-9][A-Za-z0-9 _.,()+/-]{0,255}")
QUANTITY = re.compile(r"[0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+|[a-zA-Z]{1,2})?")
IMAGE = re.compile(r"[\w./-]+(?::[\w.-]+)?(?:/[\w./-]+(?::[\w.-]+)?)?(?:@[\w-]+:[a-fA-F0-9]+)?", re.ASCII)


def matched(value, pattern=NAME):
    return value if isinstance(value, str) and pattern.fullmatch(value) else None


def absolute_path(value):
    if not isinstance(value, str) or not value.isprintable() or len(value) > 4096:
        return None
    if value.startswith("//") or value.startswith("\\\\"):
        return None  # UNC/network paths do not establish local disk provenance.
    return value if PurePosixPath(value).is_absolute() or PureWindowsPath(value).is_absolute() else None


def kubectl(context, *args):
    command = ["kubectl", "--context", context, "--request-timeout=8s", "get", *args, "-o", "json"]
    # Capture objects in memory; only the field projections below enter evidence.
    try:
        result = subprocess.run(command, capture_output=True, timeout=10, check=True)
        return json.loads(result.stdout)
    except (OSError, subprocess.SubprocessError, ValueError):
        return None  # Never expose CLI output, kubeconfig details, or object bodies.


def ready(item):
    return not item["metadata"].get("deletionTimestamp") and any(
        condition.get("type") == "Ready" and condition.get("status") == "True"
        for condition in item.get("status", {}).get("conditions", [])
    )


def requirement_matches(requirement, fields):
    key, op = requirement.get("key"), requirement.get("operator")
    values = requirement.get("values", [])
    if op == "In":
        return key in fields and fields[key] in values
    if op == "NotIn":
        return key not in fields or fields[key] not in values
    if op == "Exists":
        return key in fields
    if op == "DoesNotExist":
        return key not in fields
    if op in {"Gt", "Lt"} and key in fields and len(values) == 1:
        try:
            left, right = int(fields[key]), int(values[0])
            return left > right if op == "Gt" else left < right
        except ValueError:
            pass
    return False


def affinity_nodes(pv_spec, nodes):
    terms = (pv_spec.get("nodeAffinity") or {}).get("required", {}).get("nodeSelectorTerms", [])
    for name, node in nodes.items():
        for term in terms:
            labels, fields = term.get("matchExpressions", []), term.get("matchFields", [])
            if (labels or fields) and all(
                requirement_matches(req, node["metadata"].get("labels", {})) for req in labels
            ) and all(requirement_matches(req, {"metadata.name": name}) for req in fields):
                yield name
                break


def disk_volume(context, volume, node, nodes, cache):
    if "hostPath" in volume:
        host_path = volume["hostPath"]
        path = absolute_path(host_path.get("path"))
        return {
            "kind": "hostPath",
            "path": path,
            "local": bool(path) and host_path.get("type", "") in {"", "Directory", "DirectoryOrCreate"},
        }
    if "persistentVolumeClaim" not in volume:
        return {"kind": "other", "local": False}
    ref = volume["persistentVolumeClaim"]
    claim = matched(ref.get("claimName"))
    if claim not in cache:
        pvc = kubectl(context, "pvc", claim, "-n", NAMESPACE) if claim else None
        pv_name = matched(pvc.get("spec", {}).get("volumeName")) if pvc else None
        pv = kubectl(context, "pv", pv_name) if pv_name else None
        cache[claim] = pvc, pv_name, pv
    pvc, pv_name, pv = cache[claim]
    spec = pv.get("spec", {}) if pv else {}
    claim_ref = spec.get("claimRef", {})
    path = absolute_path(spec.get("local", {}).get("path"))
    candidates = sorted(affinity_nodes(spec, nodes)) if path else []
    bound = bool(
        pvc and pv and pvc.get("status", {}).get("phase") == "Bound"
        and pv.get("status", {}).get("phase") == "Bound"
        and claim_ref.get("name") == claim and claim_ref.get("namespace") == NAMESPACE
        and pvc.get("metadata", {}).get("uid")
        and claim_ref.get("uid") == pvc["metadata"]["uid"]
    )
    return {
        "kind": "localPV" if path else "unverifiedPVC",
        "claim": claim,
        "pv": pv_name,
        "path": path,
        "affinity_nodes": candidates,
        "bound": bound,
        "local": bound and not ref.get("readOnly", False) and candidates == [node],
    }


def topology(context):
    node_doc = kubectl(context, "nodes")
    pod_doc = kubectl(context, "pods", "-n", NAMESPACE, "-l", "role in (aggregator,store)")
    nodes = {item["metadata"]["name"]: item for item in (node_doc or {}).get("items", [])}
    observed_nodes = []
    for name, item in sorted(nodes.items()):
        status = item.get("status", {})
        info, capacity = status.get("nodeInfo", {}), status.get("capacity", {})
        observed_nodes.append({
            "name": name,
            "ready": ready(item),
            "boot_id": matched(info.get("bootID")),
            "kernel": matched(info.get("kernelVersion"), NODE_INFO),
            "os_image": matched(info.get("osImage"), NODE_INFO),
            "cpu": matched(capacity.get("cpu"), QUANTITY),
            "memory": matched(capacity.get("memory"), QUANTITY),
        })
    pods, cache = [], {}
    for item in (pod_doc or {}).get("items", []):
        metadata, spec = item["metadata"], item["spec"]
        role, node = metadata.get("labels", {}).get("role"), spec.get("nodeName")
        if metadata.get("namespace") != NAMESPACE or role not in {"aggregator", "store"}:
            continue
        containers = spec.get("containers", [])
        pod = {
            "name": metadata["name"],
            "role": role,
            "node": node,
            "ready": item.get("status", {}).get("phase") == "Running" and ready(item),
            "images": [matched(container.get("image"), IMAGE) for container in containers],
        }
        if role == "store":
            writable_mounts = {}
            for container in containers:
                for mount in container.get("volumeMounts", []):
                    if not mount.get("readOnly", False):
                        writable_mounts.setdefault(mount["name"], set()).add(mount["mountPath"])
            disks = []
            for volume in spec.get("volumes", []):
                name = volume["name"]
                if name not in writable_mounts or any(key in volume for key in (
                    "emptyDir", "configMap", "downwardAPI", "secret", "projected",
                )):
                    continue
                disks.append({
                    "name": name,
                    "mount_paths": sorted(path for path in map(absolute_path, writable_mounts[name]) if path),
                    **disk_volume(context, volume, node, nodes, cache),
                })
            pod["writable_disks"] = disks
            pod["local_disks"] = bool(disks) and all(disk["local"] and disk["mount_paths"] for disk in disks)
        pods.append(pod)
    return {
        "kubernetes_inventory": node_doc is not None and pod_doc is not None,
        "nodes": observed_nodes,
        "pods": sorted(pods, key=lambda pod: pod["name"]),
    }


def topology_checks(snapshot):
    nodes = {node["name"]: node for node in snapshot["nodes"] if node["ready"]}
    agg_nodes = {pod["node"] for pod in snapshot["pods"] if pod["ready"] and pod["role"] == "aggregator"}
    agg_boots = {nodes[node]["boot_id"] for node in agg_nodes if node in nodes} - {None}
    stores = [pod for pod in snapshot["pods"] if pod["ready"] and pod["role"] == "store"]
    return {
        "kubernetes_inventory": snapshot["kubernetes_inventory"],
        "three_ready_aggregator_nodes_separate_boot_ids": len(agg_boots) >= 3,
        "ready_store_disks_node_local": bool(stores)
        and all(pod["node"] in nodes and pod["local_disks"] for pod in stores),
    }


def placement(path):
    if not path:
        return {}
    document = read_json(path)
    if "before" not in document or document.keys() - {"before", "after"}:
        raise ValueError("invalid placement phases")
    for phase in document.values():
        if not isinstance(phase, dict) or phase.keys() != SHARDS:
            raise ValueError("invalid placement shards")
        units = set()
        for copies in phase.values():
            if not isinstance(copies, list) or len(copies) != 2:
                raise ValueError("invalid placement copies")
            roles = set()
            for copy in copies:
                if not isinstance(copy, dict) or not {"node", "unit", "role"} <= copy.keys() or copy.keys() - {
                    "node", "unit", "role", "pod",
                } or not all(matched(value) for value in copy.values()):
                    raise ValueError("invalid placement copy")
                units.add(copy["unit"])
                roles.add(copy["role"])
            if roles != {"primary", "replica"}:
                raise ValueError("invalid placement roles")
        if len(units) != 2 * len(SHARDS):
            raise ValueError("placement physical units must be distinct")
    return document


class Probe:
    def __init__(self, base, audit_token, internal_key, interval):
        self.base = base
        self.audit = {"Authorization": "Bearer " + audit_token}
        self.internal = {"X-BNPB-Internal-Key": internal_key}
        self.interval = interval
        self.next_at = time.monotonic()

    def get(self, path, query, timeout, *, audit=False, deadline=None):
        start = max(time.monotonic(), self.next_at)
        if deadline is not None and deadline - start < timeout:
            return None, {}
        time.sleep(max(0, start - time.monotonic()))
        started = time.monotonic()
        self.next_at = started + self.interval
        observation = {"at": now()}
        try:
            status, body, elapsed = get_json(
                self.base, path + "?" + urlencode(query), self.audit if audit else self.internal, timeout
            )
        except (ValueError, OSError):
            status, body, elapsed = None, {}, (time.monotonic() - started) * 1000
            observation["error"] = "request_failed"
        observation.update(status=status, elapsed_ms=round(elapsed, 2), finished_at=now())
        if not isinstance(body, dict):
            observation["error"] = "invalid_response_shape"
            body = {}
        return observation, body


def checksum(probe, expected, timeout, *, deadline=None):
    observation, body = probe.get("/internal/audit/checksum", {"source": "BMKG"}, timeout, audit=True, deadline=deadline)
    if observation is None:
        return None
    count, digest = body.get("count"), matched(body.get("checksum"), DIGEST)
    try:
        as_of = utc(body["as_of"]).isoformat()
    except (KeyError, TypeError, ValueError):
        as_of = None
    complete = (
        observation["status"] == 200 and body.get("source") == "BMKG"
        and body.get("algorithm") == "sha256" and body.get("partial") is False
        and body.get("unavailable_units") == [] and as_of is not None and digest is not None
        and type(count) is int and count > 0 and observation["elapsed_ms"] <= timeout * 1000
    )
    partial = body.get("partial")
    units = body.get("unavailable_units")
    unavailable = units if isinstance(units, list) and all(matched(unit) for unit in units) else []
    observation.update(count=count if type(count) is int else None, checksum=digest, as_of=as_of,
                       partial=partial if type(partial) is bool else None,
                       unavailable_units=unavailable,
                       valid_partial=observation["status"] == 200 and body.get("source") == "BMKG"
                       and body.get("algorithm") == "sha256" and as_of is not None and digest is not None
                       and type(count) is int and count >= 0 and partial is True and bool(unavailable)
                       and len(set(unavailable)) == len(unavailable) and observation["elapsed_ms"] <= timeout * 1000,
                       complete=complete, matches_expected=complete and digest == expected)
    return observation


def copy_tuple(copy):
    return copy["node"], copy["unit"], copy["role"]


def location_samples(probe, hazard_ids, snapshot, expected, timeout):
    disks = {
        (pod["node"], pod["name"]) for pod in snapshot["pods"]
        if pod["role"] == "store" and pod["ready"] and pod["local_disks"]
    }
    boots = {node["name"]: node["boot_id"] for node in snapshot["nodes"] if node["ready"] and node["boot_id"]}
    samples, by_shard, groups = [], {}, {}
    for i, hazard in enumerate(hazard_ids, 1):
        observation, body = probe.get("/internal/audit/location", {"hazard_id": hazard}, timeout, audit=True)
        partial = body.get("partial")
        locations = body.get("locations")
        copies = [
            {key: copy[key] for key in ("node", "unit", "role")}
            for copy in locations if isinstance(copy, dict) and matched(copy.get("node"))
            and matched(copy.get("unit")) and copy.get("role") in ("primary", "replica")
        ] if isinstance(locations, list) else []
        pairs = frozenset(map(copy_tuple, copies))
        copy_nodes = {node for node, _, _ in pairs}
        rf2 = (
            observation["status"] == 200 and observation["elapsed_ms"] <= timeout * 1000
            and partial is False and body.get("hazard_id") == hazard
            and isinstance(locations, list) and len(locations) == 2 and len(pairs) == 2
            and len({unit for _, unit, _ in pairs}) == 2
            and {role for _, _, role in pairs} == {"primary", "replica"}
            and None not in {boots.get(node) for node in copy_nodes}
            and len({boots.get(node) for node in copy_nodes}) == 2
            and copy_nodes <= {node for node, _ in disks}
        )
        shard = next((unit for _, unit, role in pairs if role == "primary"), None) if rf2 else None
        if expected and rf2:
            shard = next((
                name for name, copies_at_node in expected.items()
                if pairs == frozenset(map(copy_tuple, copies_at_node))
                and all("pod" not in copy or (copy["node"], copy["pod"]) in disks for copy in copies_at_node)
            ), None)
            rf2 = shard is not None
        if rf2:
            if by_shard.setdefault(shard, pairs) != pairs:
                rf2 = False
            else:
                groups[i] = shard
        observation.update(sample=i, locations=copies, logical_group=shard, complete_rf2=rf2,
                           partial=partial if type(partial) is bool else None)
        samples.append(observation)
    per_node = Counter(node for pairs in by_shard.values() for node in {node for node, _, _ in pairs})
    all_units = {unit for pairs in by_shard.values() for _, unit, _ in pairs}
    return samples, by_shard, groups, {
        "location_samples_complete_rf2": len(groups) == len(hazard_ids),
        "three_logical_groups": len(by_shard) == len(SHARDS) and len(all_units) == 2 * len(SHARDS),
        "no_node_covers_all_groups": bool(per_node) and max(per_node.values()) < len(SHARDS),
    }


def hazard_page(probe, limit, timeout, cursor, boundary, phase, index, deadline=None):
    query = {"source": "BMKG", "limit": limit}
    if cursor is not None:
        query["cursor"] = cursor  # Only use a cursor actually returned by the API.
    observation, body = probe.get("/internal/hazards", query, timeout, deadline=deadline)
    if observation is None:
        return None, (), None
    items, units = body.get("items"), body.get("unavailable_units")
    partial, next_cursor = body.get("partial"), body.get("next_cursor")
    safe_units = [unit for unit in units if isinstance(unit, str) and unit in SHARDS] if isinstance(units, list) else []
    try:
        if not isinstance(items, list) or len(items) > limit:
            raise ValueError("invalid item count")
        keys = tuple((utc(item["occurred_at"]), item["hazard_id"].encode("utf-8")) for item in items
                     if isinstance(item, dict) and item.get("source") == "BMKG"
                     and isinstance(item.get("hazard_id"), str) and item["hazard_id"])
        ordered = (len(keys) == len(items) and all(a > b for a, b in zip(keys, keys[1:]))
                   and (not keys or boundary is None or boundary > keys[0]))
    except (KeyError, TypeError, ValueError):
        keys, ordered = (), False
    ordered_timestamps = [key[0] for key in keys]
    if boundary is not None:
        ordered_timestamps.insert(0, boundary[0])
    cursor_string = isinstance(next_cursor, str) and 0 < len(next_cursor) <= 8192
    envelope = (type(partial) is bool and isinstance(units, list) and len(safe_units) == len(units)
                and len(set(safe_units)) == len(safe_units) and partial == bool(units)
                and "next_cursor" in body and (next_cursor is None or cursor_string))
    cursor_ok = envelope and (
        cursor_string and (cursor is None or next_cursor == cursor) if partial else
        next_cursor is None or (next_cursor != cursor and isinstance(items, list) and len(items) == limit)
    )
    observation.update(
        phase=phase, page=index + 1, input_cursor=cursor is not None,
        item_count=len(items) if isinstance(items, list) else None,
        partial=partial if type(partial) is bool else None, unavailable_units=safe_units,
        timestamp_ties=sum(a == b for a, b in zip(ordered_timestamps, ordered_timestamps[1:])),
        ordered=ordered, cursor_ok=cursor_ok,
        valid=observation["status"] == 200 and observation["elapsed_ms"] <= timeout * 1000
        and ordered and cursor_ok,
    )
    return observation, keys, next_cursor if cursor_string else None


def baseline_pages(probe, args, observations):
    pages, cursor, boundary, first_cursor = [], None, None, None
    for index in range(args.pages):
        obs, keys, cursor = hazard_page(probe, args.limit, args.read_timeout, cursor, boundary, "baseline", index)
        observations.append(obs)
        if not obs["valid"] or obs["partial"] or not keys:
            break
        pages.append((keys, cursor is not None))
        if index == 0:
            first_cursor = cursor
        if cursor is None:
            break
        boundary = keys[-1]
    return pages, first_cursor


def compare_page(probe, args, baseline, observations, phase, state, deadline=None):
    index, cursor = state
    boundary = baseline[index - 1][0][-1] if index else None
    obs, keys, next_cursor = hazard_page(
        probe, args.limit, args.read_timeout, cursor, boundary, phase, index, deadline
    )
    if obs is None:
        return False, state
    observations.append(obs)
    if not obs["valid"]:
        return True, state
    if obs["partial"]:
        # On an incomplete first page replay its opaque start marker. A later complete
        # head comparison proves that the marker did not skip the baseline's first page.
        return True, (index, next_cursor)
    obs["matches_baseline"] = (keys, next_cursor is not None) == baseline[index]
    if not obs["matches_baseline"] or index + 1 == len(baseline) or next_cursor is None:
        return True, (0, None)
    return True, (index + 1, next_cursor)


def faults(path, watch_start, watch_end, before, after, before_groups, after_groups):
    events = []
    try:
        for row in read_jsonl(path):
            event = row.get("event")
            if event not in FAULT_EVENTS:
                continue
            stamp, node, unit, timeline = utc(row["at"]), matched(row.get("node")), matched(row.get("unit")), row.get("timeline")
            if node is None or unit is None or type(timeline) is not int or timeline < 1:
                return "invalid", events, None
            events.append({"event": event, "at": stamp.isoformat(), "node": node, "unit": unit, "timeline": timeline})
            if len(events) > len(FAULT_EVENTS):
                return "duplicate", events, None
    except FileNotFoundError:
        return "missing", events, None
    except (KeyError, OSError, ValueError, TypeError):
        return "missing" if not path.is_file() else "invalid", events, None
    if {row["event"] for row in events} != set(FAULT_EVENTS):
        return "incomplete", events, None
    if tuple(row["event"] for row in events) != FAULT_EVENTS:
        return "misordered", events, None
    lost, fenced, promoted = events
    if not watch_start or not watch_end or not (
        utc(watch_start) <= utc(lost["at"]) < utc(fenced["at"]) < utc(promoted["at"]) <= utc(watch_end)
    ):
        return "outside_watch_or_misordered", events, None
    old = lost["node"], lost["unit"]
    new = promoted["node"], promoted["unit"]
    if (fenced["node"], fenced["unit"]) != old or lost["timeline"] != fenced["timeline"] or (
        old[0] == new[0] or promoted["timeline"] <= fenced["timeline"]
    ):
        return "inconsistent_transition", events, None
    matched_shards = {
        (group, after_groups[i]) for i, group in before_groups.items() if i in after_groups
        and (*old, "primary") in before[group] and (*new, "replica") in before[group]
        and (*new, "primary") in after[after_groups[i]]
    }
    if len(matched_shards) != 1:
        return "transition_not_observed_in_audit", events, None
    return "matched", events, next(iter(matched_shards))


def parser():
    args = argparse.ArgumentParser(
        description="Read live Kubernetes and Aggregator evidence; never deploys or executes a fault.",
        epilog="Use a real authorized cluster; fixtures do not provide live evidence. Keep the BMKG catalogue "
        "stable for page/checksum comparisons. Wait for WATCH READY on stderr before the separate operator "
        "starts the authorized scenario. Log loss, fencing confirmation and promotion within the watch; "
        "leave time for a live sample overlapping loss and a complete sample after promotion. Restore RF2 "
        "before post-watch checks. Full responses during failover are valid. Write actual observed UTC events "
        "to a fresh fault log, not planned timestamps. Volume projections show Kubernetes configuration, "
        "not host filesystem attestation.",
    )
    args.add_argument("--context", required=True, help="explicit kubectl context for read-only GETs")
    args.add_argument("--base-url", required=True, help="Aggregator ingress/base URL for live GETs")
    args.add_argument("--audit-token-file", required=True, help="file containing the dedicated Bearer Audit token")
    args.add_argument("--internal-key-file", required=True, help="file containing the distinct X-BNPB-Internal-Key")
    args.add_argument("--expected-checksum", required=True, help="assistant's authoritative lowercase SHA-256 for BMKG")
    args.add_argument("--hazard-id", action="append", required=True,
                      help="repeat for 3–30 distinct, known BMKG hazard IDs spanning all three logical shards")
    args.add_argument("--placement", help="optional JSON: {before: {shard-0: [{node,unit,role,pod?},...], "
                      "shard-1: [...], shard-2: [...]}, after?: {same shape}}. Use exact names; without a "
                      "phase map, primary unit names group identical RF2 copies without suffix parsing; optional "
                      "pod names bind tuples to specific ready disk-local store pods, otherwise match by node.")
    args.add_argument("--fault-log", required=True, type=Path,
                      help="fresh JSONL: {event,at,node,unit,timeline} for node_disk_lost, old_primary_fenced, "
                      "new_primary_ready in order; loss/fence share old primary and positive integer PostgreSQL "
                      "timeline; promoted prior replica must advance timeline. Other event names are ignored.")
    args.add_argument("--watch-seconds", type=int, default=60, help="watch length: 10–300 seconds (default 60)")
    args.add_argument("--interval-seconds", type=int, default=1, help="minimum interval between API GET starts: 1–10 seconds")
    args.add_argument("--pages", type=int, default=3, help="maximum pages per traversal: 2–10 (default 3)")
    args.add_argument("--limit", type=int, default=50, help="BMKG page size: 1–100 (default 50)")
    args.add_argument("--read-timeout", type=int, default=2, help="hazards GET budget: 1–10 seconds (default 2)")
    args.add_argument("--audit-timeout", type=int, default=8, help="Audit GET budget: 1–60 seconds (default 8)")
    args.add_argument("--require-partial", action="store_true", help="scenario intends a sampled unavailable shard")
    args.add_argument("--output", required=True, type=Path, help="new JSON evidence file; refuses to overwrite")
    return args


def main():
    cli = parser()
    args = cli.parse_args()
    if not matched(args.expected_checksum, DIGEST):
        cli.error("expected checksum must be 64 lowercase hexadecimal digits")
    if (not 3 <= len(args.hazard_id) <= 30 or len(set(args.hazard_id)) != len(args.hazard_id)
            or any(not hazard or len(hazard) > 2048 for hazard in args.hazard_id)):
        cli.error("provide 3–30 distinct nonempty hazard IDs")
    if not (10 <= args.watch_seconds <= 300 and 1 <= args.interval_seconds <= 10 and 2 <= args.pages <= 10
            and 1 <= args.limit <= 100 and 1 <= args.read_timeout <= 10 and 1 <= args.audit_timeout <= 60
            and args.watch_seconds >= args.read_timeout + args.interval_seconds + 1):
        cli.error("page, timeout or rate bounds invalid; watch needs time for multiple probes")
    if args.output.exists():
        cli.error("output file already exists")

    expected = placement(args.placement)
    token, key = secret(args.audit_token_file), secret(args.internal_key_file)
    if token == key:
        cli.error("Audit token and Internal Key must be distinct")
    probe = Probe(args.base_url, token, key, args.interval_seconds)
    started = now()
    before = topology(args.context)
    checksum_before = checksum(probe, args.expected_checksum, args.audit_timeout)
    samples_before, groups_before, sample_groups_before, locations_before = location_samples(
        probe, args.hazard_id, before, expected.get("before"), args.read_timeout
    )
    pages = []
    baseline, first_cursor = baseline_pages(probe, args, pages)
    checks = {f"before_{key}": passed for key, passed in {**topology_checks(before), **locations_before}.items()}
    observed_count = sum(len(keys) for keys, _ in baseline)
    count = checksum_before["count"]
    checks.update(
        checksum_before_matches=checksum_before["matches_expected"],
        baseline_multiple_pages=len(baseline) >= 2,
        baseline_traversal_consistent=bool(baseline) and len(pages) == len(baseline)
        and type(count) is int and (observed_count < count if baseline[-1][1] else observed_count == count),
    )
    observations = {
        "namespace": NAMESPACE,
        "expected_checksum": args.expected_checksum,
        "settings": {"limit": args.limit, "pages": args.pages, "watch_seconds": args.watch_seconds,
                     "interval_seconds": args.interval_seconds, "read_timeout": args.read_timeout,
                     "audit_timeout": args.audit_timeout, "require_partial": args.require_partial,
                     "mapped_phases": sorted(expected)},
        "topology_before": before, "checksum_before": checksum_before, "locations_before": samples_before,
        "page_probes": pages,
    }
    if not all(checks.values()):
        observations["watch_status"] = "not_started_baseline_failed"
        return report(args.output, FLAG, started, checks, observations)

    watch_start = now()
    deadline = time.monotonic() + args.watch_seconds
    print(f"WATCH READY at {watch_start}; operator window: {args.watch_seconds}s", file=sys.stderr, flush=True)
    # Start from the real cursor returned by the complete baseline head.
    state = (1, first_cursor)
    while True:
        sampled, state = compare_page(probe, args, baseline, pages, "watch", state, deadline)
        if not sampled:
            break
    time.sleep(max(0, deadline - time.monotonic()))
    watch_end = now()
    watch = [obs for obs in pages if obs["phase"] == "watch"]
    last_watch = watch[-1] if watch else {}
    # Resume any pending partial boundary with the same cursor after the watch ends.
    if last_watch.get("partial") is True and last_watch.get("valid") is True:
        compare_page(probe, args, baseline, pages, "resume", state)
    state = (0, None)
    for _ in baseline:
        _, state = compare_page(probe, args, baseline, pages, "recovered", state)
    after = topology(args.context)
    checksum_after = checksum(probe, args.expected_checksum, args.audit_timeout)
    samples_after, groups_after, sample_groups_after, locations_after = location_samples(
        probe, args.hazard_id, after, expected.get("after"), args.read_timeout
    )
    fault_status, fault_events, transition = faults(
        args.fault_log, watch_start, watch_end, groups_before, groups_after, sample_groups_before, sample_groups_after
    )
    during_loss, after_promotion = [], []
    if transition is not None:
        lost, _, promoted = fault_events
        during_loss = [obs for obs in watch if utc(obs["at"]) < utc(promoted["at"])
                       and utc(obs["finished_at"]) >= utc(lost["at"])]
        after_promotion = [obs for obs in watch if utc(obs["at"]) >= utc(promoted["at"])]
    # A later matching complete read must clear every stationary partial retry.
    pending_partial = False
    for obs in pages:
        if obs["phase"] in {"watch", "resume"}:
            if obs["partial"] is True:
                pending_partial = True
            elif obs.get("matches_baseline") is True:
                pending_partial = False
    group_pairs = {(sample_groups_before[i], group) for i, group in sample_groups_after.items() if i in sample_groups_before}
    recovered = [obs for obs in pages if obs["phase"] == "recovered" and obs.get("matches_baseline") is True]
    checks.update({f"after_{key}": passed for key, passed in {**topology_checks(after), **locations_after}.items()})
    checks.update(
        observed_loss_fence_promotion=transition is not None,
        probes_overlap_loss_window=bool(during_loss),
        complete_probe_after_promotion=any(obs.get("matches_baseline") is True for obs in after_promotion),
        partial_observation_requirement_met=not args.require_partial or any(obs["partial"] is True for obs in during_loss),
        page_requests_bounded_ordered_and_cursor_safe=all(obs["valid"] for obs in pages),
        complete_pages_match_baseline=all(obs.get("matches_baseline", True) for obs in pages),
        partial_cursor_recovered=not pending_partial,
        recovered_traversal_matches_baseline=[obs["page"] for obs in recovered] == list(range(1, len(baseline) + 1)),
        checksum_after_matches=checksum_after["matches_expected"] and checksum_after["count"] == checksum_before["count"],
        same_sample_shards=len(group_pairs) == len(SHARDS)
        and (len(expected) < 2 or all(before_name == after_name for before_name, after_name in group_pairs)),
    )
    observations.update(
        watch_status="finished", watch_start=watch_start, watch_end=watch_end,
        topology_after=after, checksum_after=checksum_after, locations_after=samples_after,
        fault_log_status=fault_status, fault_events=fault_events, transition_groups=transition,
        probes_overlapping_loss=len(during_loss), probes_after_promotion=len(after_promotion),
        partial_watch_probes=sum(obs["partial"] is True for obs in watch),
    )
    return report(args.output, FLAG, started, checks, observations)


if __name__ == "__main__":
    raise SystemExit(run(main))
