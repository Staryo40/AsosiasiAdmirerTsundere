"""Capture delivery and Audit evidence with GETs and existing observations only."""

import argparse
import base64
import hashlib
import json
import math
import re
from urllib.parse import quote, urlencode

from evidence_support import get_json, now, read_json, read_jsonl, report, run, secret, utc


FLAG = "delivery"
SOURCES = ("BMKG", "PVMBG", "SATELIT", "SENSOR_PANTAI")
ROLES = ("dashboard", "notifications", "demo")
ITEM_FIELDS = {"message_id", "source", "received_at", "reason", "retryable", "attempts", "status"}
TIME = re.compile(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?(?:Z|[+-]\d\d:\d\d)", re.ASCII)
SHA256 = re.compile(r"[0-9a-f]{64}")
TRACE_FIELDS = {"event", "time", "vhost", "queue", "message_id", "evidence_sha256"}
TRACE_DETAILS = {
    "retained_before": {"payload_sha256"},
    "retained_after": {"payload_sha256"},
    "publish_nack": {"messages_ready", "cause"},
    "message_received": {"delivery_id"},
    "message_duplicate_skipped": {"delivery_id"},
    "effect_committed": {"delivery_id"},
}
TRACE_HELP = """Normalized broker-events JSONL (export from actual observations; never infer from configuration):
  Every row: event, time (RFC 3339), vhost, queue, message_id (AMQP property),
  evidence_sha256 (SHA-256 of the originating capture record).
  retained_before / retained_after: add payload_sha256 (SHA-256 of broker-observed
  message bytes; same ID and bytes on both sides of the rejection). The before
  observation must predate the during invocation; the after observation must
  follow it. Keep the exact before row in the later trace.
  publish_nack: add messages_ready (integer observed at the NACK) and cause
  (max-length). Use an actual negative publisher confirm attributable to this
  queue at capacity, not a timeout, a full queue alone, or a consumer NACK.
  The rejected publish can have a different ID from the retained message.
  message_received / message_duplicate_skipped / effect_committed: add delivery_id
  (one distinct ID for each actual delivery; use that ID on its effect or skip).
  Normalize consumer receipts and duplicate skips from their actual logs; use an
  observed durable effect, e.g. the consumer's post-commit dashboard_update_recorded,
  alert_sent or feed_entry_recorded log. Join an effect log that lacks message_id
  to its unique receipt using the consumer instance, correlation_id and log order;
  omit ambiguous matches. Include all receipts, skips and durable effects for the
  selected queue/message in the run.
  Never put raw messages, bodies, reasons, tokens or credentials in this JSONL.
"""


def text(value):
    return isinstance(value, str) and bool(value.strip())


def sha256(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def digest(value):
    return isinstance(value, str) and SHA256.fullmatch(value) is not None


def request(base, path, headers, timeout):
    try:
        status, body, _ = get_json(base, path, headers, timeout)
        return status, body
    except (ValueError, OSError):
        return None, None


def binding_projection(data, vhost, queue):
    if not isinstance(data, list) or any(not isinstance(row, dict) for row in data):
        return False, False, None
    routes = [row for row in data if row.get("vhost") == vhost
              and row.get("destination") == queue and row.get("destination_type") == "queue"
              and row.get("arguments") == {}]
    default = any(row.get("source") == "" and row.get("routing_key") == queue for row in routes)
    fanout = any(row.get("source") == "hazard.events" and row.get("routing_key") == "" for row in routes)
    return default, fanout, len(data) if len(routes) == len(data) else None


def numeric_setting(arguments, policy, key):
    values = [mapping[name] for mapping, name in ((arguments, "x-" + key), (policy, key)) if name in mapping]
    valid = all(type(value) is int and value >= 0 for value in values)
    return (min(values) if values and valid else None), valid


def queues(args, timeout, headers):
    checks, observed = {}, {}
    for role in args.consumer_role:
        main = "hazard.events." + role
        for name, limit in ((main, 10000), (main + ".failed", 1000)):
            failed = name != main
            path = f"/api/queues/{quote(args.vhost, safe='')}/{quote(name, safe='')}"
            code, raw = request(args.broker_url, path + "?disable_stats=true", headers, timeout)
            data = raw if code == 200 and isinstance(raw, dict) else {}
            binding_fields = "?columns=source,vhost,destination,destination_type,routing_key,arguments"
            binds_code, binds = request(args.broker_url, path + "/bindings" + binding_fields, headers, timeout)
            default, fanout, binding_count = binding_projection(binds, args.vhost, name)
            arguments, policy = data.get("arguments"), data.get("effective_policy_definition")
            config_read = isinstance(arguments, dict) and isinstance(policy, dict)
            effective = None
            if config_read:
                # RabbitMQ 4.3 quorum: take the lesser numeric limit but prefer the
                # broker-merged regular/operator policy to the argument for overflow.
                length, length_valid = numeric_setting(arguments, policy, "max-length")
                size, size_valid = numeric_setting(arguments, policy, "max-length-bytes")
                overflow = policy.get("overflow", arguments.get("x-overflow", "drop-head"))
                ttl = "x-message-ttl" in arguments or "message-ttl" in policy
                expires = "x-expires" in arguments or "expires" in policy
                effective = {
                    "max_length": length,
                    "max_length_bytes": size,
                    "overflow": overflow if overflow in ("reject-publish", "drop-head", "reject-publish-dlx") else "other",
                    "message_ttl_present": ttl,
                    "expires_present": expires,
                }
            prefix = name + ":"
            checks[prefix + "identity_quorum_durable"] = (
                data.get("name") == name and data.get("vhost") == args.vhost and data.get("type") == "quorum"
                and data.get("durable") is True and data.get("auto_delete") is False
                and data.get("exclusive") is False
            )
            checks[prefix + "effective_bounds"] = (
                config_read and length_valid and size_valid and length == limit and overflow == "reject-publish"
            )
            checks[prefix + "no_ttl_or_expiry"] = config_read and not ttl and not expires
            checks[prefix + "bindings"] = (
                binds_code == 200 and default and fanout == (not failed) and binding_count == (1 if failed else 2)
            )
            observed[name] = {
                "queue_http_status": code, "bindings_http_status": binds_code,
                "expected_max_length": limit, "effective": effective,
                "binding_count": binding_count, "default_binding": default, "hazard_events_binding": fanout,
            }
    return checks, observed


def audit_scan(args, timeout, headers, target_ids):
    path = "/internal/audit/unprocessed"
    metadata_ok, source_ok, cursor_ok, terminal = True, True, True, False
    page_count, code, issue = 0, None, "page_limit"
    cursor, cursors, seen_ids, matches = None, set(), set(), {}
    for _ in range(args.max_pages):
        query = {"source": args.source, "limit": args.limit}
        if cursor is not None:
            query["cursor"] = cursor
        code, data = request(args.audit_url, path + "?" + urlencode(query), headers, timeout)
        if code != 200:
            issue = "no_valid_json_response" if code is None else "http_status"
            break
        if not isinstance(data, dict) or set(data) != {"items", "next_cursor"}:
            metadata_ok, issue = False, "unexpected_envelope"
            break
        items, next_cursor = data["items"], data["next_cursor"]
        if not isinstance(items, list) or len(items) > args.limit:
            metadata_ok, issue = False, "invalid_items"
            break
        for item in items:
            if not isinstance(item, dict) or set(item) != ITEM_FIELDS or not valid_item(item):
                metadata_ok, issue = False, "invalid_metadata"
                break
            if item["source"] != args.source:
                source_ok, issue = False, "out_of_filter_item"
                break
            message_id = item["message_id"]
            if message_id in seen_ids:
                cursor_ok, issue = False, "repeated_message_id"
                break
            seen_ids.add(message_id)
            if message_id in target_ids:
                matches[target_ids[message_id]] = item["status"]
        if not (metadata_ok and source_ok and cursor_ok):
            break
        page_count += 1
        if next_cursor is None:
            terminal, issue = True, None
            break
        try:
            valid_cursor = isinstance(next_cursor, str) and 1 <= len(next_cursor.encode("utf-8")) <= 4096
        except UnicodeError:
            valid_cursor = False
        if not valid_cursor or next_cursor in cursors:
            cursor_ok, issue = False, "invalid_or_repeated_cursor"
            break
        cursors.add(next_cursor)
        cursor = next_cursor
    checks = {
        "audit_http_200": code == 200,
        "audit_exact_metadata": metadata_ok and page_count > 0,
        "audit_source_filter": source_ok and page_count > 0,
        "audit_nonlooping_cursors": cursor_ok and terminal,
        "audit_terminal_null": terminal,
    }
    return checks, {
        "pages": page_count, "items": len(seen_ids), "last_http_status": code, "issue": issue,
        "matches": matches,
    }


def valid_item(item):
    status, received = item["status"], item["received_at"]
    if not (
        text(item["message_id"]) and text(item["reason"]) and item["source"] in SOURCES
        and text(received) and TIME.fullmatch(received) and received.endswith(("Z", "+00:00", "-00:00"))
        and status in ("retrying", "parked") and item["retryable"] is (status == "retrying")
        and type(item["attempts"]) is int and item["attempts"] >= 0
    ):
        return False
    try:
        utc(received)
    except (ValueError, OverflowError):
        return False
    return True


def trace_rows(args, started):
    by_event = {key: [] for key in TRACE_DETAILS}
    if not args.broker_events:
        return by_event, "not_supplied"
    seen = set()
    try:
        for row in read_jsonl(args.broker_events):
            if row.get("queue") != args.delivery_queue or row.get("vhost") != args.vhost:
                continue
            event = row.get("event")
            if event != "publish_nack" and row.get("message_id") != args.delivery_id:
                continue
            if not isinstance(event, str) or event not in TRACE_DETAILS:
                return by_event, "unknown_event"
            if (set(row) != TRACE_FIELDS | TRACE_DETAILS[event] or not text(row["message_id"])
                    or not digest(row["evidence_sha256"]) or row["evidence_sha256"] in seen
                    or not isinstance(row["time"], str) or not TIME.fullmatch(row["time"])):
                return by_event, "invalid_or_repeated_event"
            seen.add(row["evidence_sha256"])
            instant = utc(row["time"])
            if instant > started:
                return by_event, "event_after_capture_started"
            if event in ("retained_before", "retained_after"):
                if not digest(row["payload_sha256"]):
                    return by_event, "invalid_payload_digest"
            elif event == "publish_nack":
                if type(row["messages_ready"]) is not int or row["messages_ready"] < 0 or not text(row["cause"]):
                    return by_event, "invalid_nack_observation"
            elif not text(row["delivery_id"]):
                return by_event, "invalid_delivery_identity"
            by_event[event].append((instant, row))
    except (ValueError, OSError, OverflowError):
        return by_event, "unreadable_or_invalid_jsonl"
    return by_event, None


def retention_projection(event):
    if event is None:
        return None
    return {
        "time": event[1]["time"], "payload_sha256": event[1]["payload_sha256"],
        "evidence_sha256": event[1]["evidence_sha256"],
    }


def broker_evidence(args, started, prior_obs, prior_finished):
    rows, issue = trace_rows(args, started)
    before = rows["retained_before"][0] if len(rows["retained_before"]) == 1 else None
    after = rows["retained_after"][0] if len(rows["retained_after"]) == 1 else None
    before_obs = retention_projection(before)
    checks = {
        "broker_trace_readable": issue is None,
        "broker_retained_before_observed": issue is None and before is not None,
    }
    observations = {
        "queue": args.delivery_queue, "message_id_sha256": sha256(args.delivery_id), "trace_issue": issue,
        "retained_before": before_obs, "retained_after": retention_projection(after),
        "publish_nacks": len(rows["publish_nack"]), "nacks_at_capacity": 0,
        "deliveries": len(rows["message_received"]), "duplicate_skips": len(rows["message_duplicate_skipped"]),
        "durable_effects": len(rows["effect_committed"]),
    }
    if args.phase == "during":
        return checks, observations

    previous_broker = prior_obs.get("broker", {})
    linked = (
        isinstance(previous_broker, dict) and previous_broker.get("queue") == args.delivery_queue
        and previous_broker.get("message_id_sha256") == observations["message_id_sha256"]
        and before_obs is not None and previous_broker.get("retained_before") == before_obs
    )
    nacks = [row for t, row in rows["publish_nack"]
             if before and after and before[0] < t < after[0]
             and row["messages_ready"] >= 10000 and row["cause"] == "max-length"]
    observations["nacks_at_capacity"] = len(nacks)
    if nacks:
        nack = min(nacks, key=lambda row: utc(row["time"]))
        observations["capacity_nack"] = {
            "time": nack["time"], "messages_ready": nack["messages_ready"],
            "evidence_sha256": nack["evidence_sha256"], "message_id_sha256": sha256(nack["message_id"]),
        }
    else:
        observations["capacity_nack"] = None
    retention = (
        before is not None and after is not None and prior_finished is not None
        and before[0] < prior_finished < after[0]
        and before[1]["payload_sha256"] == after[1]["payload_sha256"]
    )
    receipts = {row["delivery_id"]: t for t, row in rows["message_received"]}
    skips = {row["delivery_id"]: t for t, row in rows["message_duplicate_skipped"]}
    effects = rows["effect_committed"]
    deliveries_linked = (
        len(receipts) >= 2 and len(receipts) == observations["deliveries"]
        and after is not None and min(receipts.values()) >= after[0]
    )
    deduplicated = False
    if deliveries_linked and len(effects) == 1 and skips and len(skips) == observations["duplicate_skips"]:
        effect_time, effect = effects[0]
        applied = effect["delivery_id"]
        deduplicated = applied in receipts and receipts[applied] <= effect_time and all(
            delivery != applied and delivery in receipts and receipts[delivery] <= t and effect_time <= t
            for delivery, t in skips.items()
        )
    observations["durable_effect"] = (
        {"time": effects[0][1]["time"], "evidence_sha256": effects[0][1]["evidence_sha256"]}
        if len(effects) == 1 else None
    )
    checks.update({
        "broker_trace_linked_to_during": issue is None and linked,
        "broker_same_message_retained_after": issue is None and retention,
        "broker_nack_at_capacity": issue is None and bool(nacks),
        "consumer_multiple_deliveries": issue is None and deliveries_linked,
        "consumer_exactly_one_durable_effect_with_duplicate_skip": issue is None and deduplicated,
    })
    return checks, observations


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__, epilog=TRACE_HELP,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--phase", choices=("during", "after"), required=True)
    parser.add_argument("--broker-url", required=True, help="RabbitMQ management base URL (without /api)")
    parser.add_argument("--broker-user", required=True, help="RabbitMQ management Basic username")
    parser.add_argument("--broker-password-file", required=True, help="file containing the Basic password")
    parser.add_argument("--audit-url", required=True, help="Aggregator base URL")
    parser.add_argument("--audit-token-file", required=True, help="file containing the dedicated Audit Bearer token")
    parser.add_argument("--vhost", default="/", help="RabbitMQ vhost (default: /)")
    parser.add_argument("--consumer-role", action="append", help="RabbitMQ consumer username; repeat for all three if overriding dashboard, notifications, demo")
    parser.add_argument("--source", choices=SOURCES, required=True, help="filter every Audit page to this source")
    parser.add_argument("--limit", type=int, default=1, help="Audit page size (1–1000, default: 1)")
    parser.add_argument("--max-pages", type=int, default=1000, help="maximum pages per capture (default: 1000)")
    parser.add_argument("--timeout", type=float, default=5.0, help="seconds per GET (default: 5)")
    parser.add_argument("--retrying-id", help="known Audit message ID to observe as retrying during")
    parser.add_argument("--parked-id", help="known Audit message ID to observe as parked during")
    parser.add_argument("--recovered-id", help="during-phase Audit ID to verify absent after recovery/replay")
    parser.add_argument("--during", help="successful during-phase JSON file to link the after phase to")
    parser.add_argument("--delivery-queue", required=True, help="main consumer queue for the delivery trace")
    parser.add_argument("--delivery-id", required=True, help="retained AMQP message ID to match in the delivery trace")
    parser.add_argument("--broker-events", help="normalized existing broker and consumer JSONL observations")
    parser.add_argument("--out", required=True, help="new JSON evidence output path")
    args = parser.parse_args()
    args.consumer_role = args.consumer_role or ROLES
    if (len(set(args.consumer_role)) != 3 or len(args.consumer_role) != 3
            or not all(re.fullmatch(r"[a-z0-9-]+", role) for role in args.consumer_role)):
        raise ValueError("expected three distinct consumer roles")
    if args.delivery_queue not in {"hazard.events." + role for role in args.consumer_role}:
        raise ValueError("delivery queue must be a consumer-owned main queue")
    if not 1 <= args.limit <= 1000 or args.max_pages <= 0 or not math.isfinite(args.timeout) or args.timeout <= 0:
        raise ValueError("invalid limits or timeout")
    if not text(args.vhost) or not text(args.delivery_id) or not text(args.broker_user) or ":" in args.broker_user:
        raise ValueError("invalid broker identifiers")
    return args


def main():
    args = parse_args()
    started_at = now()
    started = utc(started_at)
    basic = base64.b64encode(f"{args.broker_user}:{secret(args.broker_password_file)}".encode("utf-8"))
    checks, queue_obs = queues(args, args.timeout, {"Authorization": "Basic " + basic.decode("ascii")})
    scope = sha256(json.dumps([args.broker_url.rstrip("/"), args.audit_url.rstrip("/"),
                               args.vhost, args.source, sorted(args.consumer_role)], separators=(",", ":")))
    prior_obs, prior_started, prior_finished = {}, None, None
    if args.phase == "during":
        ids_valid = text(args.retrying_id) and text(args.parked_id) and args.retrying_id != args.parked_id
        targets = {args.retrying_id: "retrying", args.parked_id: "parked"} if ids_valid else {}
    else:
        targets = {args.recovered_id: "recovered"} if text(args.recovered_id) else {}
        prior = {}
        if args.during:
            try:
                prior = read_json(args.during)
                prior_started, prior_finished = utc(prior["started_at"]), utc(prior["finished_at"])
            except (ValueError, TypeError, OSError, KeyError, OverflowError):
                prior_finished = None
        recorded = prior.get("observations")
        if (prior.get("flag") == FLAG and prior.get("status") == "pass" and isinstance(recorded, dict)
                and recorded.get("phase") == "during" and recorded.get("scope_sha256") == scope
                and prior_finished is not None and prior_started <= prior_finished < started):
            prior_obs = recorded
        checks["linked_successful_during_capture"] = bool(prior_obs)
    audit_checks, audit_obs = audit_scan(args, args.timeout, {"Authorization": "Bearer " + secret(args.audit_token_file)}, targets)
    checks.update(audit_checks)
    audit_obs["target_ids_sha256"] = {label: sha256(message_id) for message_id, label in targets.items()}
    if args.phase == "during":
        checks["audit_distinct_expected_ids"] = ids_valid
        checks["audit_paginated_during"] = audit_obs["pages"] > 1 and audit_checks["audit_terminal_null"]
        for label in ("retrying", "parked"):
            checks["audit_known_" + label] = audit_obs["matches"].get(label) == label
    else:
        prior_audit = prior_obs.get("audit", {})
        prior_ids = prior_audit.get("target_ids_sha256", {}) if isinstance(prior_audit, dict) else {}
        prior_matches = prior_audit.get("matches", {}) if isinstance(prior_audit, dict) else {}
        recovered = audit_obs["target_ids_sha256"].get("recovered")
        linked_id = recovered is not None and isinstance(prior_ids, dict) and isinstance(prior_matches, dict) and any(
            prior_ids.get(label) == recovered and prior_matches.get(label) == label for label in ("retrying", "parked")
        )
        checks["audit_recovered_id_seen_during"] = linked_id
        checks["audit_recovered_id_absent_from_complete_scan"] = (
            recovered is not None and "recovered" not in audit_obs["matches"] and all(audit_checks.values())
        )
    trace_checks, trace_obs = broker_evidence(args, started, prior_obs, prior_finished)
    checks.update(trace_checks)
    return report(args.out, FLAG, started_at, checks, {
        "phase": args.phase, "source": args.source, "scope_sha256": scope,
        "during_finished_at": prior_finished.isoformat() if prior_obs else None,
        "queues": queue_obs, "audit": audit_obs, "broker": trace_obs,
    })


if __name__ == "__main__":
    raise SystemExit(run(main))
