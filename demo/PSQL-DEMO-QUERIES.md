# Demo PostgreSQL query runbook

Prepared on **2026-10-23** against checkout `7fbf32f2bd8624db73fa214c841311fa03af063a` and its current working tree. This is the operator database interface for [CHECKLIST-DEMO.md](../CHECKLIST-DEMO.md), especially **IFACE-02**. Run the queries selectively when presenting the corresponding scenario; this file is not a script to execute from top to bottom.

All database queries below are read-only. Source schema/outage/delay changes belong to the existing [Swagger request playbook](swagger/REQUEST-PLAYBOOK.md); container lifecycle and load demonstrations belong to the existing acceptance runners. No configuration, source controls, or container lifecycle was changed while preparing this document.

## 1. Current repository and runtime findings

| Item | Inspected state / consequence |
| --- | --- |
| PostgreSQL service | Compose service **`postgres`**, image `postgres:18.6-alpine`. Use the service name instead of a guessed container name. |
| User/database | Compose defaults are `bnpb` / `bnpb`, but use the container's `POSTGRES_USER` and `POSTGRES_DB` rather than assuming defaults. |
| Canonical schema | Version **4**; migrations **1, 2, 3, 4**. PVMBG source V1/V2 and notification `schema_version=1` are separate version domains. |
| Store tables | `schema_migrations`, `volcano_reference`, `hazard_events`, `source_seismic`, `source_warnings`, `poll_state`, `ingest_rejects`, `outbox`. |
| Dynamic data | `hazard_events.attributes` is JSONB. There is no separate `confidence_level` column or raw PVMBG reports table. |
| Other state | Auth sessions are in memory; consumer receipts/deduplication are in each consumer's own SQLite store. PostgreSQL cannot show their token or receipt tables. |
| Live observation | At **2026-10-23 08:17:20 UTC**, PostgreSQL reported 18.6 and migrations 1–4. There were 2,641 BMKG hazards, 6,188 PVMBG hazards, 8,835 outbox rows, and zero pending notifications. No PVMBG row contained `confidence_level`. These counts change during polling. |
| Live service observation | Main application services, PostgreSQL, and RabbitMQ were healthy; `broker-init` exited 0. `consumer-3` was stopped with an older exit 255, so its later addition still needs fresh runtime evidence. |

The earlier checklist recorded an unreachable Docker daemon. The daemon was reachable during this inspection. These observations establish query prerequisites, not a P1–P5 acceptance PASS or proof that running images match the checkout.

### Checklist-to-query map

| Checklist evidence | Queries here | Evidence still needed elsewhere |
| --- | --- | --- |
| IFACE-02; ENV-01 | Q01–Q02 | Evaluated image/commit, working tree, configuration, container inventory. |
| MOCK-02; P1-01–03, P1-05 | Q03–Q06 | Raw agency responses and full public API responses; source generation cadence. |
| MOCK-06; P1-04, P1-07; P4-03 | Q07–Q10 | Fresh V1→V2 source/API witnesses and unchanged running container identities. |
| P2-01, P2-05–06 | Q11–Q12 | Actual delay/outage controls, API freshness, latency/load results, and no-restart evidence. |
| P4-03; P4-07 | Q02, Q07–Q10 | P04/T12 and two readable API witnesses. |
| P5-01–06; OBS-03 | Q13–Q17 | Same message at independent consumers, broker queue state, lifecycle/producer continuity. |
| P1-06; RISK-03 | Q18 | Source/control history and ingest logs explaining rejects. |
| P3; P4-01–02, P4-04–05; OBS-02, OBS-04–05 | No sufficient SQL proof | Auth HTTP checks, deployment/network probes, correlated logs, and call timing. |

## 2. Open the operator session

Run from the **repository root**, using the same Compose project/files as the demo stack. The host examples use the repository's `rtk` wrapper. The inner `sh` runs **inside PostgreSQL's container**; it resolves database/user variables without printing a password or DSN.

```bash
rtk proxy docker compose exec postgres sh -c 'PGOPTIONS="-c default_transaction_read_only=on" exec psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
```

If already using Docker Desktop's PostgreSQL container **Exec** shell, enter this instead; `rtk` is a host tool and is not required inside the container:

```sh
PGOPTIONS='-c default_transaction_read_only=on' psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"
```

Paste this setup at the `psql` prompt:

```sql
\set ON_ERROR_STOP on
\pset pager off
\pset null '(null)'
\timing on
SET default_transaction_read_only = on;
SET statement_timeout = '5s';
SET lock_timeout = '1s';
SET TIME ZONE 'UTC';
SHOW transaction_read_only;
```

Expected: `transaction_read_only = on`. Read-only mode is an operator-session guard, not a dedicated database authorization role. Keep autocommit enabled: do not leave a long transaction open across schema toggles, delays, or outages. Each query should see fresh committed data. `\timing` measures this operator query, not public API latency.

Optional transcript: `\o /tmp/psql-demo-evidence.txt` saves query results **inside the PostgreSQL container**; use `\o` to return output to the screen. Copy the file out if needed before container recreation. It is a local evidence file, not a committed demo asset. Use `\x auto` when full JSON becomes hard to read, and `\q` to exit.

For a single batch from the host, use `-T` and a quoted heredoc so the host shell does not expand SQL/psql variables:

```bash
rtk proxy docker compose exec -T postgres sh -c 'PGOPTIONS="-c default_transaction_read_only=on" exec psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' <<'SQL'
SET statement_timeout = '5s';
SET TIME ZONE 'UTC';
SELECT clock_timestamp() AS observed_at, count(*) AS canonical_hazards
FROM hazard_events;
SQL
```

Variables such as `old_id`, `new_id`, `hazard_id`, and `phase_start` below are **psql variables**, not SQL parameters or shell environment variables. `:'name'` safely quotes their values as SQL literals. `\gset` executes the preceding query without a semicolon and requires exactly one result row. Keep the same session for a scenario; on reconnect, re-enter saved witness IDs/timestamps.

## 3. Baseline and canonical storage

### Q01 — Identify the database session and migration history

Use for IFACE-02 and the schema baseline. Expect the current code's four migrations, not a fifth migration after enabling confidence.

```sql
SELECT clock_timestamp() AS observed_at,
       current_database() AS database_name,
       current_user AS database_user,
       current_setting('server_version') AS postgres_version,
       current_setting('transaction_read_only') AS read_only;

SELECT version, applied_at FROM schema_migrations ORDER BY version;

SELECT array_agg(version ORDER BY version) = ARRAY[1,2,3,4]
       AS matches_checkout_migrations
FROM schema_migrations;
```

If the migration comparison is false, record the mismatch before interpreting later queries against this checkout.

### Q02 — Show typed core columns and JSONB storage

```sql
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
ORDER BY table_name;

SELECT ordinal_position, column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'hazard_events'
ORDER BY ordinal_position;
```

Expected: eleven canonical event fields plus storage metadata `revision` and `updated_at`. `attributes` is `jsonb`; `confidence_level` is not a relational column. SQL column layout and canonical API JSON are different representations.

### Q03 — Counts, ingestion times, and representative IDs

```sql
SELECT source, hazard_type, count(*) AS total,
       min(occurred_at) AS earliest_source_time,
       max(occurred_at) AS latest_source_time,
       max(ingested_at) AS latest_first_ingestion,
       max(updated_at) AS latest_change
FROM hazard_events
GROUP BY source, hazard_type ORDER BY source;

SELECT hazard_id, source, source_ref_id, severity, area_name,
       occurred_at, ingested_at, revision
FROM hazard_events
ORDER BY ingested_at DESC, hazard_id COLLATE "C" DESC
LIMIT 10;
```

`occurred_at` is source event time; `ingested_at` is the first canonical ingestion time and stays stable on updates; `updated_at` advances on a changed revision. Counts here support dataset inspection, but cannot prove ≥20 records at **mock startup** or the source's generation rate.

### Q04 — Inspect one canonical record and its dynamic attributes

Copy an actual `hazard_id` from Q03 or the full API response. Replace the illustrative value:

```sql
\set hazard_id 'REPLACE_WITH_REAL_HAZARD_ID'
SELECT jsonb_pretty(to_jsonb(h) - 'revision' - 'updated_at') AS canonical_event
FROM hazard_events h WHERE hazard_id = :'hazard_id';

SELECT hazard_id, revision, updated_at, jsonb_pretty(attributes) AS attributes
FROM hazard_events WHERE hazard_id = :'hazard_id';
```

Compare all eleven canonical fields with `GET /v1/hazards/{hazard_id}?view=full` as Field/Ops. Media only receives the seven summary fields; this operator query does not bypass or test API authorization. One returned row proves this ID exists; zero rows means the ID or database/project selection needs checking.

## 4. P1 mapping witnesses

### Q05 — BMKG raw seismic mapping and warning correlation

Choose a BMKG hazard with associated warnings when possible. Set its real canonical ID:

```sql
\set bmkg_id 'REPLACE_WITH_REAL_BMKG_HAZARD_ID'
SELECT h.hazard_id, h.source_ref_id, s.event_id,
       s.payload->>'region_name' AS source_region, h.area_name,
       s.payload->>'epicenter_lat' AS source_lat, h.latitude,
       s.payload->>'epicenter_lon' AS source_lon, h.longitude,
       s.payload->>'occurred_at' AS source_time, h.occurred_at,
       h.attributes->'magnitude' AS magnitude, h.severity,
       jsonb_pretty(s.payload) AS stored_source_seismic
FROM hazard_events h
JOIN source_seismic s ON s.event_id = h.source_ref_id
WHERE h.source = 'BMKG' AND h.hazard_id = :'bmkg_id';

SELECT h.hazard_id, w.warning_id, w.related_event_id,
       w.issued_at, w.payload->>'threat_level' AS threat_level,
       w.payload->'affected_zones' AS affected_zones,
       w.payload->>'estimated_arrival' AS estimated_arrival,
       h.severity AS canonical_severity,
       h.attributes->'tsunami_warnings' AS canonical_warnings
FROM hazard_events h
JOIN source_warnings w ON w.related_event_id = h.source_ref_id
WHERE h.source = 'BMKG' AND h.hazard_id = :'bmkg_id'
ORDER BY w.issued_at, w.warning_id;
```

The raw tables are Aggregator-owned copies, not direct access to BMKG memory. Compare them with BMKG's separate seismic and warning HTTP responses. Highest compatible warning wins: `Waspada→WASPADA`, `Siaga→SIAGA`, `Awas→AWAS`. **Only without a compatible warning**, magnitude `<5→NORMAL`, `5≤m<6.5→WASPADA`, `m≥6.5→SIAGA`; warning severity overrides rather than taking a maximum with magnitude severity. Incompatible warnings for non-tsunami events remain stored but their association is quarantined; inspect Q18 if that occurs.

No warning rows is a valid no-warning case, not fanout/ingest failure. For a late warning, rerun Q04/Q05 and Q15 for the same hazard: enrichment can produce a higher revision and another outbox notification without creating a second hazard.

### Q06 — PVMBG reference enrichment and retained attributes

```sql
\set pvmbg_id 'REPLACE_WITH_REAL_PVMBG_HAZARD_ID'
SELECT h.hazard_id, h.source_ref_id AS source_report_id,
       h.occurred_at AS source_reported_at, h.severity,
       h.attributes->>'volcano_id' AS volcano_id,
       h.area_name, r.name AS reference_name,
       h.latitude, r.latitude AS reference_latitude,
       h.longitude, r.longitude AS reference_longitude,
       h.attributes->'eruption_count_24h' AS eruption_count_24h,
       h.attributes->'ash_column_height_m' AS ash_column_height_m,
       h.attributes ? 'confidence_level' AS has_confidence,
       h.attributes->'confidence_level' AS confidence_level,
       h.attributes->'source_extras' AS source_extras
FROM hazard_events h
LEFT JOIN volcano_reference r ON r.volcano_id = h.attributes->>'volcano_id'
WHERE h.source = 'PVMBG' AND h.hazard_id = :'pvmbg_id';
```

Compare `source_report_id` with the agency's `report_id`, and `occurred_at` with `reported_at`. Alert level maps directly to uppercase canonical severity. Area/coordinates come from BNPB's bundled volcano reference, not invented source coordinates. Other source fields remain in `attributes`; reserved-name collisions use `source_extras`. Unknown volcano IDs are rejected, not assigned fallback coordinates.

## 5. P1/P4 live schema evolution: V1 and V2 together

Sequence: capture schema fingerprint (Q10) → obtain and save a **fresh V1** source/API witness → Q07 → switch PVMBG to V2 through its admin API → Q08/Q09 → capture Q10 again → save full API/DB evidence → restore the original source version. The queries do not toggle the source schema.

### Q07 — Save a V1 witness and wait beyond polling overlap

Copy the canonical ID corresponding to the fresh V1 report observed through source/API. Do not choose an unrelated old database row as a substitute for the fresh live witness.

```sql
\set old_id 'REPLACE_WITH_FRESH_V1_HAZARD_ID'
SELECT hazard_id, source_ref_id, occurred_at, ingested_at,
       attributes ? 'confidence_level' AS has_confidence,
       attributes
FROM hazard_events WHERE source = 'PVMBG' AND hazard_id = :'old_id';
```

Expected: one PVMBG row and `has_confidence = f`. Before enabling V2, let the volcanic checkpoint advance sufficiently beyond that witness so overlap cannot legitimately re-fetch it. Set the **effective** configured `BNPB_POLL_OVERLAP` in seconds; the checkout default is 2 seconds.

```sql
\set overlap_seconds 2
SELECT h.hazard_id, h.occurred_at AS witness_time, p.checkpoint,
       p.checkpoint - make_interval(secs => :'overlap_seconds'::double precision)
         AS next_poll_since,
       h.occurred_at < p.checkpoint
         - make_interval(secs => :'overlap_seconds'::double precision)
         AS witness_outside_overlap
FROM hazard_events h CROSS JOIN poll_state p
WHERE h.hazard_id = :'old_id' AND h.source = 'PVMBG'
  AND p.endpoint = 'volcanic';
```

Wait for `witness_outside_overlap = t`. Checkpoint progress, not a fixed sleep, is the useful condition. Record the V2 phase boundary immediately before sending the authenticated `{"version":2}` admin request in the other interface:

```sql
SELECT clock_timestamp() AS schema_toggle_at \gset
\echo V2 phase begins at :schema_toggle_at
```

### Q08 — Find a newly generated V2 witness after the toggle

After the admin call succeeds and organic generation/polling has continued:

```sql
SELECT hazard_id, source_ref_id, occurred_at, ingested_at,
       attributes->'confidence_level' AS confidence_level
FROM hazard_events
WHERE source = 'PVMBG'
  AND attributes ? 'confidence_level'
  AND occurred_at > :'schema_toggle_at'::timestamptz
ORDER BY occurred_at DESC, hazard_id COLLATE "C" DESC
LIMIT 10;
```

Choose an ID matching a **new V2 agency report** and save it below. Filtering source time avoids counting an old report merely re-rendered with V2 fields. If zero rows return, wait/recheck source output and Q11/Q18; do not fabricate a row or insert confidence manually. At the initial live inspection, zero rows had confidence, so the V2-dependent result still requires the real toggle.

### Q09 — Prove the two distinct stored witnesses coexist

```sql
\set new_id 'REPLACE_WITH_NEW_V2_HAZARD_ID'
SELECT CASE WHEN hazard_id = :'old_id' THEN 'V1 witness'
            ELSE 'V2 witness' END AS witness,
       hazard_id, source, source_ref_id, occurred_at, ingested_at,
       attributes ? 'confidence_level' AS has_confidence,
       jsonb_typeof(attributes->'confidence_level') AS confidence_json_type,
       attributes->'confidence_level' AS confidence_level,
       jsonb_pretty(attributes) AS attributes
FROM hazard_events
WHERE source = 'PVMBG' AND hazard_id IN (:'old_id', :'new_id')
ORDER BY witness;

SELECT
  :'old_id' <> :'new_id' AS distinct_ids,
  EXISTS (SELECT 1 FROM hazard_events
          WHERE source = 'PVMBG' AND hazard_id = :'old_id'
            AND NOT (attributes ? 'confidence_level')) AS v1_preserved,
  EXISTS (SELECT 1 FROM hazard_events
          WHERE source = 'PVMBG' AND hazard_id = :'new_id'
            AND CASE WHEN jsonb_typeof(attributes->'confidence_level') = 'number'
                     THEN (attributes->>'confidence_level')::numeric BETWEEN 0 AND 1
                     ELSE false END) AS v2_numeric_confidence_in_range;
```

Expect **two rows**, distinct source report IDs, and all three booleans true. Absent confidence differs from numeric `0` and JSON `null`; zero is a valid V2 confidence. Read both IDs through Field/Ops `GET /v1/hazards/{id}?view=full` and compare the stored attributes with saved source/full API payloads.

Capture while V2 is active. Restoring V1 may strip confidence from overlapping records on their next poll. If the V1 witness gained confidence, revisit the overlap condition and source/API evidence rather than treating that update as failure of JSONB coexistence.

### Q10 — Compare a structural fingerprint before and after V2

Run this **same block twice**, once before Q07 and again while Q09's V2 evidence is present. Save both results. It follows the structural categories used by the repository's P04 helper: migration rows, columns, constraints, indexes, triggers, and functions in `public`.

```sql
SELECT clock_timestamp() AS observed_at,
       md5(jsonb_build_object(
         'migrations', (SELECT jsonb_agg(to_jsonb(m) ORDER BY version)
                        FROM schema_migrations m),
         'columns', (SELECT jsonb_agg(to_jsonb(c) ORDER BY table_name, ordinal_position)
                     FROM (SELECT table_name, ordinal_position, column_name,
                                  udt_name, is_nullable, column_default
                           FROM information_schema.columns
                           WHERE table_schema = 'public') c),
         'constraints', (SELECT jsonb_agg(to_jsonb(c) ORDER BY relation, conname)
                         FROM (SELECT conrelid::regclass::text AS relation,
                                      conname, pg_get_constraintdef(oid) AS definition
                               FROM pg_constraint
                               WHERE connamespace = 'public'::regnamespace) c),
         'indexes', (SELECT jsonb_agg(to_jsonb(i) ORDER BY tablename, indexname)
                     FROM (SELECT tablename, indexname, indexdef FROM pg_indexes
                           WHERE schemaname = 'public') i),
         'triggers', (SELECT jsonb_agg(to_jsonb(t) ORDER BY relation, tgname)
                      FROM (SELECT tgrelid::regclass::text AS relation, tgname,
                                   pg_get_triggerdef(oid) AS definition
                            FROM pg_trigger WHERE NOT tgisinternal
                              AND tgrelid IN (SELECT oid FROM pg_class
                                             WHERE relnamespace = 'public'::regnamespace)) t),
         'functions', (SELECT jsonb_agg(to_jsonb(f) ORDER BY definition)
                       FROM (SELECT pg_get_functiondef(oid) AS definition
                             FROM pg_proc WHERE pronamespace = 'public'::regnamespace
                               AND prokind = 'f') f)
       )::text) AS schema_fingerprint;
```

Expect the same fingerprint and migration versions 1–4, despite V2 attributes appearing. This is a structural comparison checksum, not a security hash or proof that no transient DDL occurred between snapshots. If it differs, inspect Q01/Q02 and the P04 structural evidence before claiming no migration. Container identity/image/start/restart evidence is still required separately.

## 6. P2 polling, slow-source isolation, and outage recovery

### Q11 — Inspect persisted endpoint poll state

Run at healthy baseline, during the real PVMBG outage, and after recovery:

```sql
SELECT clock_timestamp() AS observed_at, endpoint, source, checkpoint,
       last_attempt_at, last_successful_poll,
       clock_timestamp() - last_successful_poll AS age_since_success,
       stale_since, error_category
FROM poll_state ORDER BY endpoint;
```

Expect separate `seismic`, `warnings`, and `volcanic` rows. During PVMBG failure, volcanic success/checkpoint stops advancing and an error/stale marker should appear after failure detection, while BMKG polls continue. Recovery should advance volcanic success and clear persisted error/stale markers.

This table has **no `status` column**. The API computes `healthy`/`stale`/`unavailable` from persisted state, cached data, time, and configured thresholds. SQL age is supporting evidence; use the API's `sources.PVMBG` metadata for the actual client-visible freshness claim. Healthy successful polls can return no new events, so a checkpoint need not move on every poll.

### Q12 — Count genuinely new hazards during a measured phase

Immediately before slow-source/load/outage measurement, save a time boundary:

```sql
SELECT clock_timestamp() AS phase_start \gset
\echo Measurement phase begins at :phase_start
```

After the measured interval, run:

```sql
SELECT clock_timestamp() AS observed_at, s.source,
       count(h.hazard_id) AS new_canonical_hazards,
       count(h.hazard_id) FILTER (WHERE h.occurred_at >= :'phase_start'::timestamptz)
         AS also_generated_during_phase,
       min(h.ingested_at) AS first_new_ingestion,
       max(h.ingested_at) AS last_new_ingestion
FROM (VALUES ('BMKG'), ('PVMBG')) AS s(source)
LEFT JOIN hazard_events h ON h.source = s.source
  AND h.ingested_at >= :'phase_start'::timestamptz
GROUP BY s.source ORDER BY s.source;
```

During slow PVMBG or its outage, BMKG should still gain new ingestions. For recovery, save a **new** `phase_start` immediately before disabling the outage, then rerun after polling resumes; require new records from both sources. Zero new rows is a real observation to investigate, not proof of a crash. A new ingestion may be historical catch-up; `also_generated_during_phase` distinguishes source time within the phase. DB/source clock alignment matters for that stronger check.

This query cannot establish BMKG p95 <300 ms, ≥50 parallel connections for ≥60 seconds, error rates, or no restarts. Use P02/T03–T05 evidence for those criteria. Do not run expensive repeated database scans alongside official performance measurement; take bounded before/after observations.

## 7. P5 transactional outbox and ingest correlation

### Q13 — Producer publication/backlog overview

```sql
SELECT clock_timestamp() AS observed_at, count(*) AS total_notifications,
       count(*) FILTER (WHERE sent_at IS NOT NULL) AS marked_sent,
       count(*) FILTER (WHERE sent_at IS NULL) AS pending,
       min(created_at) FILTER (WHERE sent_at IS NULL) AS oldest_pending_created_at,
       max(sent_at) AS latest_sent_at
FROM outbox;

SELECT sequence, message_id, hazard_id, revision, correlation_id,
       created_at, attempts, next_attempt_at, sent_at
FROM outbox WHERE sent_at IS NULL ORDER BY sequence LIMIT 10;
```

Zero pending is normal when dispatch is fast. `sent_at` records successful publisher-confirmed broker acceptance followed by the DB update; it does **not** prove any consumer received or processed the message. `attempts` counts dispatch attempts, not only failures. The dispatcher processes the oldest pending sequence first, including its backoff, so a later due row cannot bypass that head.

### Q14 — Pick a stable message identity for two-consumer evidence

```sql
SELECT o.sequence, o.message_id, o.hazard_id, o.revision,
       o.correlation_id, o.payload->>'event_type' AS event_type,
       h.source, o.created_at, o.sent_at
FROM outbox o JOIN hazard_events h USING (hazard_id)
ORDER BY o.sequence DESC LIMIT 10;
```

Copy a real `message_id` from this list into Q16. Match **that same message ID**, hazard ID, revision, and ingest correlation in Aggregator publication logs and both consumer logs/receipts. Two growing `/stats` counters alone are insufficient for fanout. A later HTTP read's request correlation ID is independent of this ingest correlation.

### Q15 — Show a hazard's revisions and immutable notification snapshots

Uses `hazard_id` saved in Q04; set it again if selecting another event:

```sql
SELECT sequence, message_id, hazard_id, revision, correlation_id,
       payload->>'event_type' AS event_type,
       payload->>'schema_version' AS notification_schema_version,
       payload->'hazard_event'->>'severity' AS snapshot_severity,
       payload->'hazard_event'->'attributes' AS snapshot_attributes,
       created_at, attempts, sent_at
FROM outbox WHERE hazard_id = :'hazard_id' ORDER BY revision;
```

Revision 1 is `hazard.created`; changed canonical content produces `hazard.updated` and a higher revision. Unchanged overlapping polls produce no new notification. `hazard_events` stores the latest state, while each `outbox.payload.hazard_event` is the event snapshot at that revision. An older snapshot may correctly differ from today's hazard attributes/severity.

### Q16 — Inspect a notification and check its identity

```sql
\set message_id 'REPLACE_WITH_REAL_MESSAGE_ID'
SELECT message_id, hazard_id, revision, correlation_id,
       (payload->>'message_id' = message_id
        AND payload->>'hazard_id' = hazard_id
        AND (payload->>'revision')::bigint = revision
        AND payload->>'correlation_id' = correlation_id) IS TRUE
         AS envelope_identity_matches,
       jsonb_pretty(payload) AS notification
FROM outbox WHERE message_id = :'message_id';

SELECT h.hazard_id, h.revision AS current_revision,
       o.revision AS notification_revision,
       h.revision = o.revision AS notification_is_current_revision,
       o.payload->'hazard_event'->'attributes' = h.attributes
         AS snapshot_attributes_match_current_row,
       (o.payload->'hazard_event'->>'occurred_at')::timestamptz = h.occurred_at
         AS snapshot_source_time_matches
FROM outbox o JOIN hazard_events h USING (hazard_id)
WHERE o.message_id = :'message_id';
```

Expect one envelope with matching identity. Compare snapshot attributes to the current row **only when their revisions match**. Earlier revisions are history and may have different attributes. Compare timestamp instants as `timestamptz`, because Go JSON uses `Z` while PostgreSQL JSON may render UTC with `+00:00`.

### Q17 — Record publications during a stopped-consumer interval

Save a boundary just before the separately controlled consumer stop/addition scenario:

```sql
SELECT COALESCE(max(sequence), 0) AS outbox_start_sequence,
       clock_timestamp() AS fanout_start
FROM outbox \gset
\echo Outbox sequence boundary :outbox_start_sequence at :fanout_start
```

After producer progress has been observed, freeze an upper boundary and list that bounded interval:

```sql
SELECT COALESCE(max(sequence), 0) AS outbox_end_sequence FROM outbox \gset

SELECT o.sequence, o.message_id, o.hazard_id, o.revision,
       o.correlation_id, o.created_at, o.sent_at, h.source
FROM outbox o JOIN hazard_events h USING (hazard_id)
WHERE o.sequence > :'outbox_start_sequence'::bigint
  AND o.sequence <= :'outbox_end_sequence'::bigint
ORDER BY o.sequence LIMIT 50;

SELECT count(*) AS notifications_in_interval,
       count(*) FILTER (WHERE sent_at IS NOT NULL) AS marked_sent,
       count(*) FILTER (WHERE sent_at IS NULL) AS pending
FROM outbox
WHERE sequence > :'outbox_start_sequence'::bigint
  AND sequence <= :'outbox_end_sequence'::bigint;
```

The list is capped at 50; the count covers the full bounded interval. Sequence numbers are ordering markers and can have gaps; subtracting two sequence values is not a message count. A transaction already in flight at boundary capture may commit afterward, so pair this interval with container timestamps, broker observations, and logs rather than claiming exact stop-time membership from sequence alone.

For P5-03/04, prove the other consumer continued receiving and the stopped consumer's queue retained/caught up according to policy. PostgreSQL's pending outbox is **not** the stopped consumer's backlog: the producer may publish successfully while a consumer is offline. Queue backlog lives in RabbitMQ. For P5-05, take a new boundary **after consumer-3 has declared/bound its queue**, then demonstrate a new publication/receipt and unchanged producer identity. A newly declared queue need not contain events from before subscription.

## 8. Rejected ingestion and evidence limits

### Q18 — Explain missing data or reused source identities

```sql
SELECT endpoint, category, field, repair_status,
       count(*) AS distinct_reject_fingerprints,
       sum(occurrences) AS total_observations,
       max(last_seen_at) AS latest_seen
FROM ingest_rejects
GROUP BY endpoint, category, field, repair_status
ORDER BY latest_seen DESC;

SELECT endpoint, source_ref_id, source_timestamp, category, field,
       first_seen_at, last_seen_at, occurrences, repair_status,
       payload_sha256, payload_bytes
FROM ingest_rejects
ORDER BY last_seen_at DESC, reject_id DESC LIMIT 20;
```

Rejects retain bounded identity/fingerprint metadata rather than full rejected payloads. Repeated polls can increment `occurrences` on the same fingerprint. Restarted in-memory mocks can reuse IDs against persistent canonical storage; `source_identity_reused` is useful evidence of that condition. Invalid core fields/enums/confidence and unknown volcano IDs are not examples of supported additive schema evolution.

Do not delete rejects, truncate hazards/outbox, alter migrations, manually add confidence, or reset volumes to make demo evidence look clean. Preserve the actual observation and use the established scenario tooling or a separately planned fresh project if a clean baseline is required.

### What SQL cannot certify

- **P3:** login/role isolation, expiry, refresh/replay, and secret handling require HTTP/security evidence. Auth has no PostgreSQL token table.
- **P4 ownership:** database connections do not prove network isolation. Use the owner-positive/non-owner DNS and direct-IP probes, network membership, no-published-port check, and credential/mount review. Operator Docker-admin inspection is distinct from application store ownership.
- **P4 deployment / P1 no restart:** capture image/container IDs, start times, restart counts, and requests through the lifecycle. A schema fingerprint alone cannot prove service continuity.
- **P5 consumer delivery:** receipts are in consumer-owned SQLite and logs; queue ready/unacked/connected state is in RabbitMQ. Publisher confirmation is not consumer completion. Delivery is at least once, with possible duplicate publication after broker acceptance but before `sent_at` is committed.
- **Observability/performance:** use correlated service logs and real client timings. PostgreSQL's configured `jsonlog` files live under `PGDATA/log`; application stdout and database query timing are different evidence.

## 9. Source references and validation record

Queries were derived from current non-ignored implementation, not inferred framework tables:

- [Checklist database interface and P1–P5 evidence](../CHECKLIST-DEMO.md).
- [PostgreSQL service, network, and logging configuration](../bnpb/compose.yaml).
- [Migration runner and current schema version](../bnpb/internal/aggregator/store.go) and [initial schema](../bnpb/internal/aggregator/migrations/001_initial.sql), [identity constraint](../bnpb/internal/aggregator/migrations/002_outbox_identity.sql), [snapshot indexes](../bnpb/internal/aggregator/migrations/003_snapshot_indexes.sql), [outbox sequence index](../bnpb/internal/aggregator/migrations/004_outbox_sequence.sql).
- [BMKG mapping](../bnpb/internal/aggregator/seismic.go), [warning storage/correlation](../bnpb/internal/aggregator/store_warnings.go), [PVMBG mapping](../bnpb/internal/aggregator/volcanic.go), and [freshness calculation](../bnpb/internal/aggregator/freshness.go).
- [Canonical upsert and notification creation](../bnpb/internal/aggregator/store_hazard.go), [outbox dispatcher](../bnpb/internal/aggregator/outbox.go), and [delivery bookkeeping](../bnpb/internal/aggregator/store_outbox.go).
- [Existing read-only SQL helper, structural comparison, and witness checks](../testing/P04/checks.py); [P1 fresh witnesses and overlap handling](../testing/P01/acceptance.py).

PostgreSQL 18 API usage was checked through Context7 against the official [psql reference](https://www.postgresql.org/docs/18/app-psql.html) and [JSON functions/operators](https://www.postgresql.org/docs/18/functions-json.html).

At **2026-10-23 08:23 UTC**, all **23 SQL code blocks** executed successfully against the running PostgreSQL 18.6 database using read-only sessions and real BMKG/PVMBG/message IDs in place of the illustrative placeholders. Migration comparison returned true. All 18 relative document links resolved, query labels Q01–Q18 were unique and complete, and Markdown fences were balanced.

No V2 witness was available: Q08 returned zero rows, and Q09 was exercised with an intentionally absent V2 ID, correctly reporting `v2_numeric_confidence_in_range = f`. Thus positive V1/V2 coexistence, toggling, sustained progress/outage/recovery, and consumer lifecycle/fanout scenarios remain **NOT_RUN** in this preparation. Source controls were not changed to obtain a passing result. Re-run the relevant queries during the real scenario and retain its evidence.
