# BNPB Backend

The BNPB backend is the side of TsundereDISTRIBUTED that ingests both agency sources, owns the canonical record of every hazard, and serves it back out to downstream clients. It is not one service but six independent Go binaries sharing one module: an Aggregator that polls BMKG and PVMBG and owns the only database, an Auth service that issues opaque tokens to three downstream identities, a public Client-Facing API that is the single entrypoint for those identities, and three independent broker consumers that each react to the same event stream for their own purpose.

Only the Client-Facing API (`:8080`) is reachable from outside the Docker network — Auth, the Aggregator, and all three consumers are internal-only, reachable solely by other containers on their respective Docker networks. Every service shares the same structural conventions (`internal/platform`): validated config, health checks, structured JSON logs, correlation-ID propagation, and graceful shutdown.

## Table of Contents

- [Technology and Frameworks](#technology-and-frameworks)
- [Program Structure](#program-structure)
- [Program Requirements](#program-requirements)
- [Running the Application](#running-the-application)
- [Endpoints](#endpoints)
  - [Client-Facing API](#client-facing-api-public-8080)
  - [Auth](#auth-internal-8084)
  - [Aggregator](#aggregator-internal-8083)
  - [Consumers](#consumers-internal-8085-8086-8087)
  - [Field intake](#field-intake-internal-8088)
  - [Satellite feed](#satellite-feed-internal-8089)
  - [Shared platform behavior](#shared-platform-behavior)

## Technology and Frameworks

| Layer | Choice | Version |
| --- | --- | --- |
| Language | Go | 1.26.0 (toolchain 1.26.8) |
| Canonical storage driver | `pgx/v5` | 5.10.0 |
| Message broker client | `amqp091-go` | 1.15.0 |
| Per-consumer local storage | `modernc.org/sqlite` (pure Go, no CGO) | 1.60.1 |
| Lint | golangci-lint | v2.14.0 (CI-pinned) |
| Contract | OpenAPI 3.1 + JSON Schema | `bnpb/contracts/v1/` |

## Program Structure

| Service | Binary | Port | Purpose | Depends on |
| --- | --- | --- | --- | --- |
| Aggregator | `cmd/aggregator` | `8083` (internal) | Polls BMKG + PVMBG, maps to canonical `HazardEvent`, owns PostgreSQL, publishes to RabbitMQ, serves the internal read API | PostgreSQL, RabbitMQ, BMKG, PVMBG |
| Auth | `cmd/auth` | `8084` (internal) | Issues/refreshes/introspects opaque tokens for the three downstream identities | — (in-memory only) |
| Client-Facing API | `cmd/api` | `8080` (public) | The only entrypoint for downstream clients; facades Auth, reads from the Aggregator | Auth, Aggregator |
| consumer-1 | `cmd/consumer-1` | `8085` (internal) | Dashboard updater — latest state per hazard | RabbitMQ, own SQLite |
| consumer-2 | `cmd/consumer-2` | `8086` (internal) | Notifier — records SIAGA/AWAS alerts | RabbitMQ, own SQLite |
| consumer-3 | `cmd/consumer-3` | `8087` (internal, `demo` profile) | Regional portal feed — append-only log | RabbitMQ, own SQLite |

```text
bnpb/
├── cmd/                    # one main.go per binary above
├── internal/
│   ├── aggregator/         # polling, mapping, Canonical Store, outbox, AMQP publisher, read API
│   ├── auth/                # token issuance/refresh/introspection
│   ├── api/                  # public routes, Auth/Aggregator clients, projection
│   ├── consumer-1/            # own AMQP loop + SQLite (dashboard)
│   ├── consumer-2/            # own AMQP loop + SQLite (notifier)
│   ├── consumer-3/            # own AMQP loop + SQLite (portal feed)
│   └── platform/               # shared: config, httpserver, logging, correlation, service bootstrap
├── broker/                 # provision.sh, init.Dockerfile, rabbitmq.conf
├── contracts/v1/           # OpenAPI contract + JSON Schemas (Aggregator's internal read API)
├── compose.yaml
└── .env.example
```

`internal/<service>` packages never import each other — only `internal/platform`, enforced by `golangci-lint`'s `depguard` rule. No business logic is shared across services (KI.1); each consumer's connect/consume/ack loop is its own copy, not a shared library.

## Program Requirements

- **Docker** with **Compose 2.20 or newer**, or **Go 1.26.x** matching `go.mod` to build/run binaries directly.
- A free TCP port `8080` on the host (the only one this folder exposes on its own — see [Running the Application](#running-the-application) for the full port map when run from the repository root).
- `openssl` (or equivalent) to generate secrets.

## Running the Application

### 1. Prepare configuration

```bash
cp .env.example .env
```

At minimum, generate and fill in:

```dotenv
POSTGRES_PASSWORD=<secret>
RABBITMQ_DEFAULT_PASS=<secret>
BNPB_AUTH_INTERNAL_KEY=<random, 32+ bytes>
BNPB_AGGREGATOR_INTERNAL_KEY=<random, 32+ bytes>
BNPB_MEDIA_CLIENT_SECRET=<random, 32+ bytes>
BNPB_FIELD_CLIENT_SECRET=<random, 32+ bytes>
BNPB_OPS_CLIENT_SECRET=<random, 32+ bytes>
BNPB_RABBITMQ_PUBLISHER_PASSWORD=<secret>
BNPB_CONSUMER_1_RABBITMQ_PASSWORD=<secret>
BNPB_CONSUMER_2_RABBITMQ_PASSWORD=<secret>
BNPB_CONSUMER_3_RABBITMQ_PASSWORD=<secret>
BNPB_BMKG_API_KEY=<same value as bmkg/.env's BMKG_API_KEY>
BNPB_PVMBG_READER_CLIENT_ID=<same as pvmbg/.env's reader id>
BNPB_PVMBG_READER_CLIENT_SECRET=<same as pvmbg/.env's reader secret>
BNPB_CANONICAL_DATABASE_URL=postgresql://bnpb:<POSTGRES_PASSWORD>@postgres:5432/bnpb
BNPB_AMQP_URL=amqp://publisher:<BNPB_RABBITMQ_PUBLISHER_PASSWORD>@rabbitmq:5672
```

TTLs and every other tunable have working defaults in `.env.example` (poll intervals, timeouts, queue bounds, etc.).

### 2. Run with Docker — standalone

Run these from this folder:

```bash
docker compose up -d --build
docker compose ps --all
```

Check that `broker-init` shows **Exited (0)** before trusting the Aggregator or any consumer to be connected — it provisions the exchange, queue policies, and every per-service RabbitMQ user before anything else is allowed to start.

```bash
curl http://localhost:8080/health
curl http://localhost:8080/ready
```

Note: run standalone like this, the Aggregator has no real BMKG/PVMBG to poll — use the repository root's `docker compose up -d --build` instead to bring up the whole system together (see the [root README](../README.md)).

### 3. Add the demo consumer live, without restarting anything

```bash
docker compose --profile demo up -d --build consumer-3
```

`consumer-1` and `consumer-2` are untouched — confirm with `docker inspect <container> --format '{{.State.StartedAt}}'` before and after.

### 4. Stop or restart a single service live

```bash
docker compose stop consumer-2      # everything else keeps running; RabbitMQ queues its backlog
docker compose start consumer-2     # picks the backlog back up
docker compose up -d --build auth   # rebuild and restart just one service
```

### 5. Shut down

```bash
docker compose down       # keep PostgreSQL/RabbitMQ/consumer SQLite data
docker compose down -v    # wipe it
```

### Tests

```bash
go build ./...
go vet ./...
go test ./...
golangci-lint run ./...   # v2.14.0, matches CI
```

## Endpoints

Platform-wide conventions apply to every service below (see [Shared platform behavior](#shared-platform-behavior) at the end): `GET /health` always answers `200 {"service":"<name>","status":"ok"}` with no dependency checks, every standard platform response carries `X-Correlation-ID`, and shared middleware can return 429/500 on any standard route. The four Audit endpoints intentionally do not use correlation IDs and return their exact two-field error envelope.

### Client-Facing API (public, `:8080`)

| Method and path | Auth | Purpose |
| --- | --- | --- |
| `POST /auth/login` | none | Exchange `client_id`/`client_secret` for a token pair |
| `POST /auth/refresh` | none | Rotate a refresh token for a new pair |
| `GET /v1/hazards` | Bearer token | List hazards, filtered and paginated |
| `GET /v1/hazards/{hazard_id}` | Bearer token | One hazard |
| `GET /v1/ops/failed-messages` | Bearer token, `ops` role | Failed messages with their payloads |
| `POST /v1/ops/failed-messages/replay` | Bearer token, `ops` role | Process parked records again |
| `POST /v1/field-reports` | Bearer token, `field` role | Submit a field report; `202` once durably queued |
| `GET /v1/situation-summary` | Bearer token | Latest AI situation summary, or `503` |
| `GET /v1/analytics` | Bearer token | Aggregate counts for the dashboard, marked `partial` when a storage unit is unreachable |
| `GET /health` / `GET /ready` | none | Liveness / dependency readiness |

**`POST /auth/login`** — body `{"client_id":"...","client_secret":"..."}`, both required non-empty. Forwards to Auth's `/internal/auth/login`. Success `200`: `{"access_token","token_type":"Bearer","expires_in","refresh_token","refresh_expires_in"}`, with `Cache-Control: no-store`. Request body capped at 16 KiB; unknown or trailing JSON fields are rejected.

**`POST /auth/refresh`** — body `{"refresh_token":"..."}`, required non-empty. Same shape and forwarding pattern, to `/internal/auth/refresh`.

**`GET /v1/hazards`** — query parameters, **only** these keys are allowed (any unknown, duplicate, or empty-value key → `400 invalid_query`):

| Param | Rule |
| --- | --- |
| `source` | `BMKG`, `PVMBG`, `SATELIT`, or `SENSOR_PANTAI` |
| `hazard_type` | `SEISMIC`, `VOLCANIC`, `WILDFIRE`, or `COASTAL` |
| `severity` | `NORMAL`, `WASPADA`, `SIAGA`, or `AWAS` |
| `occurred_from`, `occurred_to` | RFC3339Nano; `from` must not be after `to` |
| `limit` | integer, 1–500 |
| `cursor` | opaque string, ≤2048 chars, `^[A-Za-z0-9_-]+$` — not decoded by the API, passed straight to the Aggregator |
| `view` | `summary` (default) or `full`; stripped before forwarding upstream |

Only `BMKG/SEISMIC`, `PVMBG/VOLCANIC`, `SATELIT/WILDFIRE`, and `SENSOR_PANTAI/COASTAL` are valid pairs; every cross-pair returns `400`. There is no `TSUNAMI` enum. `role=media` requesting `view=full` → `403 forbidden`, on both list and detail.

**`GET /v1/hazards/{hazard_id}`** — only `view` is accepted (same enum/role rules as list). `hazard_id` empty or >256 chars → `400`. Not-found and most upstream errors are passed through verbatim from the Aggregator (see below); if the Aggregator's response body isn't valid JSON, the API answers `503 dependency_unavailable` instead of passing a broken body through.

**`GET /v1/ops/failed-messages`** and **`POST /v1/ops/failed-messages/replay`** — only a token with `role=ops` passes; `media` and `field` get `403 forbidden` before anything reaches the Aggregator. The list accepts `source`, `limit` (1–20, default 20) and `cursor`, and forwards to the Aggregator's `GET /internal/dlq/messages`. Each item carries the message metadata plus `payload_base64`, `payload_bytes` and `payload_truncated`.

Replay takes a JSON body, `{}` or `{"source":"PVMBG"}`, and forwards to `POST /internal/dlq/replay`. Parked records are re-ingested in `source_ref_id` order with their original input identity, so a record is applied at most once. A call stops starting new rows after 3 seconds. When the response has `"remaining": true`, send again with `{"continue": "<continue value>"}` to finish the same pass. The response counts `attempted`, `recovered`, `still_parked` and `skipped`. Aggregator errors are not passed through on these routes: a rejected request is `400`, anything else is `503 dependency_unavailable`.

```bash
curl -s -H "Authorization: Bearer $OPS_TOKEN" "$API/v1/ops/failed-messages?source=PVMBG" | jq '.items[] | {message_id, reason, status, attempts}'
curl -s -X POST -H "Authorization: Bearer $OPS_TOKEN" -H 'Content-Type: application/json' -d '{"source":"PVMBG"}' "$API/v1/ops/failed-messages/replay"
```

**`POST /v1/field-reports`** — only `role=field`; `media` and `ops` get `403 forbidden`. Body, with unknown or trailing fields rejected:

```json
{"report_id":"fr-2026-11-21-0001","reported_at":"2026-11-21T03:00:00Z","latitude":-2.51,"longitude":113.80,
 "hazard_type":"WILDFIRE","description":"Api terlihat di sisi timur desa","related_hazard_id":null}
```

`report_id` is 1–128 of `[A-Za-z0-9._-]` and is the idempotency key, scoped to the sending client. `reported_at` is RFC 3339 and not in the future. `hazard_type` is `SEISMIC`, `VOLCANIC`, `WILDFIRE`, `COASTAL` or `OTHER`. `description` is 1–2000 characters, and `related_hazard_id` is optional. The API publishes the report as a persistent message to the `field.reports` quorum queue and answers `202 {"report_id","accepted_at"}` only after RabbitMQ confirms it. If the broker is unreachable or does not confirm within 2 seconds, the answer is `503 intake_unavailable` with `Retry-After`, and nothing is promised; retry with the same `report_id`. A retry of a report that was in fact queued is stored once (see [Field intake](#field-intake-internal-8088)).

**`GET /v1/situation-summary`** — every role. Forwards the latest summary from `ai-summarizer` unchanged: `{"summary","provider","model","ai_generated","generated_at","age_seconds","event_count","window_minutes","fault","last_attempts"}`. `provider` is `anthropic`, `openrouter`, `gemini`, or `template`. The API waits at most 2 seconds. A missing key, no summary yet, an unreachable summarizer, or an invalid body all become `503 dependency_unavailable`, so the summary can never hold up an API worker.

**`GET /v1/analytics`** — every role, since the answer holds only counts. Accepts exactly `from`, `to` and optional `bucket` (`hour`, `day`, `week`); anything else is `400 invalid_query` before the Aggregator is called. Forwards to the Aggregator's `GET /internal/analytics` with the internal key, which stays on the server. A `200`, a `400` (for example a range over 366 days) or a `429` (one analytics query per Aggregator instance at a time) is passed through; anything else becomes `503 dependency_unavailable`. The body has `total`, `series` (`{start,category,count}` for `EARTHQUAKE`, `TSUNAMI`, `VOLCANIC`, `WILDFIRE`, `COASTAL`), `severity`, `areas`, `partial`, and `unavailable_units`. When `partial` is `true`, every number covers only the reachable units, and the dashboard marks every card.

**Response shapes** — `view=full` passes the Aggregator's record through after re-validating it:
```json
{"items": [{"hazard_id":"...","source":"BMKG","source_ref_id":"...","hazard_type":"SEISMIC","severity":"AWAS","area_name":"...","latitude":0.0,"longitude":0.0,"occurred_at":"...","ingested_at":"...","attributes":{}}], "next_cursor": "opaque-or-null", "sources": {}, "as_of": "..."}
```
`view=summary` (default) returns the same envelope shape but each item is the **7-field allowlist**: `hazard_id, source, hazard_type, severity, area_name, occurred_at, ingested_at` — no `source_ref_id`, no coordinates, no `attributes`. A new field added to the full record never leaks into Summary automatically. Detail uses `{"item": ..., "sources": {}, "as_of": "..."}` instead of `items`.

**Error codes** — body `{"error":{"code":"...","message":"...","correlation_id":"..."}}` unless noted:

| Code | Status | Condition |
| --- | --- | --- |
| `invalid_request` | 400 | Login/refresh body not valid JSON or missing a required field |
| — | 415 | `Content-Type` isn't exactly `application/json` |
| `invalid_query` | 400 | Any `/v1/hazards` param validation failure above |
| `missing_token` | 401 | No `Authorization` header |
| `invalid_token` | 401 | Malformed header, or the token is inactive per Auth |
| `invalid_credentials` | 401 | Login: wrong client id/secret |
| `invalid_refresh_token` | 401 | Refresh: invalid/expired refresh token |
| `forbidden` | 403 | `role=media` + `view=full`, or a non-`ops` role on `/v1/ops/*` |
| `dependency_unavailable` | 503 | Auth or Aggregator unreachable, or returned something the API can't trust (invalid JSON, inconsistent shape) |
| passthrough | 400/404/429/503 | The Aggregator itself returned one of these with a valid JSON body — forwarded verbatim, unchanged |

**`GET /ready`** checks the Aggregator's `/ready` and Auth's `/health` concurrently (Auth has no `/ready`); either failing → `503 "API dependency is unavailable"`. The Aggregator checks PostgreSQL for readiness, not RabbitMQ.

### Auth (internal, `:8084`)

Never reachable from the host. All three routes require `X-BNPB-Auth-Key`, matching `BNPB_AUTH_INTERNAL_KEY`.

| Method and path | Purpose |
| --- | --- |
| `POST /internal/auth/login` | Issue a token pair for one of the three identities |
| `POST /internal/auth/refresh` | Rotate both tokens, revoking the old access token |
| `POST /internal/auth/introspect` | Resolve an access token to its identity/role/expiry |

- Tokens are opaque random 256-bit values, not JWTs. Auth stores only their SHA-256 hash, the identity, role, and expiry — never the token itself.
- Access token default TTL 60s (`BNPB_AUTH_ACCESS_TOKEN_TTL`); refresh session has an **absolute** 8h limit (`BNPB_AUTH_REFRESH_SESSION_TTL`) that refreshing never extends.
- Two concurrent refreshes of the same token: exactly one succeeds.
- Restart clears every session — in-memory only, by design.

`POST /internal/auth/login` body: `{"client_id":"media|field-team|internal-ops","client_secret":"..."}`. `POST /internal/auth/refresh` body: `{"refresh_token":"..."}`. Both succeed with the same `TokenResponse` shape the API passes straight through (see above). `POST /internal/auth/introspect` body `{"access_token":"..."}`, responds `{"active":bool,"client_id":"...","role":"media|field|ops","expires_at":"..."}` (fields beyond `active` omitted when inactive).

Error codes: `invalid_request` (400, bad body), `missing_credential` (401, no `X-BNPB-Auth-Key`), `invalid_credential` (403, wrong key), `invalid_credentials` (401, wrong client id/secret on login), `invalid_refresh_token` (401, bad/expired refresh token).

### Aggregator (internal, `:8083`)

In local Compose, the Aggregator is not exposed to the host except through the inspection-only mapping in the root `compose.override.yaml`. The hazard routes use `X-BNPB-Internal-Key`, matching `BNPB_AGGREGATOR_INTERNAL_KEY`. Missing header → `401 missing_credential`; present-but-wrong (including repeated or empty) → `403 invalid_credential`. Hazard responses set `Cache-Control: no-store`.

| Method and path | Purpose |
| --- | --- |
| `GET /internal/hazards` | List canonical hazards |
| `GET /internal/hazards/{hazard_id}` | One canonical hazard |
| `GET /health` | Process liveness |
| `GET /readyz` | Data-only Kubernetes readiness probe |
| `GET /ready` | Readiness diagnostics, including poll-owner state |

**`GET /internal/hazards`** — same allowed query keys as the public API's list endpoint (`source, hazard_type, severity, occurred_from, occurred_to, limit, cursor`; no `view` here — that's an API-layer concept). `limit` defaults to `BNPB_LIST_DEFAULT_LIMIT` (100) when omitted, capped by `BNPB_LIST_MAX_LIMIT` (500, 1–500 range). Sort order is `occurred_at DESC, hazard_id DESC` (locale-independent collation).

`cursor` here is **not** opaque to the Aggregator — it's a base64url (unpadded, canonical) encoding of a JSON object `{v, occurred_at, hazard_id, filters}`: `v` must be `1`, `occurred_at` must be a valid instant, `hazard_id` non-empty, and `filters` must match the *current* request's filters exactly byte-for-byte — a cursor is bound to the filter set it was issued under; reusing it with different filters is `400 invalid_query`. This is real keyset pagination, not an offset: `WHERE (occurred_at, hazard_id) < (cursor.occurred_at, cursor.hazard_id)`.

**`GET /internal/hazards/{hazard_id}`** — no query string allowed at all (even a bare `?` is `400 invalid_query`). Not found → `404 not_found`. Store error → `503 store_unavailable`.


M2 Aggregator-owned routes (source-level implementation; dedicated credentials are distinct):

| Method and path | Credential | Behavior |
| --- | --- | --- |
| `GET /internal/audit/events` | Audit Bearer | Logical ascending events; inclusive `since`; default 100/max 1000; partial cursor does not claim incomplete progress. |
| `GET /internal/audit/checksum` | Audit Bearer | Per-source logical event count and byte-sorted SHA-256 of LF-joined `source_ref_id` values (no final LF; duplicate values remain). |
| `GET /internal/audit/location` | Audit Bearer | Probes configured physical copies and live PostgreSQL identity/role; missing evidence is partial. |
| `GET /internal/audit/unprocessed` | Audit Bearer | Bounded metadata-only Member 3 proxy; absent/unavailable upstream is 503. |
| `POST /internal/catalog/seismic` | Catalog Bearer | At most 1000 records/4 MiB; owner-routed durable ingestion with original-byte retry identity; internal store-budget chunking and no poll-state changes. Retained input → 409; uncertain outcome → 503. |
| `GET /internal/storage/measurements` | Catalog Bearer | Parallel per-pool PostgreSQL measurements with verified logical identity: exact observed outbox revision count, sampled/approximate insert/update stats, and relation bytes (not node/WAL totals). Missing or unavailable units are explicit. |
| `GET /internal/analytics` | Internal key | Bounded counts by time/category, severity and area across logical shards, with explicit partial results. |

All four Audit endpoints return UTC timestamps where applicable and exact error bodies `{"error":{"code":"…","message":"…"}}`, with no `correlation_id`. Unspecified Audit event source means all sources. `BNPB_AUDIT_CHECKSUM_TIMEOUT` defaults to 5s and accepts 1ms–1m; it is separate from the unchanged public read timeout default of 200ms. Schema migration 010 adds four C-order Audit indexes for event traversal and checksum sorting, trading additional index space and write work for those reads. Host-fence protocol statuses are 404 for a closed grant, 410 for an expired grant, and bounded 503 for an uncertain outcome; these status meanings do not establish host service availability.

These routes are not part of the existing local Compose API surface unless all required settings and backing dependencies are supplied.
**HazardEvent** (the canonical record, 11 fields total including `attributes`):
```json
{
  "hazard_id": "...", "source": "BMKG", "source_ref_id": "...",
  "hazard_type": "SEISMIC", "severity": "AWAS", "area_name": "...",
  "latitude": 0.0, "longitude": 0.0,
  "occurred_at": "2026-...Z", "ingested_at": "2026-...Z",
  "attributes": {}
}
```

**Error codes:**

| Code | Status | Condition |
| --- | --- | --- |
| `invalid_query` | 400 | Param validation failure (list), or any query string at all (detail) |
| `missing_credential` | 401 | No `X-BNPB-Internal-Key` |
| `invalid_credential` | 403 | Wrong, empty, or repeated key |
| `not_found` | 404 | Detail: no such `hazard_id` |
| `source_unavailable` | 503 | List only — *every* source selected by the current filters is unavailable **and** has no cached data at all. A single stale or cached-but-fresher-than-nothing source still returns `200`. Detail never returns this code — a stale record there is `200`, a truly missing one is `404`. |
| `store_unavailable` | 503 | PostgreSQL error |
| — | 429 | The Aggregator's own read-path semaphore (`BNPB_READ_MAX_IN_FLIGHT`, default 4) is full — separate from the platform-wide `BNPB_MAX_IN_FLIGHT` limiter |

**`GET /readyz`** succeeds when at least one logical primary passes the PostgreSQL identity, role, and schema checks, regardless of poll ownership. No valid unit → 503. It does not wait on poll authority, upstream services, or RabbitMQ. **`GET /ready`** adds per-shard and poll-owner diagnostics; the Kubernetes readiness probe uses `/readyz`, not `/ready`.

M2-only optional configuration is kept commented out in `.env.example`; configure it only in an Aggregator runtime with matching credentials and backing dependencies:

| Setting | Purpose |
| --- | --- |
| `BNPB_CANONICAL_DATABASE_URL`, `BNPB_SHARD_0_DATABASE_URL` … `_2_DATABASE_URL` | Privileged migration/authority DSNs. These are not runtime reader-only URLs. |
| `BNPB_CANONICAL_WRITER_DATABASE_URL`, `BNPB_SHARD_0_WRITER_DATABASE_URL` … `_2_WRITER_DATABASE_URL` | Restricted runtime read/write URLs to the corresponding migration DSN targets, with distinct credentials; routed mode supplies all shard URLs. |
| `BNPB_AUDIT_TOKEN` | Dedicated Audit Bearer secret. |
| `BNPB_AUDIT_CHECKSUM_TIMEOUT` | Audit checksum deadline and starting budget for Analytics; default `5s`, range `1ms`–`1m`. Analytics clamps its budget below `BNPB_WRITE_TIMEOUT`. Public read timeout remains `BNPB_DB_READ_TIMEOUT` (`200ms` by default). |
| `BNPB_AUTHORITY_OWNER_ENDPOINT` | Optional private owner origin discovered for routed Catalog requests; when Catalog is configured, owner discovery is required. A follower forwards original bytes transparently at most once. |
| `BNPB_UNPROCESSED_API_URL`, `BNPB_UNPROCESSED_API_TOKEN` | Optional metadata-only external owner API; both required together. |
| `BNPB_STORAGE_COPY_DATABASE_URLS` | Bounded JSON list of configured physical copy DSNs; never infer replicas from placement metadata. |
| `BNPB_CATALOG_TOKEN` | Dedicated catalog/measurement Bearer secret. |
| `BNPB_AUTHORITY_KUBERNETES_SERVER_URL`, `BNPB_AUTHORITY_KUBERNETES_NAMESPACE`, `BNPB_AUTHORITY_KUBERNETES_CONFIGMAP`, `BNPB_AUTHORITY_KUBERNETES_TOKEN_FILE`, `BNPB_AUTHORITY_KUBERNETES_CA_FILE` | ConfigMap CAS authority API connection. |
| `BNPB_AUTHORITY_HOST_BASE_URL`, `BNPB_AUTHORITY_HOST_TOKEN_FILE`, `BNPB_AUTHORITY_HOST_CA_FILE`, `BNPB_AUTHORITY_DOMAINS` | Host-fence API, credential-file paths, and allowed domain set. |
| `BNPB_K8S_NODE_NAME`, `BNPB_AUTHORITY_NODE_DOMAINS` | Kubernetes owner identity: Downward API `spec.nodeName` and ConfigMap JSON object mapping node names to dedicated fence domain UUIDs in the allowlist. |
| `BNPB_AUTHORITY_OWNER_DOMAIN_UUID` | Literal owner domain UUID for non-Kubernetes use only; must be in the allowed domain set. Do not set it together with node-based identity. |
| `BNPB_AUTHORITY_RENEWAL_INTERVAL` | Authority renewal period; default `5s`, positive and below the `10s` fencing lead. Runtime owner retries are capped at 5s; activation attempts are bounded at 20s. |

Authority fields/domains are all-or-none. Host-native configuration must bind every allowed owner domain UUID to its actual PostgreSQL guard target(s) using a bounded resource file and privileged guard-only credentials; physical mappings, credential values, and native Secret/config paths have not been supplied. Missing/incomplete/mismatched bindings fail closed. The host `init` and `serve` source commands accept `--resources-file` (and the other flags shown in source), but native build/run/fencing proof is missing; those flag names are not an installation procedure. Graceful owner shutdown cancels and joins workers, then closes guards; takeover fences the old owner domain.

#### Failed messages

The Aggregator keeps every source message it could not use in its `failed_messages` table, with the original payload (up to 1 MiB; `payload_truncated` marks a cut). The same payload seen again raises `attempts` instead of adding a row. There are two kinds:

| Kind | Example | Status | What happens next |
| --- | --- | --- | --- |
| `response` | A truncated, deeply nested, non-UTF-8, or oversized PVMBG body | `retrying`, retryable | The next complete poll of that endpoint fetches the same data again and marks it `resolved`. No Ops action is needed. |
| `record` | A PVMBG report from schema version 3–5 that the strict mapping rejects | `parked`, not retryable | Waits for Ops. Ops sets a `BNPB_PVMBG_*` adaptation value, restarts the Aggregator, and replays it. A recovered record becomes `resolved`. |

A broken source never stops polling or crashes the Aggregator, and other sources keep polling meanwhile. The API exposes the list and replay to BNPB Internal Ops only (see [Client-Facing API](#client-facing-api-public-8080)). The gauge `bnpb_failed_messages{source,status}` on `/metrics` counts rows for each source and status.

#### One poller per source

With a single canonical store (no `BNPB_SHARD_*` URLs) and `BNPB_AGGREGATOR_LEASE_NAME` set, only the replica holding that Kubernetes Lease polls BMKG, PVMBG and Sensor Pantai. Every replica still serves reads and publishes its outbox. The lease lasts 15 seconds and is renewed every 5. A holder that cannot renew for 10 seconds stops polling before anyone else may take over. The pod name (`BNPB_AGGREGATOR_IDENTITY`) identifies each replica.

Clocks do not decide who may store poll results. Each takeover raises the Lease's `leaseTransitions` counter, and the new holder first records that number and the Lease UID in the one-row `poll_fence` table. Every poll write (records, checkpoint, poll state, the Sensor Pantai cursor) then checks, in its own transaction, that the row still names its term. A replica whose clock ran ahead may take the Lease early, and a frozen replica may wake up still believing it holds it, but neither can store anything once a newer term has claimed the row. Such a write fails with poll category `lease_lost`. The takeover claim waits for any in-flight write of the old term, so no old write commits after it. `/ready` reports `poll_owner` as `leader` or `standby`, `agency_poll` log lines carry `lease_holder` and `lease_generation`, and `bnpb_aggregator_poll_leading` is 1 on the replica that polls.

With the lease name empty (Compose), every replica polls as in Milestone 1. Sharded stores refuse the setting and use the poll authority described above instead.

### Consumers (internal, `:8085`/`:8086`/`:8087`)

Each consumer exposes only two routes, both unauthenticated (internal network only):

| Consumer | `GET /stats` response |
| --- | --- |
| consumer-1 (dashboard) | `{"hazards_tracked": <int>, "messages_seen": <int>}` |
| consumer-2 (notifier) | `{"alerts_sent": <int>, "messages_seen": <int>}` |
| consumer-3 (portal feed) | `{"feed_entries": <int>, "messages_seen": <int>}` |

`GET /health` behaves the same as every other service. There is no write endpoint anywhere — consumers are driven entirely by the broker, never by direct HTTP calls.

### AI summarizer (internal, `:8090`)

`ai-summarizer` reads its own `hazard.events.ai-summarizer` quorum queue into SQLite (`BNPB_AI_SUMMARIZER_DB_PATH`), one row per hazard at its newest revision, and drops rows older than the window. An unreadable message is acknowledged and skipped, since a summary is best effort. Once a minute it writes a summary when the events changed, or when the last one came from the template; the root README's *Bonus: AI situation summary* section covers the digest, the provider chain, and the reply checks.

| Route | Purpose |
| --- | --- |
| `GET /internal/situation-summary` | Latest summary and the last provider attempts |
| `POST /internal/summarize` | Write a summary now |
| `POST /internal/fault` | Body `{"mode":"slow"}`, `"wrong"`, `"down"`, or `""`: make the first provider misbehave |

All three need `X-BNPB-Internal-Key` set to `BNPB_AI_SUMMARIZER_INTERNAL_KEY`. `/metrics` has `bnpb_summary_requests_total{provider,outcome}` and `bnpb_summary_age_seconds`.

### Field intake (internal, `:8088`)

`field-intake` consumes the `field.reports` quorum queue and stores each report in its own SQLite (`BNPB_FIELD_INTAKE_DB_PATH`). It acknowledges a delivery only after the SQLite commit, so a crash before the ack makes the broker redeliver it. The primary key `(client_id, report_id)` turns that redelivery, or a client retry, into a counted duplicate instead of a second report.

| Case | What is stored |
| --- | --- |
| First copy of a report | One row in `field_reports` |
| Same report again (redelivery or client retry; `accepted_at` may differ) | Nothing new; `deliveries` on the row increases |
| Same `(client_id, report_id)` with different content | The first copy stays; the new one is kept in `report_conflicts` |
| Unreadable or oversized message | A row in `rejected_reports`, then acked so it cannot block the queue |

`GET /internal/field-reports?after=<seq>&limit=<1-1000>` needs `X-BNPB-Internal-Key` set to `BNPB_FIELD_INTAKE_INTERNAL_KEY`. It returns `{"items":[{seq, client_id, report_id, message_id, hazard_type, reported_at, accepted_at, stored_at, deliveries}], "next_after", "stored", "conflicts", "rejected"}`, for comparing stored reports with the 202 answers a client received. The counter `bnpb_field_reports_total{outcome}` on `/metrics` counts settled deliveries by outcome.

### Satellite feed (internal, `:8089`)

The satellite service reads the course's Feed Satelit, an SSE stream at `GET /v1/stream?from=<offset>`. It turns hotspot detections into fires and delivers fire changes to the Aggregator, which stores them as `SATELIT`/`WILDFIRE` hazards.

**One reader at a time.** On Kubernetes two replicas run. Only the holder of the Lease `BNPB_SATELLITE_LEASE_NAME` (`satellite-feed`) connects to the feed; the other waits as a standby. The lease lasts 15 seconds and is renewed every 5. A holder that cannot renew for 10 seconds stops reading before anyone else can take over. In Compose the lease name is empty and the single instance always reads.

**Checkpoint and resume.** Each replica keeps its own SQLite file (`BNPB_SATELLITE_DB_PATH`) with three parts:

- an append-only log of feed events
- `next_offset`, the checkpoint
- an outbox of fire changes

Every event is logged and applied in the same transaction as the checkpoint. After a crash or reconnect the service reconnects with `from=next_offset`, so nothing is skipped or applied twice. On startup it rebuilds its fire state by replaying the log. A standby that has never held the lease starts from offset 0 and catches up on its own. If the feed sends an offset higher than expected, the service reconnects instead of skipping ahead, and logs `feed_disconnected` with category `offset_gap`.

**Clustering rules.** Two detections belong to the same fire when they are at most 1 km and 360 minutes apart, directly or through other detections.

- Low-confidence (`l`) detections and retracted detections are not clustered.
- A single detection is held back until a second one joins it.
- `update` and `retract` events change an existing detection and can split or merge fires.
- Data more than 360 minutes older than the newest detection is late. It is counted but not applied, so the published result stays the same as a replay of the log.
- Severity is `AWAS` with 20 or more detections or a total FRP of at least 100, `SIAGA` with 6 or more detections or at least 30, and otherwise `WASPADA`.
- A fire's ID is derived from its first detection, so a replay names every fire the same way.

**Delivery.** Each fire change is written to the outbox in the same transaction as the event that caused it. A sender posts outbox rows to the Aggregator's `POST /internal/satellite/fires` with `Bearer BNPB_SATELLITE_INGEST_TOKEN`, and marks them sent only after a `200`. The Aggregator deduplicates by input identity, so a resend after a crash changes nothing. With an empty ingest token the service keeps reading but leaves fires in the outbox.

| Route | Purpose |
| --- | --- |
| `GET /health` | Process liveness |
| `GET /ready` | Always `200`, with diagnostics: `leading`, `connected`, `next_offset`, event counts, fire statistics (`active`, `singletons`, `late`, `late_updates`, `orphans`, `duplicates`, `watermark`), and delivery `backlog`/`sent`/`refused` |
| `GET /metrics` | `bnpb_satellite_*` series: `leading`, `connected`, `next_offset`, `feed_latest_offset`, `feed_lag_events`, `feed_events_total{kind}`, `engine_events_total{outcome}`, `active_fires`, `singleton_fires`, `watermark_timestamp_seconds`, `outbox_backlog`, `delivered_total{result}`, and the `store_latency_seconds` histogram (from an event arriving to the Aggregator storing its fire change) |

Only the leader reports `feed_lag_events`: the feed's latest offset from `GET /v1/session`, plus one, minus `next_offset`. A reading older than one minute is dropped instead of reported. The Grafana dashboard **BNPB Streams** shows these series.

**Checking the result.** `bnpb-satellite -verify [database path]` opens the database read-only, so it can run beside the live service. It compares three sets of fires:

- the newest fire published in the outbox
- a fresh replay of the event log
- a one-shot batch clustering of the same detections

It prints JSON with `published_equals_rebuild`, `published_equals_batch`, the fires found in only one of them, every fire's detection keys, and `undelivered_changes`. It exits `1` on any mismatch.

```text
docker compose exec satellite /bnpb-satellite -verify
kubectl -n sistem-informasi-kebencanaan-bnpb exec satellite-0 -- /bnpb-satellite -verify
```

### Shared platform behavior

- **Correlation ID** — `X-Correlation-ID` request header, echoed on the response; generated if missing or invalid (1–64 chars, `A-Za-z0-9._-`), and attached to every log line for that request across every service.
- **Concurrency limiting (429)** — shared routes may use the generic `{"error":"too many requests"}` platform rejection. Audit routes preserve their exact `{"error":{"code":"…","message":"…"}}` envelope, with no correlation field in the body.
- **Panic recovery (500)** — any unhandled panic anywhere becomes a clean `500 {"error":"internal server error"}`, never a crash or a leaked stack trace to the client.
- **No business-logic sharing** — every `internal/<service>` package only ever imports `internal/platform`, enforced by CI lint (`depguard`). Each consumer's AMQP/SQLite code is its own copy by design (KI.1), not a shared library.

## M2 Member 1 Steps 1–9a — source and local-evidence boundary

The implementation now includes the fixed three-logical-shard routing and durable per-input receipts (Step 5); ConfigMap CAS owner/epoch plus per-resource PostgreSQL generation guards and independent host BOOTTIME fencing controls (Step 6); owner-scoped durable outbox retries and persistent consumer dedupe (Step 7); cross-shard public partial reads with logical v2 cursors (Step 8); four dedicated-token Audit routes and physical-copy probes (Step 9); and a dedicated catalog ingest/measurement API plus offline loader/comparison CLIs (Step 9a). These source changes do not establish deployed Kubernetes, distributed PostgreSQL HA, external host-fence availability, or fault acceptance.

Step 5 stores each input identity and ordinal with per-shard durable outcomes, committing canonical/source/outbox state atomically within one shard. Replays are idempotent, conflicting receipt bytes are rejected, and a central checkpoint advances only across a contiguous prefix of durable outcomes. Step 6 serializes protected SQL mutations with authority guards and holds their locks through commit. Application epoch and PostgreSQL generation remain distinct; a host BOOTTIME deadline and independently armed host timer—not a guest wall clock or PG clock—provide the frozen-guest fence path. The controls are not evidence that a multi-node HA database or independent hypervisor timer has been deployed.

Step 7 retries the same immutable outbox message identity and bytes after uncertain publication; each consumer's durable dedupe/effect state survives restart. Dedupe is intentionally retained indefinitely because pruning without a safe replay horizon could permit old duplicate effects; this trades storage capacity for correctness. Step 8 performs concurrent bounded live shard reads under one deadline; these are not a global snapshot. Explicit partial/unavailable metadata accompanies reachable candidates; incomplete pages retry the last complete boundary (or opaque v2 start marker) and incomplete details do not become false 404s.

Step 9 exposes `GET /internal/audit/events`, `/internal/audit/checksum`, `/internal/audit/location`, and `/internal/audit/unprocessed`, each guarded by a dedicated Audit Bearer token. Checksum uses byte-sorted `source_ref_id` values separated by LF with no final LF. Location probes actual configured copies and live `pg_is_in_recovery()` plus PostgreSQL `bnpb.node_name` / `bnpb.unit_name`; placement intent alone is not proof. Unprocessed metadata is fetched over the network from the configured Member 3 API; absent/unreachable/invalid/oversized metadata yields 503. No actual Member 3 API was available.

Step 9a exposes `POST /internal/catalog/seismic` and `GET /internal/storage/measurements`, protected by a separate catalog Bearer token. Owner discovery uses `owner_endpoint`; a follower transparently forwards the original request bytes to the owner at most one hop. Retries preserve the original input identity and bytes. Requests allow up to 1000 records/4 MiB and the owner splits them internally by configured store write budget, without writing poll state or synthesizing source success. Durable retained outcomes return 409; uncertain outcomes return bounded 503. The actual CLI names are `cmd/catalog-load` (flags `-artifact`, `-base-url`, `-token-file`, `-audit-token-file`, `-expected-checksum`, `-resources-manifest`, `-run-output`) and `cmd/placement-compare` (flags `-input`, `-resources`, optional `-burst-area`). Catalog artifacts stream JSONL or JSON arrays; records are limited to 128 KiB and each wire request to 4 MiB, with batches of at most 20 records; there is no arbitrary whole-artifact 256 MiB cap. The comparison reports offline `fixed_hash` and `longitude_band` strategies, not runtime placement metrics. Catalog/reference data and deployed node metadata are unavailable, so no catalog checksum, per-node distribution, Kubernetes readiness, or M2 external acceptance is claimed.
Storage measurements query configured physical pools in parallel and validate each pool's logical store identity. The canonical revision field counts observed outbox rows exactly; PostgreSQL insert/update statistics are sampled and approximate. Relation-size bytes are not total node disk, filesystem, or WAL usage, and do not establish measured distributed skew.
The source broker policy changes are user-authorized but are not applied to the live broker: intended main/dead-letter queue bounds are 10,000/1,000, with no TTL or expiry and `reject-publish` overflow. This remains a source change, not observed broker configuration.

M2 startup settings (writer URLs, Audit/Catalog credentials, optional copy inventory, and Authority endpoint/domain set) are feature-specific and must be supplied consistently; leave optional entries in `.env.example` commented out for local Compose. Do not use sample node names or empty credentials as configured values. The installed `golangci-lint` is 1.64.8, while CI pins 2.14.0; no local lint pass is claimed. This repository has no verified Kubernetes deployment command; the Compose workflow remains for local development only.

Placement starts empty unless configured through `BNPB_SHARD_PLACEMENT`, a bounded startup-only JSON environment value from an Aggregator-only ConfigMap. `OpenStore` atomically migrates and bootstraps it before readiness: first initialization requires RF2-ready metadata; identical configuration/reordering/restarts are no-ops; stale different content or future revisions fail startup. Omission preserves storage. Structured `placement_loaded` logs expose the actual revision/count for the next CAS update. No new admin endpoint, reader privilege, watcher or dependency is added. Copy identity/no-forgetting/no-full-node guards remain enforced; metadata does not provision physical replicas. See the [bootstrap and selected fencing contract](contracts/v1/README.md#placement-bootstrap). SATELIT/SENSOR_PANTAI freshness remains explicitly unconfigured until their adapters exist.

Local verification from `bnpb/`:

```text
go test ./internal/aggregator ./internal/api ./internal/consumer-1 ./internal/consumer-2 ./internal/consumer-3
```

From application root: `python -B -m unittest discover -s testing -p test_canonical_contracts.py`.

PostgreSQL integration fixtures are opt-in: start a **new disposable loopback PostgreSQL 18** server on a free port, create database `bnpb_m2_step4_test`, set `BNPB_M2_LOCAL_TEST_DATABASE_URL` to that server/database with an explicit port, then run `go test -tags=integration -run '^Test(M2|Placement)' ./internal/aggregator` from `bnpb/`. Each test creates/removes its own random schema; this environment variable must never point to a service/canonical database. These fixtures do not replace the separate `postgres-test/bnpb_test` M1 Compose scenarios or demonstrate distributed HA/fault acceptance.

### Distributed evidence handoff

- **Primary routes:** For the control store and each logical shard, A2 must provide a stable route gated by the Patroni `/primary` endpoint (PostgreSQL primary **and** leader lock). Fence the old primary and drain old pooled sessions, verify the replacement has applied through the acknowledged WAL boundary, then expose the new route. Close/rebind the application guard before enabling writes on the new timeline. Use separate Patroni scopes and `pg-role` without changing `role=store`. Verify bounded partial reads recover to complete reads at the same logical cursor.
- **Polling:** Capture upstream receipt logs, not just response-completion logs, and correlate endpoint/request ID to owner and epoch. Distinguish rejected auth, a replayed durable receipt (no fetch), and a request whose response never became durable. Measure from owner death to the replacement's first fetch, including fencing, activation, the elapsed handoff and any receipt replay. Test the ≤60 s bound and the skew/freeze rejection on the approved topology. Configure the fence endpoint on the independent libvirt host that can address all configured guest domains; this does not cover loss of that host.
- **Catalogue:** Run `catalog-load` with the assistant catalogue, supplied checksum, and actual resource manifest. Allow temporary disk space for its private artifact snapshot. Require complete before/after measurements and a matching complete Audit checksum/count with other writes excluded from the measurement window. Join measurements by `(node, unit)` and placement shard; count each logical shard once in logical totals and each physical copy once in physical totals. Verify no node holds all logical records, not merely that each node hosts fewer than three shard IDs.
- **Regional burst:** Use a separate bounded same-region input with new source reference IDs so it exercises writes rather than replay. Measure offered/accepted/completed rate, latency, CPU, disk and network on every node in the same sampling window. Keep resources, replica policy, batch size, and baseline constant; record the baseline and post-burst checksums separately. Use `placement-compare` on identical inputs for the offline hash/longitude comparison only. PostgreSQL tuple counters are sampled and do not count standby WAL replay; use physical node metrics for replica write load.

## Member 1 — repair, analytics, and readiness

- [Repair handoff](contracts/v1/README.md#step-10--penempatan-dan-perbaikan-standby): repair metadata is additive; canonical schema head is 13 with immutable routed receipt bindings. Drain pre-fix routed writers before the coordinated schema/binary cutover. The copy source must be the current ready primary in placement and match a configured allowed member. Once the target is synchronous, capture a fixed WAL flush barrier on the source and wait for the target to replay through it. The standby remains `nofailover`; fencing the old primary, promotion and route cutover, and demonstrating tolerance of a second loss remain external to the repair controller.
- [Analytics handoff](contracts/v1/README.md#step-11--handoff-analitik-untuk-anggota-2): `GET /internal/analytics` is an internal-key route for the Member 2 backend. Keep the key out of the browser. Range endpoints accept up to nine fractional-second digits and must be representable as RFC 3339 in UTC. Output is bounded, and the query budget is clamped below the HTTP write timeout. Treat `partial` as incomplete even if a chart value is zero; reconcile all dimensions with Audit events on the same stable population.
- [Aggregator deployment contribution](../deploy/aggregator.yaml): replace, do not duplicate, the Member 2 `aggregator` Deployment/Service. It follows `origin/feat/k8s-platform@3d08ba8`: `app.kubernetes.io/name=aggregator`, image key `aat/bnpb-aggregator`, `bnpb-common` and `aggregator-config`, and NodePort `30083` → `8083`. Kubernetes 1.30+ and at least three eligible hostname domains are required. Use the Aggregator-only `aggregator-secrets` Secret and `aggregator` ServiceAccount with narrow RBAC and the Member 2 image-pull binding; do not import the shared all-service Secret wholesale. The first routed shard DSN (`BNPB_SHARD_0_DATABASE_URL`) is a required Secret key reference, not optional. Supply `host-fence-token` and `host-fence-ca.crt` in that Secret. Inject `BNPB_K8S_NODE_NAME` from Downward API `spec.nodeName` and put the `BNPB_AUTHORITY_NODE_DOMAINS` node-to-dedicated-fence-UUID JSON mapping in `aggregator-config`; do not set the non-Kubernetes literal owner UUID in this workload. If configured, bind the catalog owner URL to the pod. Use `/health` for liveness and `/readyz` for data-only Kubernetes readiness; `/ready` provides diagnostics.
- [Store overlay patch](../deploy/store.yaml): apply as a targeted Kustomize strategic-merge patch to desired `apps/v1` StatefulSets with a `postgres` container. For A2's current monolithic resource use `name: postgres`; for per-scope workloads carrying the workload label, select `role=store`. It merges Appendix B memory values without discarding CPU settings. Keep `role=store` for the workload and use a separate `pg-role` via `kubernetes.role_label` for Patroni. For a repair standby, the prebound node-local data claim must mount at the same path as the explicitly configured `postgresql.data_dir` and `PGDATA`.
- A2 still supplies actual configuration and Secret values, scoped RBAC, per-scope Store workloads and pinned Patroni/PostgreSQL image, disk and node-local PV/PVC bindings, post-fence routing, network policy that enforces storage ownership without obstructing Tester Agent, and telemetry integration. These source contributions are not a standalone cluster deployment or evidence of acceptance.
- `/metrics` uses separate admission from the data routes and exposes per-route HTTP request totals and duration histograms, including controlled 429s. Scrape per pod to retain backend identity. Shared Go HTTP calls forward a valid `traceparent` unchanged, including the Catalog follower-to-owner relay; correlation IDs and outbound durations aid log diagnosis. An actual distributed trace still requires A2's exporter/backend and instrumentation at each service on the path.
- [Verification order and evidence](../testing/README.md#urutan-verifikasi-member-1--repair-analitik-dan-readiness) separate the disposable loopback tests from Kubernetes, Tester Agent, and assistant-catalogue acceptance.
