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

Platform-wide conventions apply to every service below (see [Shared platform behavior](#shared-platform-behavior) at the end): `GET /health` always answers `200 {"service":"<name>","status":"ok"}` with no dependency checks, every response carries `X-Correlation-ID`, and a 429/500 from the shared middleware can occur on any route.

### Client-Facing API (public, `:8080`)

| Method and path | Auth | Purpose |
| --- | --- | --- |
| `POST /auth/login` | none | Exchange `client_id`/`client_secret` for a token pair |
| `POST /auth/refresh` | none | Rotate a refresh token for a new pair |
| `GET /v1/hazards` | Bearer token | List hazards, filtered and paginated |
| `GET /v1/hazards/{hazard_id}` | Bearer token | One hazard |
| `GET /health` / `GET /ready` | none | Liveness / dependency readiness |

**`POST /auth/login`** — body `{"client_id":"...","client_secret":"..."}`, both required non-empty. Forwards to Auth's `/internal/auth/login`. Success `200`: `{"access_token","token_type":"Bearer","expires_in","refresh_token","refresh_expires_in"}`, with `Cache-Control: no-store`. Request body capped at 16 KiB; unknown or trailing JSON fields are rejected.

**`POST /auth/refresh`** — body `{"refresh_token":"..."}`, required non-empty. Same shape and forwarding pattern, to `/internal/auth/refresh`.

**`GET /v1/hazards`** — query parameters, **only** these keys are allowed (any unknown, duplicate, or empty-value key → `400 invalid_query`):

| Param | Rule |
| --- | --- |
| `source` | `BMKG` or `PVMBG` |
| `hazard_type` | `SEISMIC` or `VOLCANIC` |
| `severity` | `NORMAL`, `WASPADA`, `SIAGA`, or `AWAS` |
| `occurred_from`, `occurred_to` | RFC3339Nano; `from` must not be after `to` |
| `limit` | integer, 1–500 |
| `cursor` | opaque string, ≤2048 chars, `^[A-Za-z0-9_-]+$` — not decoded by the API, passed straight to the Aggregator |
| `view` | `summary` (default) or `full`; stripped before forwarding upstream |

`source`/`hazard_type` must be consistent (`BMKG`+`VOLCANIC` or `PVMBG`+`SEISMIC` → `400`). `role=media` requesting `view=full` → `403 forbidden`, on both list and detail.

**`GET /v1/hazards/{hazard_id}`** — only `view` is accepted (same enum/role rules as list). `hazard_id` empty or >256 chars → `400`. Not-found and most upstream errors are passed through verbatim from the Aggregator (see below); if the Aggregator's response body isn't valid JSON, the API answers `503 dependency_unavailable` instead of passing a broken body through.

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
| `forbidden` | 403 | `role=media` + `view=full` |
| `dependency_unavailable` | 503 | Auth or Aggregator unreachable, or returned something the API can't trust (invalid JSON, inconsistent shape) |
| passthrough | 400/404/429/503 | The Aggregator itself returned one of these with a valid JSON body — forwarded verbatim, unchanged |

**`GET /ready`** checks the Aggregator's own `/ready` and Auth's `/health` concurrently (not Auth's `/ready` — Auth has none); either failing → `503 "API dependency is unavailable"`. Postgres/RabbitMQ are only checked transitively, through the Aggregator's own `/ready`.

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

Never reachable from the host (except via the root `compose.override.yaml`'s inspection-only port mapping). Every route requires `X-BNPB-Internal-Key`, matching `BNPB_AGGREGATOR_INTERNAL_KEY`. Missing header → `401 missing_credential`; present-but-wrong (including repeated or empty) → `403 invalid_credential`. Every response sets `Cache-Control: no-store`.

| Method and path | Purpose |
| --- | --- |
| `GET /internal/hazards` | List canonical hazards |
| `GET /internal/hazards/{hazard_id}` | One canonical hazard |
| `GET /health` / `GET /ready` | Liveness / Canonical Store readiness |

**`GET /internal/hazards`** — same allowed query keys as the public API's list endpoint (`source, hazard_type, severity, occurred_from, occurred_to, limit, cursor`; no `view` here — that's an API-layer concept). `limit` defaults to `BNPB_LIST_DEFAULT_LIMIT` (100) when omitted, capped by `BNPB_LIST_MAX_LIMIT` (500, 1–500 range). Sort order is `occurred_at DESC, hazard_id DESC` (locale-independent collation).

`cursor` here is **not** opaque to the Aggregator — it's a base64url (unpadded, canonical) encoding of a JSON object `{v, occurred_at, hazard_id, filters}`: `v` must be `1`, `occurred_at` must be a valid instant, `hazard_id` non-empty, and `filters` must match the *current* request's filters exactly byte-for-byte — a cursor is bound to the filter set it was issued under; reusing it with different filters is `400 invalid_query`. This is real keyset pagination, not an offset: `WHERE (occurred_at, hazard_id) < (cursor.occurred_at, cursor.hazard_id)`.

**`GET /internal/hazards/{hazard_id}`** — no query string allowed at all (even a bare `?` is `400 invalid_query`). Not found → `404 not_found`. Store error → `503 store_unavailable`.

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

**`GET /ready`** checks **PostgreSQL only** (schema version matches what the Aggregator expects) — it does not check RabbitMQ connectivity.

### Consumers (internal, `:8085`/`:8086`/`:8087`)

Each consumer exposes only two routes, both unauthenticated (internal network only):

| Consumer | `GET /stats` response |
| --- | --- |
| consumer-1 (dashboard) | `{"hazards_tracked": <int>, "messages_seen": <int>}` |
| consumer-2 (notifier) | `{"alerts_sent": <int>, "messages_seen": <int>}` |
| consumer-3 (portal feed) | `{"feed_entries": <int>, "messages_seen": <int>}` |

`GET /health` behaves the same as every other service. There is no write endpoint anywhere — consumers are driven entirely by the broker, never by direct HTTP calls.

### Shared platform behavior

- **Correlation ID** — `X-Correlation-ID` request header, echoed on the response; generated if missing or invalid (1–64 chars, `A-Za-z0-9._-`), and attached to every log line for that request across every service.
- **Concurrency limiting (429)** — every service's whole mux is wrapped by a semaphore sized `BNPB_MAX_IN_FLIGHT` (default 64). Once full, excess requests get `429 {"error":"too many requests"}` immediately — note this is a **flatter** shape than the API's own `publicError` (no `code`/`correlation_id` keys). `/health` is exempt.
- **Panic recovery (500)** — any unhandled panic anywhere becomes a clean `500 {"error":"internal server error"}`, never a crash or a leaked stack trace to the client.
- **No business-logic sharing** — every `internal/<service>` package only ever imports `internal/platform`, enforced by CI lint (`depguard`). Each consumer's AMQP/SQLite code is its own copy by design (KI.1), not a shared library.
