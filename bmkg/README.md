# Mock BMKG

Mock BMKG is a simulation of Indonesia's meteorology/seismology/geophysics agency (Badan Meteorologi, Klimatologi, dan Geofisika), built in Rust with [Axum](https://github.com/tokio-rs/axum). It serves seismic events and, for the ones that warrant one, tsunami warnings — seeded at start-up, then topped up on its own at a configurable rate while it runs. It is a **passive** service: it never calls anything else, it only answers requests from whoever polls it.

Within TsundereDISTRIBUTED, BMKG is one of two independent agency sources the Aggregator polls (the other is PVMBG). It listens on port `8081`, authenticates callers with a single static shared secret, answers every request with a small deliberate delay (BMKG's defining trait in the spec), and keeps everything in memory — a restart means a fresh seed, by design.

## Table of Contents

- [Technology and Frameworks](#technology-and-frameworks)
- [Program Structure](#program-structure)
- [Program Requirements](#program-requirements)
- [Running the Application](#running-the-application)
- [Endpoints](#endpoints)
- [Logging](#logging)
- [Notes and Limits](#notes-and-limits)

## Technology and Frameworks

| Layer | Choice | Version |
| --- | --- | --- |
| Language | Rust | edition 2024 |
| HTTP framework | Axum | 0.8.9 |
| Async runtime | Tokio | 1.53.1 (full features) |
| Serialization | serde, serde_json | 1.0.229, 1.0.151 |
| Timestamps | chrono | 0.4.45 |
| Env loading | dotenvy | 0.15.7 |
| RNG | rand | 0.10.3 |
| Constant-time comparison | subtle | 2.6.1 |
| Middleware utilities | tower | 0.5.3 |

## Program Structure

```text
bmkg/
├── src/
│   ├── main.rs        # entry point, startup, graceful shutdown
│   ├── config/         # environment variable loading and validation
│   ├── models/         # SeismicEvent, TsunamiWarning, ThreatLevel
│   ├── state/          # in-memory store (Arc<RwLock<...>>)
│   ├── seed/            # deterministic seeding + the live event generator
│   ├── plugins/          # auth, correlation id, response delay, access log, errors
│   ├── routing/         # route handlers and request parsing
│   └── logging/         # structured logging setup
├── Cargo.toml / Cargo.lock
├── Dockerfile
├── compose.yaml
└── .env.example
```

## Program Requirements

Either of the following:

- **Rust** (stable, edition 2024 toolchain) to run it directly with Cargo, or
- **Docker** with **Compose 2.20 or newer** (`docker compose version`) to run it as a container.

A free TCP port `8081` on the host (or remap it, see below).

## Running the Application

### 1. Prepare configuration

```bash
cp .env.example .env
```

Set `BMKG_API_KEY` to a freshly generated value:

```bash
openssl rand -hex 32
```

Everything else in `.env.example` has a usable default. `.env` is git-ignored and must never be committed. Keep this key independent of anything used by PVMBG — the spec requires the two mocks' credentials to come from separate trust domains, and a PVMBG token sent here is just a string that will never match.

| Variable | Default | What it does |
| --- | --- | --- |
| `BMKG_API_KEY` | required | The shared secret sent as `X-BMKG-Key` on every call to the two data routes. |
| `BMKG_PORT` | `8081` | TCP port the HTTP server binds to. |
| `BMKG_RESPONSE_DELAY_MIN_MS` | `50` | Lower bound of the random delay added to every response. Must not exceed the maximum. |
| `BMKG_RESPONSE_DELAY_MAX_MS` | `150` | Upper bound of that delay. Set both to the same value for an exact fixed delay. |
| `BMKG_GENERATOR_INTERVAL_SECONDS` | `10` | Seconds between two newly generated events while the service keeps running. Decimals allowed, at least `0.001`. |
| `BMKG_LOG_LEVEL` | `INFO` | `TRACE`, `DEBUG`, `INFO`, `WARN`, `ERROR` or `OFF`. |
| `BMKG_LOG_FORMAT` | `json` | `json` (one object per line) or `text` (readable lines). Lowercase only. |

BMKG **refuses to start** with a clear message if a required variable is missing or a value is invalid. None of these settings can be changed at runtime — there are no admin routes at all; changing `.env` always needs a restart.

### 2. Run locally with Cargo

```bash
cargo run                          # start the service; loads .env; seeds every region from 2026-01-01 until now
cargo run -- 10                    # same, but seed exactly 10 initial events instead
BMKG_PORT=9091 cargo run           # listen on another port for one run, without editing .env
cargo test                         # run the tests
cargo build --release              # compile the optimized binary (target/release/bmkg)
```

Ready when it prints `BMKG is running on http://0.0.0.0:8081 (...)`. Stop with Ctrl+C — it installs its own SIGINT/SIGTERM handler and shuts down at once (see [Notes and Limits](#notes-and-limits) for why that matters). `cargo run` loads `.env` itself from whichever directory you run it in — run it from this folder. A real environment variable always wins over the same name in `.env`.

### 3. Run with Docker

Run these from this folder (the one with `compose.yaml`):

```bash
docker compose up -d --build         # build the image and start the container on port 8081
docker compose logs -f bmkg          # follow the logs (add --no-log-prefix for clean JSON)
docker compose stop bmkg             # stop it (the container is kept)
docker compose up -d --no-build      # start it again without rebuilding
docker compose up -d --build bmkg    # rebuild and restart only this service
docker compose down                  # stop and remove the container
```

- The image contains only the compiled binary — no `.env` and no secrets baked in. The container reads `.env` at start (`env_file` in `compose.yaml`); a missing `.env` stops compose with an error. After changing `.env`, run `docker compose up -d --force-recreate`.
- Without compose: `docker build -t bmkg .` then `docker run --rm --env-file .env -p 8081:8081 bmkg`. Arguments after the image name go to the service, so `docker run ... bmkg 10` seeds 10 events.
- To use a different host port, change the left side of `"8081:8081"` in `compose.yaml` — inside the container it is always 8081.
- Runs as a non-root user; `docker stop` exits cleanly in under a second (code 0).
- To run this service as part of the full system, a compose file at the repository root includes it with `include: [bmkg/compose.yaml]` — see the [root README](../README.md).

### 4. Verify every scenario

Set up your shell once (from this folder, with the service running):

```bash
set -a; . ./.env; set +a
BASE=http://localhost:8081
timed() { curl -s -o /dev/null -w "%{http_code} in %{time_total}s\n" "$@"; }
```

**Read seismic events and tsunami warnings:**

```bash
curl -s $BASE/seismic-events -H "X-BMKG-Key: $BMKG_API_KEY" | head -c 300; echo
curl -s $BASE/tsunami-warnings -H "X-BMKG-Key: $BMKG_API_KEY" | head -c 300; echo
```

**Filter with `since`:**

```bash
LATEST=$(curl -s $BASE/seismic-events -H "X-BMKG-Key: $BMKG_API_KEY" | python3 -c "import sys,json;print(json.load(sys.stdin)[-1]['occurred_at'])")
curl -s "$BASE/seismic-events?since=$LATEST" -H "X-BMKG-Key: $BMKG_API_KEY"; echo   # expect []
```

**New data over time** (expect one or more events newer than `LAST`, one per `BMKG_GENERATOR_INTERVAL_SECONDS`):

```bash
LAST=$(curl -s $BASE/seismic-events -H "X-BMKG-Key: $BMKG_API_KEY" | python3 -c "import sys,json;print(json.load(sys.stdin)[-1]['occurred_at'])")
sleep 11
curl -s "$BASE/seismic-events?since=$LAST" -H "X-BMKG-Key: $BMKG_API_KEY"; echo
```

**Authentication failures:**

```bash
timed $BASE/seismic-events                                              # 401, no key
timed $BASE/seismic-events -H "X-BMKG-Key: wrong-key"                   # 401, wrong key
timed $BASE/seismic-events -H "X-BMKG-Key: $BMKG_API_KEY"               # 200, the real key
```

**A malformed `since`:**

```bash
curl -s "$BASE/seismic-events?since=not-a-timestamp" -H "X-BMKG-Key: $BMKG_API_KEY"; echo   # 400
```

**The response delay doesn't block other requests** (expect roughly one delay's worth of total time, not 30 added up):

```bash
time (seq 30 | xargs -P 30 -I{} curl -s -o /dev/null $BASE/seismic-events -H "X-BMKG-Key: $BMKG_API_KEY")
```

**Correlation id:**

```bash
curl -s -D - -o /dev/null $BASE/health -H "X-Correlation-ID: demo-123" | grep -i correlation   # echoed back
curl -s -D - -o /dev/null $BASE/health | grep -i correlation                                   # a generated UUID
```

**Stop, rebuild, and restart the container on its own:**

```bash
docker compose stop bmkg             # the service is gone; other services in a bigger compose file are untouched
timed $BASE/health                   # connection refused
docker compose up -d --build bmkg    # rebuild only this image and start it again
```

The data starts again from a fresh seed, because nothing is stored.

## Endpoints

Errors are always JSON: `{"error": "<message>"}`. There is no root `/` route; an unmatched path is a plain 404.

| Method and path | Needs | Purpose |
| --- | --- | --- |
| `GET /health` | nothing | Health check: always `200 OK`. BMKG has no outage mode. |
| `GET /seismic-events?since=<timestamp>` | `X-BMKG-Key` | The seismic events. |
| `GET /tsunami-warnings?since=<timestamp>` | `X-BMKG-Key` | The tsunami warnings, filtered by `issued_at`. |

### Authentication

One static shared secret, checked on every call to the two data routes (`/health` stays public):

```bash
curl http://localhost:8081/seismic-events -H "X-BMKG-Key: <key>"
```

- Compared in constant time, so probing many wrong keys cannot learn anything from how long the check takes.
- A missing key, a wrong key, a key of the wrong length, a key differing only in case, or a PVMBG-style Bearer token sent as the key value are all rejected the same way: `401 {"error":"missing or invalid API key"}`.
- No scopes, so no `403` case: a request either has the right key or it does not.

### `since` semantics

`since` (optional) is an ISO-8601 UTC timestamp such as `2026-05-15T09:00:00Z`. Only records **strictly after** it are returned; without it, every record is returned. A value that does not parse as RFC 3339 is `400 {"error":"'since' must be an ISO-8601 timestamp, ..."}`.

### Status codes

| Code | Meaning here |
| --- | --- |
| `200` | Normal response |
| `400` | `since` is not a valid timestamp |
| `401` | Missing, wrong, or foreign `X-BMKG-Key` |
| `404` | Unknown route |

### Response schemas

**Regions:** `Aceh`, `Nias`, `Mentawai`, `Bengkulu`, `Selat Sunda`, `Yogyakarta`, `Palu`, `Ambon`, `Maluku Utara`, `Papua Barat`. Coordinates are generated within Indonesia's rough bounding box.

**`potential_tsunami`** is set when an event is both strong and shallow (`magnitude >= 6.5` and `depth_km <= 60.0`). Only those events get a matching `TsunamiWarning`, with `related_event_id` pointing back at it.

A seismic event, exactly as served (fields and casing fixed by the spec):

```json
{
  "event_id": "evt-000001",
  "magnitude": 5.1,
  "depth_km": 165.10393692142745,
  "epicenter_lat": -0.18009331455378685,
  "epicenter_lon": 113.67148087861572,
  "region_name": "Nias",
  "occurred_at": "2026-01-01T00:00:00Z",
  "potential_tsunami": false
}
```

A tsunami warning:

```json
{
  "warning_id": "tsw-000001",
  "related_event_id": "evt-000016",
  "threat_level": "Siaga",
  "affected_zones": ["Pesisir Papua Barat", "Pesisir Ambon"],
  "estimated_arrival": "2026-01-02T21:22:00Z",
  "issued_at": "2026-01-02T21:01:00Z"
}
```

`threat_level` is one of `Waspada`, `Siaga`, `Awas`. **`issued_at` is an addition beyond the spec's five listed fields** (`warning_id`, `related_event_id`, `threat_level`, `affected_zones`, `estimated_arrival`) — the spec allows additions as long as nothing already-specified changes, and `since` filtering needs a real "when was this created" timestamp; `estimated_arrival` is a *future prediction*, not a creation time, so it cannot serve that role. `GET /tsunami-warnings?since=` filters by `issued_at`, not `estimated_arrival` — keep this in mind if you build a poller against this service.

**Data generation:** by default one event every 3 hours from 2026-01-01 00:00 UTC until now is seeded at start-up (the spec asks for at least 20; a fresh run typically seeds a couple of thousand). `cargo run -- N` seeds exactly `N` events instead. While running, one new event appears every `BMKG_GENERATOR_INTERVAL_SECONDS` for a random region, timestamped at the real moment it is generated. The seed is deterministic (fixed RNG seed), so the same run always produces the same history, and live generation continues the same id sequence so a live event's id never collides with a seeded one.

## Logging

Logs go to **standard output**, one event per line, via [`tracing`](https://docs.rs/tracing). Read them with the terminal running `cargo run`, or with `docker compose logs -f bmkg` (add `--no-log-prefix` for clean JSON).

- `BMKG_LOG_FORMAT=json` (default) writes one JSON object per line; `text` writes readable console lines.
- `BMKG_LOG_LEVEL` sets the minimum level.

### Correlation id

Every request has a **correlation id**, so one flow can be followed across services:

- Read from the request header **`X-Correlation-ID`**. If missing, a UUID is generated.
- Accepted only if 1 to 64 characters from `A-Za-z0-9._-`. Anything else is **replaced** by a generated UUID and never logged.
- Returned in the `X-Correlation-ID` response header and appears as `correlation_id` on every log line for that request, including a rejected one.

### The access log line

One line per request, once the response is ready (`event` = `http_request`):

```json
{"timestamp":"2026-09-26T14:38:59Z","level":"INFO","message":"GET /seismic-events 200 98ms","event":"http_request",
 "method":"GET","path":"/seismic-events","status":200,"duration_ms":98,"delay_ms":93,
 "client_ip":"172.18.0.1","correlation_id":"43043639-6671-406b-af73-93dfd23d86b2","target":"bmkg::access_log"}
```

- `duration_ms` is the whole time and includes the delay; `delay_ms` is the delay alone — subtracting the two gives BMKG's own processing time.
- Levels: a healthy `/health` is `DEBUG` (so polling does not clutter the log), a rejected request is `WARN`, a genuine server error would be `ERROR` (no code path currently produces one), everything else is `INFO`.
- `client_ip` is the connection address, not a forwarded header. The path is capped at 256 characters. Headers, request bodies, query strings and the API key are never logged.

### Event lines

| `event` | Level | When | Fields |
| --- | --- | --- | --- |
| `startup` | INFO | The service has started | `host`, `seed_records`, `generator_interval_ms`, `delay_min_ms`, `delay_max_ms`, `log_level`, `log_format` |
| `auth_rejected` | WARN | A protected route got no valid key | `reason` = `missing_key` or `invalid_key`, `correlation_id` |
| `shutdown` | INFO | SIGTERM or Ctrl+C received | — |

`auth_rejected` never logs the key value that was sent, right or wrong — only the reason.

## Notes and Limits

- **In memory only, per process.** All seeded data lives inside one process and is lost on restart. Running several copies gives each its own independent data.
- **One credential, no scopes.** BMKG has exactly one caller (the Aggregator) and one shared secret; there is nothing analogous to PVMBG's reader/admin split, and no admin routes exist at all.
- **`since` is exclusive.** A poller that always resends the latest timestamp it has seen will not see a record with that exact timestamp again, by design.
- **No panic-to-500 handler.** Handlers here do not panic in normal operation, so this has not mattered in practice, but there is no layer converting an unexpected panic into a clean `500` JSON response the way PVMBG's catch-all does. A genuine bug here would currently drop the connection rather than answer cleanly.
