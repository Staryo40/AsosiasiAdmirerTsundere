# Mock PVMBG

Mock PVMBG is a simulation of Indonesia's volcano-monitoring agency (Pusat Vulkanologi dan Mitigasi Bencana Geologi / Badan Geologi), built in Kotlin with [Ktor](https://ktor.io/). It serves volcanic activity reports, seeded at start-up and topped up with a new report every few seconds while it runs. It is a **passive** service: it never calls anything else, it only answers requests from whoever polls it.

Within TsundereDISTRIBUTED, PVMBG is one of two independent agency sources the Aggregator polls (the other is BMKG). It listens on port `8082`, has its own login (a client exchanges `client_id`/`client_secret` for a short-lived Bearer JWT), and — unlike BMKG — can be made slow, switched into a simulated outage, and have its schema change, all while running and without a restart. It keeps everything in memory: a restart means a fresh seed, by design.

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
| Language | Kotlin | 2.4.0 |
| HTTP framework | Ktor (server, Netty engine) | 3.5.2 |
| JVM target | — | toolchain 21 |
| Concurrency | kotlinx.coroutines | (coroutine-native Ktor) |
| Serialization | kotlinx.serialization (JSON) | bundled via Ktor content negotiation |
| Auth tokens | Ktor `auth-jwt` | 3.5.2 |

## Program Structure

```text
pvmbg/
├── src/
│   ├── main/            # application source: config, model, repository, routing, security, seed, service
│   └── test/             # Kotlin test suite
├── build.gradle.kts
├── gradle/libs.versions.toml   # dependency version catalog
├── Dockerfile
├── compose.yaml
└── .env.example
```

## Program Requirements

Either of the following:

- **JDK 21** to run it directly with Gradle, or
- **Docker** with **Compose 2.20 or newer** (`docker compose version`) to run it as a container.

A free TCP port `8082` on the host (or remap it, see below).

## Running the Application

### 1. Prepare configuration

```bash
cp .env.example .env
```

Fill it in — values below the "Optional tuning" heading have defaults and can stay as-is. Generate the signing key and both client secrets with:

```bash
openssl rand -hex 32
```

`.env` is git-ignored and must never be committed. Use plain values (hex is ideal) with no quotes and no spaces — the same file is read by Gradle, Docker, and the shell snippets below.

| Variable | Default | What it does |
| --- | --- | --- |
| `PVMBG_JWT_SECRET` | required | Key PVMBG signs its tokens with and checks every incoming token against. Never shared with any client. Use at least 32 random characters. |
| `PVMBG_READER_CLIENT_ID` / `_SECRET` | required | The **reader** client: can read reports. Meant for the Aggregator. |
| `PVMBG_ADMIN_CLIENT_ID` / `_SECRET` | required | The **admin** client: can read reports and use the `/admin` routes. Meant for the demo operator. Keep different from the reader's. |
| `PVMBG_TOKEN_TTL_SECONDS` | `300` | How long an access token stays valid. Use 30-60 to demonstrate expiry. |
| `PVMBG_LOGIN_RATE_LIMIT` | `5` | Login attempts allowed per minute per client id (an unknown id is counted per IP instead). |
| `PVMBG_RESPONSE_DELAY_MIN_MS` | `500` | Lower bound of the random delay on every response except `/admin/*`. |
| `PVMBG_RESPONSE_DELAY_MAX_MS` | `3000` | Upper bound. Set both to `3000` for a fixed 3-second delay, or both to `0` for none. |
| `PVMBG_GENERATOR_INTERVAL_SECONDS` | `10` | Seconds between two newly generated reports. Decimals allowed (at least `0.001`). |
| `PVMBG_LOG_LEVEL` | `INFO` | `TRACE`, `DEBUG`, `INFO`, `WARN`, `ERROR` or `OFF`. |
| `PVMBG_LOG_FORMAT` | `json` | `json` or `text`. Lowercase only. |

PVMBG **refuses to start** with a clear message if a required variable is missing or invalid. The delay range can also be changed live via [`POST /admin/response-delay`](#post-adminresponse-delay) — the `.env` value is only the starting point.

### 2. Run locally with Gradle

```bash
./gradlew run                     # start the service; loads .env; seeds every volcano from 2026-01-01 until now
./gradlew run --args="10"         # same, but seed exactly 10 initial reports instead
./gradlew test                    # run the tests
./gradlew build                   # compile, test and build the runnable jar (build/libs/pvmbg-all.jar)
```

Ready when it prints `PVMBG is running on http://localhost:8082`. Stop with Ctrl+C. `./gradlew run` loads `.env` itself; starting `main()` from an IDE's own Run button does **not** load it, so add the variables to that run configuration instead. A real environment variable always wins over `.env`, so `PVMBG_TOKEN_TTL_SECONDS=30 ./gradlew run` works for one-off changes.

### 3. Run with Docker

Run these from this folder (the one with `compose.yaml`):

```bash
docker compose up -d --build      # build the image and start the container on port 8082
docker compose logs -f            # follow the logs
docker compose stop pvmbg         # stop it (the container is kept)
docker compose up -d --no-build   # start it again without rebuilding
docker compose up -d --build pvmbg   # rebuild and restart only this service
docker compose down               # stop and remove the container
```

- The image contains only the jar — no `.env` and no secrets baked in. The container reads `.env` at start; a missing `.env` stops compose with an error. After changing `.env`, run `docker compose up -d --force-recreate`.
- Without compose: `docker build -t pvmbg .` then `docker run --rm --env-file .env -p 8082:8082 pvmbg`.
- Runs as a non-root user; `docker stop` exit code 143 is normal for a JVM.
- To run this service as part of the full system, a compose file at the repository root includes it with `include: [pvmbg/compose.yaml]` — see the [root README](../README.md).

### 4. Verify every scenario

Set up your shell once (from this folder, with the service running):

```bash
set -a; . ./.env; set +a
BASE=http://localhost:8082
login()  { curl -s -X POST $BASE/auth/login -H 'Content-Type: application/json' -d "{\"client_id\":\"$1\",\"client_secret\":\"$2\"}"; echo; }
token()  { login "$1" "$2" | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p'; }
READER=$(token "$PVMBG_READER_CLIENT_ID" "$PVMBG_READER_CLIENT_SECRET")
ADMIN=$(token "$PVMBG_ADMIN_CLIENT_ID" "$PVMBG_ADMIN_CLIENT_SECRET")
admin() { curl -s -o /dev/null -w "%{http_code}\n" -X POST "$BASE/admin/$1" -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' -d "$2"; }
timed() { curl -s -o /dev/null -w "%{http_code} in %{time_total}s\n" "$@"; }
```

Tokens last 5 minutes by default — if a command starts answering `401`, re-run the `READER=`/`ADMIN=` lines. With the default delay each call takes 0.5-3s; for snappier testing run `admin response-delay '{"min_ms":0,"max_ms":0}'` first.

**Read reports** (expect a JSON array without `confidence_level`):

```bash
curl -s -H "Authorization: Bearer $READER" $BASE/volcanic-reports | head -c 400; echo
```

**New data over time** (polling with `since`):

```bash
LAST=$(curl -s -H "Authorization: Bearer $READER" $BASE/volcanic-reports | grep -o '"reported_at":"[^"]*"' | tail -1 | cut -d'"' -f4)
sleep 12
curl -s -H "Authorization: Bearer $READER" "$BASE/volcanic-reports?since=$LAST"; echo
```

**The schema changes while running** (`confidence_level` — no restart needed):

```bash
admin schema-version '{"version":2}'     # expect 204
curl -s -H "Authorization: Bearer $READER" "$BASE/volcanic-reports?since=$LAST" | head -c 300; echo   # now with confidence_level
admin schema-version '{"version":1}'     # back to the original schema
```

**A simulated outage:**

```bash
admin outage '{"enabled":true}'                                  # 204
timed $BASE/health                                               # 503, immediately
timed -H "Authorization: Bearer $READER" $BASE/volcanic-reports  # 503, whatever the token
timed $BASE/                                                     # 200, root stays up
token "$PVMBG_ADMIN_CLIENT_ID" "$PVMBG_ADMIN_CLIENT_SECRET" | head -c 20; echo   # the admin can still log in
admin outage '{"enabled":false}'                                 # back to normal, no restart
```

**A slow service, and that the delay doesn't block other requests** (50 parallel requests take ~3s total, not 150):

```bash
admin response-delay '{"min_ms":3000,"max_ms":3000}'
time (seq 50 | xargs -P 50 -I{} curl -s -o /dev/null -H "Authorization: Bearer $READER" $BASE/volcanic-reports)
```

**Release a request stuck in a long delay:**

```bash
admin response-delay '{"min_ms":600000,"max_ms":600000}'
timed -H "Authorization: Bearer $READER" $BASE/volcanic-reports &      # waits...
sleep 2; admin response-delay '{"min_ms":0,"max_ms":0}'                # ...and comes back at once with 200
wait
```

**Authentication and authorisation failures:**

```bash
timed $BASE/volcanic-reports                                                      # 401 no token
timed -H "Authorization: Bearer not.a.token" $BASE/volcanic-reports              # 401 invalid token
timed -X POST $BASE/admin/outage -H "Authorization: Bearer $READER" -H 'Content-Type: application/json' -d '{"enabled":true}'   # 403 reader on an admin route
```

**Login rate limit:**

```bash
for i in 1 2 3 4 5 6 7; do curl -s -o /dev/null -w "attempt $i: %{http_code}\n" -X POST $BASE/auth/login \
  -H 'Content-Type: application/json' -d "{\"client_id\":\"$PVMBG_READER_CLIENT_ID\",\"client_secret\":\"wrong\"}"; done
```

The limit counts every login attempt (right or wrong) in the last minute. Once reached, the answer is `429` with `Retry-After`. The admin client has its own budget.

**Token expiry:** start with `PVMBG_TOKEN_TTL_SECONDS=20 ./gradlew run`, log in, wait 25s, then call `/volcanic-reports` with the old token: `401`, log shows `auth_rejected` with `reason=expired_token`.

**Correlation id:**

```bash
curl -s -D - -o /dev/null -H "X-Correlation-ID: demo-123" $BASE/health | grep -i correlation    # echoed back
```

**Stop, rebuild, and restart the container on its own:**

```bash
docker compose stop pvmbg            # the service is gone; other services in a bigger compose file are untouched
timed $BASE/health                   # connection refused (000)
docker compose up -d --build pvmbg   # rebuild only this image and start it again
```

## Endpoints

Errors are always JSON: `{"error": "<message>"}`.

| Method and path | Needs | Purpose |
| --- | --- | --- |
| `GET /` | nothing | Plain "Hello, World!". Always answers, even during an outage. |
| `GET /health` | nothing | Health check: `200 OK`, or `503` during a simulated outage. |
| `POST /auth/login` | nothing | Exchange client credentials for an access token. |
| `GET /volcanic-reports?since=<timestamp>` | `read` token | The reports. |
| `POST /admin/schema-version` | `admin` token | Switch the report schema (adds or removes `confidence_level`). |
| `POST /admin/outage` | `admin` token | Switch the simulated outage on or off. |
| `POST /admin/response-delay` | `admin` token | Change the response delay range. |

### Authentication

PVMBG issues and checks its own tokens, separate from any other service's credentials.

1. `POST /auth/login` with the client id and secret returns a token.
2. Send it as `Authorization: Bearer <access_token>` on later calls.
3. When it expires (after `PVMBG_TOKEN_TTL_SECONDS`), log in again.

| Client | Scope | Can use |
| --- | --- | --- |
| reader | `read` | `/volcanic-reports` |
| admin | `admin` (includes `read`) | `/volcanic-reports` and every `/admin/*` route |

The token is a JWT signed with HS256 using `PVMBG_JWT_SECRET`, claims `iss`=`pvmbg`, `aud`=`pvmbg-api`, `sub`=client id, `scope`, `jti`, `iat`, `exp`. A token from another service, one signed with another key, an expired one, or a tampered one is rejected with `401`. A valid `read` token on an admin route is `403`.

### `POST /auth/login`

```bash
curl -X POST http://localhost:8082/auth/login -H 'Content-Type: application/json' \
  -d '{"client_id":"<id>","client_secret":"<secret>"}'
```

Response `200`: `{"access_token":"<jwt>","token_type":"Bearer","expires_in":300}`.

| Status | When |
| --- | --- |
| `401` | Wrong id or wrong secret — same message for both, so the caller cannot tell which was wrong. |
| `429` | Too many attempts. `Retry-After` says how many seconds to wait. Counted per client id; an unknown id is counted per IP. Wrong attempts count too. |
| `503` | The **reader** tries to log in during a simulated outage. The **admin** can still log in, so the outage can be switched off. |
| `400`, `415` | The body is not valid JSON with both fields, or the content type is not `application/json`. |

### `GET /volcanic-reports`

Needs a `read` (or `admin`) token. Returns a JSON array of reports, oldest first.

- `since` (optional) is an ISO-8601 UTC timestamp. Only reports **strictly after** it are returned.
- `400` if `since` is invalid. `401` without a valid token. `403` is not possible for this route. `503` during an outage.

### `POST /admin/schema-version`

Body `{"version": 1}` or `{"version": 2}`. Answers `204`, takes effect immediately. Any other number is `400`.

### `POST /admin/outage`

Body `{"enabled": true}` or `{"enabled": false}`. Answers `204`, takes effect immediately.

| While an outage is on | Behaves as |
| --- | --- |
| `GET /health`, `GET /volcanic-reports` | `503` straight away, regardless of token |
| `POST /auth/login` as reader | `503` |
| `POST /auth/login` as admin, every `/admin/*` route | work as normal (still checked: `401`/`403` still apply) |
| `GET /` | `200` |
| Requests already waiting out their delay | released at once with `503` |

### `POST /admin/response-delay`

Body `{"min_ms": 500, "max_ms": 3000}`. Answers `204`, and `400` if a value is negative or `min_ms` > `max_ms`. Every response except `/admin/*` waits a random time in that range first — even a rejected one. The wait does not block a thread. Sending this request **releases every request already waiting**, which continues at once with its normal answer. Only the `.env` value survives a restart.

### Status codes

| Code | Meaning here |
| --- | --- |
| `400` | Bad body, bad `since`, or bad admin value |
| `401` | Missing, invalid, expired or foreign token, or wrong login credentials |
| `403` | Valid token, scope too low |
| `415` | Body is not `application/json` |
| `429` | Login rate limit reached |
| `500` | Unexpected failure (details go to the log, never the body) |
| `503` | Simulated outage |

### Response schemas

**Volcanoes:** `merapi`, `semeru`, `bromo`, `krakatau`, `sinabung`. `alert_level` is one of `Normal`, `Waspada`, `Siaga`, `Awas`.

A report as served by default (**schema version 1**):

```json
{
  "report_id": "rpt-000123",
  "volcano_id": "merapi",
  "alert_level": "Waspada",
  "eruption_count_24h": 12,
  "ash_column_height_m": 1840.0,
  "reported_at": "2026-09-21T12:00:00Z"
}
```

After `POST /admin/schema-version` with `{"version": 2}` (**schema version 2**), each report also has `"confidence_level": 0.87` (a number between 0 and 1). Version 1 does not have the field.

**Data generation:** by default one report per volcano for every 6-hour slot from 2026-01-01 00:00 UTC until now is seeded at start-up. While running, one new report for a random volcano appears every `PVMBG_GENERATOR_INTERVAL_SECONDS`, timestamped now. The seed is fixed, so the seeded history is the same on every start.

## Logging

Logs go to **standard output**, one event per line. Read them with the terminal running `./gradlew run`, or `docker compose logs -f` (add `--no-log-prefix` for clean JSON).

- `PVMBG_LOG_FORMAT=json` (default) writes one JSON object per line with fixed field names; `text` writes readable console lines.
- `PVMBG_LOG_LEVEL` sets the minimum level. Every line carries `service: pvmbg`, a timestamp, the level and the logger name.
- Lines are written on a separate thread and none is ever dropped, so logging does not slow requests down; pending lines are flushed on shutdown.

### Correlation id

- Read from the request header **`X-Correlation-ID`**. If missing, a UUID is generated.
- Accepted only if 1 to 64 characters from `A-Za-z0-9._-`. Anything else is **replaced** by a generated UUID and never logged.
- Returned in the `X-Correlation-ID` response header and appears as `correlation_id` on every log line for that request.

### The access line

One line per request, written when the response has been sent (`event` = `http_request`):

```json
{"@timestamp":"2026-09-21T21:38:30.210+07:00","message":"GET /volcanic-reports 200 404ms","logger":"access","level":"INFO",
 "correlation_id":"poll-7f3c","event":"http_request","method":"GET","path":"/volcanic-reports","status":200,
 "duration_ms":404,"delay_ms":300,"client_ip":"0:0:0:0:0:0:0:1","client_id":"reader-client","scope":"read","service":"pvmbg"}
```

- `duration_ms` includes the artificial delay; `delay_ms` is the simulated-delay portion (shorter than the picked delay when released early).
- `client_id`/`scope` appear only when the token was valid. Levels: `INFO` normally, `WARN` for 401/403/429/503, `ERROR` for other 5xx, `DEBUG` for a healthy `/health`.
- At `DEBUG`, a second line (`event` = `request_received`) is written on arrival, so a request stuck in its delay is visible while it waits.
- Path capped at 256 characters. Headers, request bodies, query strings and tokens are never logged.

### Event lines

| `event` | Level | When | Fields |
| --- | --- | --- | --- |
| `startup` | INFO | Service started | `host`, `port`, `seed_records`, `generator_interval_ms`, `delay_min_ms`, `delay_max_ms`, `token_ttl_seconds`, `login_rate_limit_per_minute`, `log_level`, `log_format` |
| `login_succeeded` | INFO | Token issued | `client_id`, `scope`, `token_id`, `expires_in` |
| `login_failed` | WARN | Wrong login | `client_id`, `reason` = `unknown_client` or `wrong_secret` |
| `login_rejected` | WARN | Reader login during outage | `client_id`, `reason` = `outage` |
| `rate_limited` | WARN | Login limit reached | `key` (`client:<id>` or `ip:<address>`), `retry_after_seconds` |
| `auth_rejected` | WARN | No valid token on a protected route | `reason` = `missing_token`, `wrong_scheme`, `malformed_token`, `expired_token` or `invalid_token` |
| `scope_denied` | WARN | Valid token, scope too low | `client_id`, `required`, `granted` |
| `outage_changed` | INFO | Outage switched | `enabled`, `client_id` |
| `schema_version_changed` | INFO | Schema switched | `version`, `client_id` |
| `response_delay_changed` | INFO | Delay range changed | `min_ms`, `max_ms`, `client_id` |
| `unhandled_exception` | ERROR | Unexpected failure | `status`, `exception`, plus stack trace on the same line |

### What is and is not logged

- **Never logged:** access tokens, client secrets, the signing key, request bodies, headers (including `Authorization`), query strings. A token is identified only by its `token_id`.
- **Logged exactly as sent:** the `client_id` of a **failed login**, cut at 256 characters, even if it matches no client — if someone types a password into the id field, it ends up in the log.
- Line breaks and quotes in any logged value are escaped, so a hostile value cannot split or forge a log line.

## Notes and Limits

- **In memory only.** Reports, the schema version, the outage flag, the delay range and rate-limit counters are lost on restart, and live only inside one process.
- **Health and root are public.** Everything else needs a token. The two clients come from `.env`; there is no user database.
- **`since` is exclusive** and timestamps have one-second precision, so a poller that always sends the latest `reported_at` it has seen could miss a report added in that same second. Polling with an inclusive margin and de-duplicating by `report_id` is safer.
- **Throwaway test credentials.** Tests use dummy values (e.g. `test-reader-secret`) set in `build.gradle.kts`; they are not real credentials. Real ones exist only in your git-ignored `.env`.
