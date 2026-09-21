# PVMBG (mock service)

A simulation of the volcano-monitoring agency's service (PVMBG / Badan Geologi), built for the IF4031 Milestone 1 project.
It is a **passive** service: it only answers requests, and the BNPB gateway polls it. It listens on **port 8082**.

- Serves `VolcanicReport`s, seeded at start-up and topped up with a new report every few seconds while it runs.
- Has its own login: a client exchanges its `client_id` and `client_secret` for a short-lived Bearer token (a JWT signed by PVMBG).
- Can be made slow, switched into a simulated outage, and changed to add a `confidence_level` field, all while running.
- Keeps everything in memory. Nothing is stored on disk, so a restart starts again from a fresh seed.

Contents: [Run it](#run-it) | [Configuration](#configuration) | [Data](#data) | [API](#api) | [Trigger every scenario](#trigger-every-scenario) | [Logging](#logging) | [Notes and limits](#notes-and-limits)

## Run it

You need either **JDK 21 + this repository**, or **Docker with Compose 2.20 or newer** (`docker compose version`).

### 1. Create your `.env` (both ways)

```bash
cp .env.example .env
```

Open `.env` and fill it in. The values below the "Optional tuning" heading have defaults and can stay as they are.
Every variable is explained in `.env.example` and in [Configuration](#configuration). Generate the signing key with:

```bash
openssl rand -hex 32
```

`.env` is git-ignored and must never be committed. Use plain values (hex is ideal) with no quotes and no spaces, because the
same file is read by Gradle, by Docker and by the shell snippets below.

### 2a. Run locally with Gradle

```bash
./gradlew run                     # start the service; loads .env; seeds every volcano from 2026-01-01 until now
./gradlew run --args="10"         # same, but seed exactly 10 initial reports instead
./gradlew run --args="-P:ktor.deployment.port=9000"   # listen on another port
./gradlew test                    # run the tests
./gradlew build                   # compile, test and build the runnable jar (build/libs/pvmbg-all.jar)
```

When it is ready it prints `PVMBG is running on http://localhost:8082`. Stop it with Ctrl+C.

`./gradlew run` loads `.env` itself. If you start `main()` with an IDE's own Run button instead, `.env` is **not** loaded, so
add the variables to that run configuration. A real environment variable always wins over the same name in `.env`, so
`PVMBG_TOKEN_TTL_SECONDS=30 ./gradlew run` works for one-off changes.

### 2b. Run with Docker

Run these from this folder (the one with `compose.yaml`):

```bash
docker compose up -d --build      # build the image and start the container on port 8082
docker compose logs -f            # follow the logs (Ctrl+C stops following, not the service)
docker compose stop pvmbg         # stop it (the container is kept)
docker compose up -d --no-build   # start it again without rebuilding
docker compose up -d --build pvmbg   # rebuild and restart only this service
docker compose down               # stop and remove the container
```

- The image contains only the jar. **No `.env` and no secrets are baked in**: the container reads `.env` when it starts
  (`env_file` in `compose.yaml`), so a missing `.env` stops compose with an error. After changing `.env`, run
  `docker compose up -d --force-recreate`.
- Without compose: `docker build -t pvmbg .` then `docker run --rm --env-file .env -p 8082:8082 pvmbg`. Arguments after the
  image name go to the service, so `docker run ... pvmbg 10` seeds 10 reports.
- To use another port on your machine, change the left side of `"8082:8082"` in `compose.yaml`. Inside the container the
  service always listens on 8082.
- The container runs as a non-root user, and `docker stop` ends it cleanly in under a second (exit code 143 is normal for a JVM).
- To run this service as part of a bigger system, a compose file at the repository root can include it with
  `include: [pvmbg/compose.yaml]`.
- Container logs use UTC timestamps.

## Configuration

All settings come from environment variables (read from `.env` for Gradle and compose). PVMBG **refuses to start** with a
clear message if a required variable is missing or a value is invalid, so a typo can never silently change behaviour.

| Variable | Default | What it does |
|---|---|---|
| `PVMBG_JWT_SECRET` | required | Key PVMBG signs its tokens with and uses to check every incoming token. Never shared with any client. Use at least 32 random characters. |
| `PVMBG_READER_CLIENT_ID` | required | Login name of the **reader** client: can read reports. Meant for the BNPB gateway. |
| `PVMBG_READER_CLIENT_SECRET` | required | Password of the reader client. |
| `PVMBG_ADMIN_CLIENT_ID` | required | Login name of the **admin** client: can read reports and use the `/admin` routes. Meant for the demo operator. |
| `PVMBG_ADMIN_CLIENT_SECRET` | required | Password of the admin client. Keep it different from the reader's. |
| `PVMBG_TOKEN_TTL_SECONDS` | `300` | How long an access token stays valid. Use 30 to 60 to demonstrate expiry. |
| `PVMBG_LOGIN_RATE_LIMIT` | `5` | Login attempts allowed per minute per client id (an unknown id is counted per IP address instead). |
| `PVMBG_RESPONSE_DELAY_MIN_MS` | `500` | Lower bound of the random delay added to every response except `/admin/*`. Must not exceed the maximum. |
| `PVMBG_RESPONSE_DELAY_MAX_MS` | `3000` | Upper bound of that delay. Set both to `3000` for a fixed 3-second delay. Set both to `0` for none. |
| `PVMBG_GENERATOR_INTERVAL_SECONDS` | `10` | Seconds between two newly generated reports. Decimals are allowed (at least `0.001`). |
| `PVMBG_LOG_LEVEL` | `INFO` | `TRACE`, `DEBUG`, `INFO`, `WARN`, `ERROR` or `OFF`. |
| `PVMBG_LOG_FORMAT` | `json` | `json` (one JSON object per line) or `text` (readable lines). Lowercase only. |

The delay range can also be changed while the service runs (see [`POST /admin/response-delay`](#post-adminresponse-delay)),
and the value in `.env` is only the starting value.

Other settings:

| Setting | Value | How to change it |
|---|---|---|
| Port | `8082` | `./gradlew run --args="-P:ktor.deployment.port=9000"`, or the port mapping in `compose.yaml` for Docker |
| Number of seeded reports | every volcano, every 6 hours since 2026-01-01 | `--args="N"` seeds exactly N reports |

## Data

- **Volcanoes:** `merapi`, `semeru`, `bromo`, `krakatau`, `sinabung`.
- **Seed at start-up:** by default one report per volcano for every 6-hour slot from 2026-01-01 00:00 UTC until now
  (thousands of reports; the spec asks for at least 20). `--args="N"` seeds exactly `N` reports instead, ending at the latest slot.
- **While running:** one new report for a random volcano every `PVMBG_GENERATOR_INTERVAL_SECONDS` seconds, timestamped now.
- **Values:** `alert_level` is random over `Normal`, `Waspada`, `Siaga`, `Awas`. Counts and heights are random. The seed is fixed,
  so the seeded history is the same on every start, apart from how far it reaches.

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

After `POST /admin/schema-version` with `{"version": 2}` (**schema version 2**) each report also has `"confidence_level": 0.87`
(a number between 0 and 1). Version 1 does not have the field. Reports are always returned oldest first.

## API

Errors are always JSON: `{"error": "<message>"}`.

| Method and path | Needs | Purpose |
|---|---|---|
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
2. Send it on later calls as `Authorization: Bearer <access_token>`.
3. When it expires (after `PVMBG_TOKEN_TTL_SECONDS`), log in again.

Two clients exist, and the token carries the client's **scope**:

| Client | Scope | Can use |
|---|---|---|
| reader | `read` | `/volcanic-reports` |
| admin | `admin` (includes `read`) | `/volcanic-reports` and every `/admin/*` route |

The token is a JWT signed with HS256 using `PVMBG_JWT_SECRET`, with the claims `iss` = `pvmbg`, `aud` = `pvmbg-api`,
`sub` = the client id, `scope`, `jti`, `iat` and `exp`. A token from another service, one signed with another key, an expired
one, or a tampered one is rejected with `401`. Using a valid `read` token on an admin route is `403`.

### `POST /auth/login`

```bash
curl -X POST http://localhost:8082/auth/login -H 'Content-Type: application/json' \
  -d '{"client_id":"<id>","client_secret":"<secret>"}'
```

Response `200`: `{"access_token":"<jwt>","token_type":"Bearer","expires_in":300}`.

| Status | When |
|---|---|
| `401` | Wrong id or wrong secret. The message is the same for both, so the caller cannot tell which was wrong. |
| `429` | Too many attempts. `Retry-After` says how many seconds to wait. Counted per client id; an unknown id is counted per IP. Wrong attempts count as well as right ones. |
| `503` | The **reader** tries to log in during a simulated outage. The **admin** can still log in, so the outage can be switched off. |
| `400`, `415` | The body is not valid JSON with both fields, or the content type is not `application/json`. |

### `GET /volcanic-reports`

Needs a `read` (or `admin`) token. Returns a JSON array of reports.

- `since` (optional) is an ISO-8601 UTC timestamp such as `2026-09-21T00:00:00Z`. Only reports **strictly after** it are returned,
  so a poller can pass the latest `reported_at` it has seen. Without it every report is returned.
- `400` if `since` is not a valid timestamp. `401` without a valid token. `403` is not possible for this route. `503` during an outage.

### `POST /admin/schema-version`

Body `{"version": 1}` or `{"version": 2}`. Answers `204`. Takes effect immediately, with no restart. Any other number is `400`.

### `POST /admin/outage`

Body `{"enabled": true}` or `{"enabled": false}`. Answers `204`. Takes effect immediately.

While an outage is on:

| Request | Answer |
|---|---|
| `GET /health`, `GET /volcanic-reports` | `503` straight away, whatever the token (a dead service cannot check it) |
| `POST /auth/login` as reader | `503` |
| `POST /auth/login` as admin, and every `/admin/*` route | work as normal. The token is still checked, so `401` or `403` still apply. |
| `GET /` | `200` |
| Requests already waiting out their delay | released at once with `503` |

### `POST /admin/response-delay`

Body `{"min_ms": 500, "max_ms": 3000}`. Answers `204`, and `400` if a value is negative or `min_ms` is above `max_ms`.

Every response except `/admin/*` waits a random time in that range first, **even a rejected one** (a `401` for a bad token is slow too).
The wait does not block a thread, so many requests can wait at the same time. Sending this request, even with the same values, also
**releases every request that is already waiting**: they continue at once and get their normal answer. Only the value in `.env`
survives a restart.

### Status codes at a glance

| Code | Meaning here |
|---|---|
| `400` | Bad body or bad `since` or bad admin value |
| `401` | Missing, invalid, expired or foreign token, or wrong login credentials |
| `403` | Valid token, but the scope is too low |
| `415` | Body is not `application/json` |
| `429` | Login rate limit reached |
| `500` | Unexpected failure. The body never contains details; they go to the log. |
| `503` | Simulated outage |

## Trigger every scenario

These use `curl`. First set up your shell once (from this folder, with the service running):

```bash
set -a; . ./.env; set +a          # loads the credentials from .env into your shell
BASE=http://localhost:8082
login()  { curl -s -X POST $BASE/auth/login -H 'Content-Type: application/json' -d "{\"client_id\":\"$1\",\"client_secret\":\"$2\"}"; echo; }
token()  { login "$1" "$2" | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p'; }
READER=$(token "$PVMBG_READER_CLIENT_ID" "$PVMBG_READER_CLIENT_SECRET")
ADMIN=$(token "$PVMBG_ADMIN_CLIENT_ID" "$PVMBG_ADMIN_CLIENT_SECRET")
admin() { curl -s -o /dev/null -w "%{http_code}\n" -X POST "$BASE/admin/$1" -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' -d "$2"; }
timed() { curl -s -o /dev/null -w "%{http_code} in %{time_total}s\n" "$@"; }
```

Tokens last 5 minutes by default. If a command starts answering `401`, run the two `READER=` and `ADMIN=` lines again.
With the default delay each call takes 0.5 to 3 seconds. To make the examples snappy, first run `admin response-delay '{"min_ms":0,"max_ms":0}'`
and set it back at the end.

### 1. Read reports

```bash
curl -s -H "Authorization: Bearer $READER" $BASE/volcanic-reports | head -c 400; echo
```

Expect a JSON array of reports without `confidence_level`.

### 2. New data appears over time (polling with `since`)

```bash
LAST=$(curl -s -H "Authorization: Bearer $READER" $BASE/volcanic-reports | grep -o '"reported_at":"[^"]*"' | tail -1 | cut -d'"' -f4)
echo "latest so far: $LAST"; sleep 12
curl -s -H "Authorization: Bearer $READER" "$BASE/volcanic-reports?since=$LAST"; echo
```

Expect one or more reports newer than `LAST` (one per `PVMBG_GENERATOR_INTERVAL_SECONDS`). For a faster demo start the service
with `PVMBG_GENERATOR_INTERVAL_SECONDS=1`.

### 3. The schema changes while running (`confidence_level`)

```bash
admin schema-version '{"version":2}'     # expect 204
curl -s -H "Authorization: Bearer $READER" "$BASE/volcanic-reports?since=$LAST" | head -c 300; echo   # now with confidence_level
admin schema-version '{"version":1}'     # back to the original schema
```

No restart of anything is needed.

### 4. A simulated outage

```bash
admin outage '{"enabled":true}'                                  # 204
timed $BASE/health                                               # 503, immediately
timed -H "Authorization: Bearer $READER" $BASE/volcanic-reports  # 503, whatever the token
timed $BASE/                                                     # 200, root stays up
login "$PVMBG_READER_CLIENT_ID" "$PVMBG_READER_CLIENT_SECRET"    # {"error":"service is in outage"}
token "$PVMBG_ADMIN_CLIENT_ID" "$PVMBG_ADMIN_CLIENT_SECRET" | head -c 20; echo   # the admin can still log in
admin outage '{"enabled":false}'                                 # back to normal, no restart
timed $BASE/health                                               # 200
```

### 5. A slow service

```bash
admin response-delay '{"min_ms":3000,"max_ms":3000}'
timed -H "Authorization: Bearer $READER" $BASE/volcanic-reports         # about 3 seconds
timed $BASE/health                                                      # about 3 seconds too
timed -H "Authorization: Bearer garbage" $BASE/volcanic-reports        # 401, also about 3 seconds
timed -X POST $BASE/admin/outage -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' -d '{"enabled":false}'  # admin: instant
```

**Many requests at the same time** (the delay does not block other requests): 50 parallel requests take about 3 seconds in total, not 150.

```bash
time (seq 50 | xargs -P 50 -I{} curl -s -o /dev/null -H "Authorization: Bearer $READER" $BASE/volcanic-reports)
```

**Release a request that is stuck** (for example after setting a very long delay):

```bash
admin response-delay '{"min_ms":600000,"max_ms":600000}'
timed -H "Authorization: Bearer $READER" $BASE/volcanic-reports &      # waits...
sleep 2; admin response-delay '{"min_ms":0,"max_ms":0}'                # ...and comes back at once with 200
wait
```

Switching the outage **on** also releases waiting requests, with `503` instead.

### 6. Authentication and authorisation failures

```bash
timed $BASE/volcanic-reports                                                      # 401 no token
timed -H "Authorization: Bearer not.a.token" $BASE/volcanic-reports              # 401 invalid token
timed -H "X-BMKG-Key: some-bmkg-key" $BASE/volcanic-reports                       # 401 another service's credential is worthless here
timed -X POST $BASE/admin/outage -H "Authorization: Bearer $READER" -H 'Content-Type: application/json' -d '{"enabled":true}'   # 403 reader on an admin route
login "$PVMBG_READER_CLIENT_ID" wrong-secret                                      # 401 {"error":"invalid client credentials"}
login some-unknown-id some-secret                                                 # 401, the same message
```

### 7. Login rate limit (your own small attack)

```bash
for i in 1 2 3 4 5 6 7; do curl -s -o /dev/null -w "attempt $i: %{http_code}\n" -X POST $BASE/auth/login \
  -H 'Content-Type: application/json' -d "{\"client_id\":\"$PVMBG_READER_CLIENT_ID\",\"client_secret\":\"wrong\"}"; done
```

The limit counts **every** login attempt of that client in the last minute (right or wrong, and even during an outage), including the ones from
the setup above and from earlier scenarios. To see the 429 arrive exactly at the sixth attempt, wait a minute since your last login first.
Once the limit is reached the answer is `429` with a `Retry-After` header. The admin client has its own budget and is unaffected.
Wait a minute for the reader to work again. Tokens that were already issued keep working.

### 8. Token expiry

Start the service with a short lifetime, for example `PVMBG_TOKEN_TTL_SECONDS=20 ./gradlew run` (or change `.env` and run
`docker compose up -d --force-recreate`). Log in, wait 25 seconds, then call `/volcanic-reports` with the old token: `401`, and the
log shows `auth_rejected` with `reason=expired_token`. Log in again for a new token. The old one stays rejected.

### 9. Correlation id

```bash
curl -s -D - -o /dev/null -H "X-Correlation-ID: demo-123" $BASE/health | grep -i correlation    # echoed back
curl -s -D - -o /dev/null $BASE/health | grep -i correlation                                    # a generated UUID
curl -s -D - -o /dev/null -H "X-Correlation-ID: not valid!" $BASE/health | grep -i correlation  # replaced by a UUID
```

Then find every log line of that request with `grep demo-123` (see [Logging](#logging)).

### 10. Stop, rebuild and restart the container on its own

```bash
docker compose stop pvmbg            # the service is gone; other services in a bigger compose file are untouched
timed $BASE/health                   # connection refused (000)
docker compose up -d --build pvmbg   # rebuild only this image and start it again
```

The data starts again from a fresh seed, because nothing is stored.

## Logging

Logs go to **standard output**, one event per line. Read them with the terminal running `./gradlew run`, or with
`docker compose logs -f` (add `--no-log-prefix` to get clean JSON lines).

- `PVMBG_LOG_FORMAT=json` (default) writes one JSON object per line with fixed field names. `text` writes readable lines for a console.
- `PVMBG_LOG_LEVEL` sets the minimum level. Every line carries `service: pvmbg`, a timestamp, the level and the logger name.
- Lines are written on a separate thread and none is ever dropped, so logging does not slow requests down. Pending lines are flushed on shutdown.

### Correlation id

Every request has a **correlation id**, so one flow can be followed across services.

- PVMBG reads it from the request header **`X-Correlation-ID`**. If it is missing, PVMBG generates a UUID.
- An id is accepted only if it is 1 to 64 characters from `A-Za-z0-9._-`. Anything else is **replaced** by a generated UUID and never logged.
- The id is returned in the `X-Correlation-ID` response header and appears as `correlation_id` on **every** log line written for that request.
- The BNPB gateway must use this same header name and send the same id to every service in one flow, otherwise the logs cannot be joined.

Find one request across the logs:

```bash
docker compose logs --no-log-prefix | grep '"correlation_id":"demo-123"'
```

### The access line

One line per request, written when the response has been sent (`event` = `http_request`):

```json
{"@timestamp":"2026-09-21T21:38:30.210+07:00","message":"GET /volcanic-reports 200 404ms","logger":"access","level":"INFO",
 "correlation_id":"poll-7f3c","event":"http_request","method":"GET","path":"/volcanic-reports","status":200,
 "duration_ms":404,"delay_ms":300,"client_ip":"0:0:0:0:0:0:0:1","client_id":"reader-client","scope":"read","service":"pvmbg"}
```

- `duration_ms` is the whole time and includes the artificial delay. `delay_ms` is the part that was the simulated delay,
  which is shorter than the picked delay when the request was released early. Subtract to get PVMBG's own processing time.
- `client_id` and `scope` appear only when the token was valid. `client_ip` is the connection address, not a forwarded header.
- Levels: `INFO` normally, `WARN` for 401, 403, 429 and 503 (503 is the deliberate simulated outage), `ERROR` for other 5xx.
  A healthy `/health` is logged at `DEBUG` only, so it does not clutter the log.
- At `DEBUG` a second line (`event` = `request_received`) is written when a request arrives, so a request stuck in its delay is visible while it waits.
- The path is cut at 256 characters. Headers, request bodies, query strings and tokens are never logged.

### Event lines

Things that happened, each with an `event` field and the same `correlation_id`:

| `event` | Level | When | Fields |
|---|---|---|---|
| `startup` | INFO | The service has started | `host`, `port`, `seed_records`, `generator_interval_ms`, `delay_min_ms`, `delay_max_ms`, `token_ttl_seconds`, `login_rate_limit_per_minute`, `log_level`, `log_format` |
| `login_succeeded` | INFO | A token was issued | `client_id`, `scope`, `token_id`, `expires_in` |
| `login_failed` | WARN | Wrong login | `client_id`, `reason` = `unknown_client` or `wrong_secret` |
| `login_rejected` | WARN | Reader login during an outage | `client_id`, `reason` = `outage` |
| `rate_limited` | WARN | Login limit reached | `key` (`client:<id>` or `ip:<address>`), `retry_after_seconds` |
| `auth_rejected` | WARN | A protected route got no valid token | `reason` = `missing_token`, `wrong_scheme`, `malformed_token`, `expired_token` or `invalid_token` |
| `scope_denied` | WARN | Valid token, scope too low | `client_id`, `required`, `granted` |
| `outage_changed` | INFO | Outage switched | `enabled`, `client_id` (the admin who did it) |
| `schema_version_changed` | INFO | Schema switched | `version`, `client_id` |
| `response_delay_changed` | INFO | Delay range changed | `min_ms`, `max_ms`, `client_id` |
| `unhandled_exception` | ERROR | An unexpected failure | `status`, `exception`, plus the stack trace on the same line |

`auth_rejected`'s `expired_token` is worked out from an unverified look at the token, so it is a hint for the log only. The request is rejected either way.

### What is and is not logged

- **Never logged:** access tokens, client secrets, the signing key, request bodies, headers (including `Authorization`), and query strings.
  A token is identified in the log only by its `token_id`.
- **Logged exactly as sent:** the `client_id` of a **failed login**. It is written as it arrived (cut at 256 characters), even when it
  matches no client. **If someone types a password into the id field, it ends up in the log.** Keep this in mind when handling the logs.
- Line breaks and quotes in any logged value are escaped, so a hostile value cannot split or forge a log line.

## Notes and limits

- **In memory only.** Reports, the schema version, the outage flag, the delay range and the rate-limit counters are lost on restart.
  They also live inside one process, so running several copies gives each its own data and its own counters.
- **Health and root are public.** Everything else needs a token. The two clients are set up from `.env`; there is no user database.
- **`since` is exclusive** and timestamps have one-second precision, so a poller that always sends the latest `reported_at` it has seen
  could miss a report added in that same second. Polling with an inclusive margin and removing duplicates by `report_id` is safer.
- **Throwaway test credentials.** The tests use dummy values (for example `test-reader-secret`) set in `build.gradle.kts`; they are not real
  credentials. Real ones exist only in your git-ignored `.env`.
