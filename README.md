# TsundereDISTRIBUTED - Asosiasi Admirer Tsundere

TsundereDISTRIBUTED is a disaster-coordination platform that aggregates seismic and volcanic hazard reports from two independent agency sources, Indonesia's BMKG (meteorology/seismology) and PVMBG (volcanology), into one canonical event stream. Incoming reports are mapped into a single `HazardEvent` format, stored in a database owned by exactly one service, published to a message broker, and served to downstream clients through a role-based read API — with independent consumers reacting to the same event stream for dashboards, alerting, and third-party feeds.

This repository is the Milestone 1 submission for IF4031 Arsitektur Aplikasi Terdistribusi. It contains mock implementations of both agency sources, the full BNPB backend (ingestion, canonical storage, authentication, client-facing API, and message-broker consumers), and the Docker Compose orchestration tying all of it into one runnable system.

## Table of Contents

- [Technology and Frameworks](#technology-and-frameworks)
- [Program Structure](#program-structure)
- [Program Requirements](#program-requirements)
- [Running the Application](#running-the-application)
- [Acceptance Testing (P01–P05)](#acceptance-testing-p01p05)
- [Endpoints](#endpoints)
- [Creators](#creators)

## Technology and Frameworks

| Component | Language / Framework | Version |
| --- | --- | --- |
| Mock BMKG | Rust, Axum | Rust edition 2024 · Axum 0.8.9 · Tokio 1.53.1 |
| Mock PVMBG | Kotlin, Ktor | Kotlin 2.4.0 · Ktor 3.5.2 · JVM toolchain 21 |
| BNPB backend (Aggregator, Auth, Client-Facing API, consumer-1/2/3) | Go | Go 1.26.0 · toolchain 1.26.8 |
| Canonical Store | PostgreSQL | 18.6 |
| Message broker | RabbitMQ | 4.3.5 (management image) |
| Per-consumer local storage | SQLite via `modernc.org/sqlite` | pure Go, no CGO |
| Orchestration | Docker, Docker Compose | Compose file `include:` (requires Compose 2.20+) |
| Acceptance testing | Python | 3.9+ (standard library only) |

## Program Structure

![System architecture and data flow](docs/architecture.svg)

Every box above is an independent Docker container with its own Dockerfile and lifecycle. Dashed, colored regions are separate Docker networks — a service can only reach what it shares a network with. This is what keeps BMKG and PVMBG polling fully isolated from each other (a slow PVMBG can never block BMKG traffic), and what makes the Canonical Store reachable only by the Aggregator.

Simplified file layout:

```text
.
├── bmkg/                   # Mock BMKG (Rust/Axum)
│   └── src/                # config, models, plugins, routing, seed, state
├── pvmbg/                  # Mock PVMBG (Kotlin/Ktor)
│   └── src/                # main, test
├── bnpb/                   # BNPB backend (Go)
│   ├── cmd/                # aggregator, api, auth, consumer-1/2/3
│   ├── internal/           # per-service packages + shared platform/
│   ├── broker/             # RabbitMQ provisioning
│   └── contracts/v1/       # OpenAPI contract + schemas
├── testing/                # acceptance suites (Python), per Problem
│   ├── P01/ P02/ P03/ P04/ P05/
│   └── common/             # shared test helpers
├── docs/architecture.svg   # diagram used above
├── compose.yaml            # root orchestration (includes all three)
├── compose.override.yaml   # host-exposed inspection ports
└── README.md
```

Each of `bmkg/`, `pvmbg/`, and `bnpb/` also has its own `README.md` with component-specific detail, and its own `compose.yaml` so it can be developed and run standalone.

## Program Requirements

- Docker Engine and Docker Compose **2.20 or newer** (required for the root `compose.yaml`'s `include:` directive).
- Enough free memory/CPU for eleven containers at once (two mocks, five BNPB Go services, PostgreSQL, RabbitMQ, plus the one-shot `broker-init`).
- These host ports free: `8080` (Client-Facing API), `8081` (BMKG), `8082` (PVMBG), `8083` (Aggregator, inspection only), `15672` (RabbitMQ management UI, inspection only).
- `openssl` (or any equivalent) to generate secrets when preparing `.env` files.

Optional, only needed if you want to run a component's code or tests without Docker:

- Rust (stable) for BMKG.
- JDK 21 for PVMBG.
- Go matching `bnpb/go.mod` (1.26.x) for the BNPB backend.
- Python 3.9+ for the acceptance suites under `testing/`.

## Running the Application

### 1. Prepare configuration

Each of `bmkg/`, `pvmbg/`, and `bnpb/` needs its own `.env`, copied from its `.env.example` and filled with real secrets (never commit these). See each component's own README for the exact variables; `openssl rand -hex 32` is used throughout to generate secret values.

```bash
cp bmkg/.env.example bmkg/.env
cp pvmbg/.env.example pvmbg/.env
cp bnpb/.env.example bnpb/.env
```

### 2. Start from a clean state

Run these from the **repository root**.

**Core system only** (consumer-3 excluded — this is the default and what you want for normal operation):

```bash
docker compose up -d --build
docker compose ps
curl http://localhost:8080/health
curl http://localhost:8080/ready
```

**Core system with the third consumer included from the start** (consumer-3 exists purely to demonstrate that a consumer can be plugged into the broker without touching the producer — see below for adding it live instead):

```bash
docker compose --profile demo up -d --build
```

Either way, check `docker compose ps --all`: the one-shot `broker-init` service must show **Exited (0)** before the Aggregator or any consumer can connect to RabbitMQ.

### 3. Add a service to an already-running system, live

This is the normal way to bring up consumer-3: start the core system first (step 2, without `--profile demo`), then add it later without restarting anything already running.

```bash
docker compose --profile demo up -d --build consumer-3
```

`consumer-1` and `consumer-2` are untouched by this — you can confirm with `docker inspect <container> --format '{{.State.StartedAt}}'` before and after, the timestamps won't change.

### 4. Stop a single service, live, without shutting down the rest

```bash
docker compose stop consumer-3
```

Everything else — the Aggregator, the other two consumers, the API — keeps running normally. RabbitMQ keeps queueing messages for `consumer-3`'s own queue while it's down; restart it the same way to pick the backlog back up:

```bash
docker compose start consumer-3
```

This works for any single service, not just consumer-3 — swap the name (e.g. `docker compose stop pvmbg`) to take any one component down independently.

To rebuild a service after changing its code, without touching anything else:

```bash
docker compose up -d --build <service-name>
```

### 5. Shut down the entire system

Preserve all data (PostgreSQL, RabbitMQ, each consumer's SQLite) for next time:

```bash
docker compose --profile demo down
```

Wipe all data and start fresh next time:

```bash
docker compose --profile demo down -v
```

The `demo` profile is included so these commands also remove `consumer-3` if it
was started during the live-consumer demonstration.

### Other useful commands

Service names, as Compose knows them (use any of these in place of `<service-name>` below):

| Service | What it is |
| --- | --- |
| `bmkg` | Mock BMKG — seismic/tsunami data source |
| `pvmbg` | Mock PVMBG — volcanic data source |
| `aggregator` | Polls both mocks, maps to canonical `HazardEvent`, owns PostgreSQL, publishes to RabbitMQ |
| `auth` | Issues, refreshes, and introspects tokens for the three downstream identities |
| `api` | The public Client-Facing API — the only entrypoint for downstream clients |
| `consumer-1` | Dashboard updater — latest state per hazard |
| `consumer-2` | Notifier — records SIAGA/AWAS alerts |
| `consumer-3` | Regional portal feed — append-only log (`demo` profile only) |
| `postgres` | The Canonical Store, owned solely by the Aggregator |
| `rabbitmq` | The message broker (`hazard.events` fanout exchange) |
| `broker-init` | One-shot: provisions RabbitMQ policies and per-service users before anything else starts |

```bash
docker compose ps                        # status of every service
docker compose logs -f <service-name>     # follow one service's logs
docker compose logs -f api auth aggregator bmkg pvmbg   # follow the main data-flow path
```

## Acceptance Testing (P01–P05)

Run all commands below from the repository root in PowerShell. P01 and P04
create isolated Compose projects; P02, P03, and P05 use the main stack. Make
sure Docker Compose, Python 3.9+, and `bmkg/.env`, `pvmbg/.env`, and
`bnpb/.env` are available.

### Prerequisites

```powershell
Set-Location 'D:\Coding\localrepositories\AsosiasiAdmirerTsundere'

python --version
docker compose version
python -m pip install -r testing/P04/requirements.txt
```

`testing/P04/requirements.txt` installs the validator also required by P01.

### Problem 1 — P01

Run the complete P01 acceptance suite in the same PowerShell session so the
Compose variables and endpoints target the same stack:

```powershell
$env:COMPOSE_PROJECT_NAME = 'aat-p1-' + (Get-Date -Format 'yyyyMMddHHmmss')
$env:COMPOSE_PATH_SEPARATOR = ';'
$env:COMPOSE_FILE = 'compose.yaml;testing/P01/compose.acceptance.yaml'
$env:API_BASE = 'http://localhost:18080'
$env:BMKG_BASE = 'http://localhost:18081'
$env:PVMBG_BASE = 'http://localhost:18082'

docker compose config --quiet
docker compose up -d --build --wait
python testing/P01/run_all.py
docker compose --profile tests down
```

With the P01 stack running, run individual scenarios with:

```powershell
python testing/P01/run_all.py --only T01
python testing/P01/run_all.py --only T02
python testing/P01/T02-runtime-schema-evolution/run.py --deadline 120
python -m unittest discover -s testing/P01 -p test_acceptance.py -v
```

T01 covers mapping and boundary cases; T02 triggers PVMBG schema evolution at
runtime without restarting any container.

### Problem 4 — P04

The P04 runner creates and cleans up an isolated stack for every execution:

```powershell
python -m unittest discover -s testing/P04 -p test_checks.py -v
python testing/P04/run_all.py
```

To run each scenario separately:

```powershell
python testing/P04/run_all.py --only T10
python testing/P04/run_all.py --only T11
python testing/P04/run_all.py --only T12 --deadline 120
```

T10 tests independent rebuilds, T11 Canonical Store isolation, and T12
flexible storage/schema evolution.

### Clean Up the P01/P04 Environment

Run this after P01/P04, especially if a runner stops before its cleanup step:

```powershell
Remove-Item Env:COMPOSE_PROJECT_NAME -ErrorAction SilentlyContinue
Remove-Item Env:COMPOSE_PATH_SEPARATOR -ErrorAction SilentlyContinue
Remove-Item Env:COMPOSE_FILE -ErrorAction SilentlyContinue
Remove-Item Env:COMPOSE_PROFILES -ErrorAction SilentlyContinue
Remove-Item Env:API_BASE -ErrorAction SilentlyContinue
Remove-Item Env:BMKG_BASE -ErrorAction SilentlyContinue
Remove-Item Env:PVMBG_BASE -ErrorAction SilentlyContinue
Remove-Item Env:TEST_RESULTS_ROOT -ErrorAction SilentlyContinue
```

### Main Stack

Before P02, P03, or P05, start the main stack:

```powershell
docker compose config --quiet
docker compose up -d --build
docker compose ps --all
```

Continue only after `broker-init` shows `Exited (0)`.

### Problem 2 — P02

```powershell
python testing/P02/run_all.py
```

To run an individual scenario:

```powershell
python testing/P02/T03-head-of-line/run.py
python testing/P02/T04-sustained-load/run.py
python testing/P02/T05-outage-recovery/run.py
```

T03 tests head-of-line blocking, T04 sustained load, and T05 PVMBG
outage/recovery.

### Problem 3 — P03

```powershell
python testing/P03/run_all.py
```

To run an individual scenario:

```powershell
python testing/P03/T06-cross-domain-credentials/run.py
python testing/P03/T07-media-field-authorization/run.py
python testing/P03/T08-token-expiry-refresh/run.py
python testing/P03/T09-secret-hygiene/run.py
```

T06 tests credential separation, T07 Media field authorization, T08 token
expiry/refresh, and T09 secret hygiene.

### Problem 5 — P05

The P05 demo stops and restarts `consumer-2` and adds `consumer-3` live. Save
the transcript as evidence:

```powershell
$log = "testing/P05/evidence-{0}.log" -f (Get-Date -Format 'yyyyMMdd-HHmmss')
python testing/P05/p5_demo.py 2>&1 | Tee-Object -FilePath $log
```

After all tests finish, shut down the main stack:

```powershell
docker compose --profile demo down
```

The `--profile demo` flag is required here because P5.4 creates `consumer-3`
with that profile. Without it, Compose excludes the profiled service from the
teardown and the `aat_events` network can remain attached.

Runtime evidence is written under `testing/results/P01`–`testing/results/P04`
or to the P05 log file. An exit code of `0` only means that the selected
automated scenario passed. 

## Endpoints

The internal contract between the Aggregator and the Client-Facing API is formally specified as an OpenAPI 3.1 document with JSON Schemas, under `bnpb/contracts/v1/`. Run it locally with Swagger UI:

```bash
docker run --rm -p 8089:8080 \
  -e SWAGGER_JSON=/contract/openapi.yaml \
  -v "$(pwd)/bnpb/contracts/v1:/contract" \
  swaggerapi/swagger-ui
```

Then open `http://localhost:8089` in a browser.

This contract covers the **internal** read API (`GET /internal/hazards`, `GET /internal/hazards/{hazard_id}`) that only the Client-Facing API is allowed to call. The **public** endpoints that downstream clients (Media, Field Team, BNPB Internal Ops) actually use — `POST /auth/login`, `POST /auth/refresh`, `GET /v1/hazards`, `GET /v1/hazards/{hazard_id}` — are documented in prose, with full request/response examples and the Summary/Full field-projection rules, in [`bnpb/README.md`](bnpb/README.md). Each mock source's own endpoints are documented the same way in [`bmkg/README.md`](bmkg/README.md) and [`pvmbg/README.md`](pvmbg/README.md).

There is no graphical interface anywhere in this system — every component, including the three broker consumers, is a headless API driven by `curl`, a script, or the Swagger UI above.

## Creators

<table>
    <tr align="left">
        <td><b>NIM</b></td>
        <td><b>Name</b></td>
        <td align="center"><b>GitHub</b></td>
    </tr>
    <tr align="left">
        <td>13523056</td>
        <td>Salman Hanif</td>
        <td align="center" >
            <div style="margin-right: 20px;">
            <a href="https://github.com/salmaanhaniif" >
                <img src="https://avatars.githubusercontent.com/u/165023067?v=4" width="48px;" alt=""/> 
                <br/> <sub><b> @salmaanhaniif </b></sub>
            </a><br/>
            </div>
        </td>
    </tr>
    <tr align="left">
        <td>13523100</td>
        <td>Aryo Wisanggeni</td>
        <td align="center" >
            <div style="margin-right: 20px;">
            <a href="https://github.com/Staryo40" >
                <img src="https://avatars.githubusercontent.com/u/139449070?v=4" width="48px;" alt=""/> 
                <br/> <sub><b> @Staryo40 </b></sub>
            </a><br/>
            </div>
        </td>
    </tr>
    <tr align="left">
        <td>13523106</td>
        <td>Athian Nugraha Muarajuang</td>
        <td align="center" >
            <div style="margin-right: 20px;">
            <a href="https://github.com/Starath" >
                <img src="https://avatars.githubusercontent.com/u/164960528?v=4" width="48px;" alt=""/> 
                <br/> <sub><b> @Starath </b></sub>
            </a><br/>
            </div>
        </td>
    </tr>
</table>
