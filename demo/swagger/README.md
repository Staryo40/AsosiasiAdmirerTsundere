# Local Swagger Demo

This optional package serves the official Swagger UI distribution and three
source-specific OpenAPI documents through one loopback-only origin:

    http://127.0.0.1:8089

It is team-operated tooling for the main stack. It does not change backend
behavior, the assessed milestone tag, source credentials, or the terminal
tools used for database, load, log, container, and broker evidence.

For the shortest demo flow, use [Demo Inputs — Copy/Paste into Swagger](DEMO-COPY-PASTE.md).
The [complete request playbook](REQUEST-PLAYBOOK.md) catalogs all operations
and parameters. Both complement the runbook here and the three OpenAPI
contracts; neither replaces the existing acceptance tools.

Fill the demo sheet from the active service credentials by running:

    python demo/swagger/generate_demo_copy_paste.py

Open `demo/swagger/DEMO-COPY-PASTE.local.md` afterward. It is ignored by Git
and excluded from the Docker build context. It contains credentials; keep it
local and regenerate it after changing the service `.env` files.

## Start and stop

From the repository root, validate and start the full main stack with the
existing override and this optional overlay:

    docker compose -f compose.yaml -f compose.override.yaml -f compose.swagger.yaml config --quiet
    docker compose -f compose.yaml -f compose.override.yaml -f compose.swagger.yaml up -d

When the main stack is already running, build and start only the demo service:

    docker compose -f compose.yaml -f compose.override.yaml -f compose.swagger.yaml up -d --build --no-deps swagger-demo

Then open http://127.0.0.1:8089. The static UI and its local specifications do
not wait for the business services. If an upstream is stopped, its request
shows a gateway transport failure until that service is available again.

Port 8089 is also used by the standalone Swagger example in the root README.
If it is occupied, change only the host side of the port mapping in
compose.swagger.yaml, for example from 8089 to 8090. Keep the 8080 container
port and relative OpenAPI server URLs unchanged, then rerun the service-only
command. Check local host firewalls and trust before sharing the machine:
the existing API and source port mappings on 8080-8082 are wildcard-bound, so
the new loopback mapping does not make the whole stack loopback-only.

Stop only this optional service with:

    docker compose -f compose.yaml -f compose.override.yaml -f compose.swagger.yaml stop swagger-demo

Do not use volume deletion as demo cleanup or recovery.

The gateway-only health endpoint is GET /healthz. A 200 there means that
NGINX is serving the demo package; it says nothing about BNPB, BMKG, or PVMBG
readiness. The three API documents are named BNPB Public, BMKG Source, and
PVMBG Source and Admin. BNPB Public is selected at page load.

## Credentials and screen sharing

The gateway receives no credentials from Compose and has no environment file,
secret mount, Docker socket, or persistent browser store. Obtain credentials
through the existing local configuration and service flows. Never copy
credentials into these specifications, image files, URLs, or this runbook.

Stock Swagger UI can show login bodies and responses, the Authorize dialog,
generated cURL commands, and request headers. The writeOnly schema annotation
does not mask a JSON body. Pause screen sharing for login and authorization,
or use a rehearsed response-only viewport. If a step cannot be presented
safely, use the existing sanitized terminal output. Do not export an
unsanitized HAR or network trace. Clear authorizations, sensitive request
bodies, and response panels at the end, then close the demo browser session.

Use only the token string in the intended Authorize field; Swagger adds the
Bearer prefix. Use BnpbAccessToken for BNPB, BmkgApiKey for the X-BMKG-Key
header, and PvmbgAccessToken for PVMBG. The two bearer tokens are different
trust domains even though both use the Authorization header. Clear or replace
authorization deliberately when changing identity or document.

Every document exposes the optional X-Correlation-ID request header. Use a
nonsecret identifier such as demo-p1-v2-01; the backend echoes or generates
the response header. Swagger's displayed duration is the browser-observed
request duration in milliseconds, including browser and gateway work. It is
not the sustained-load measurement or a backend latency guarantee.

## Scenario boundaries

For each task, record the actual action and input, the expected behavior, the
observable response or terminal evidence, and the recovery. Use the existing
P01-P05 guides and scenario matrix rather than running every suite as a single
demo plan.

- P1 mapping and live schema evolution: read a manageable BMKG or PVMBG source
  record and the public BNPB projection. Capture the before state, set PVMBG
  schema version 2 only after recording the intended baseline, wait for new
  ingestion, and inspect canonical attributes through P01/T01-T02 and the
  owner-aware database tools. A 204 from the setter acknowledges the request;
  it does not prove an observed setting or a newly ingested record.
- P2 slow-source isolation: set PVMBG min_ms and max_ms to 3000, then show
  ordinary requests while the existing P02/T03 load tool measures BMKG-only
  p95. Restore the previously agreed delay bounds. A fast Swagger request is
  not the percentile proof.
- P2 outage and recovery: enable the PVMBG source outage, observe its actual
  503 while the container remains alive, then disable it and verify recovery.
  Do not confuse that response with a gateway 502/504 from a stopped service.
- P3 trust and roles: use the correctly assigned identity, then demonstrate
  the actual backend rejection for a credential from another trust domain.
  Media full access should return 403; Field Team and Internal Ops are tested
  under their own configured identities. P03/T08 remains the proof for
  natural expiry, refresh rotation, and old-token rejection.
- P4 and P5: continue using the existing owner-aware lifecycle, storage,
  broker, and consumer tools. The gateway is not connected to Canonical Store
  or events and adds no consumer controls or receipt endpoint.

There is no GET endpoint for the current PVMBG schema version, outage flag,
or delay range. If the live setting is unknown, establish an explicit
baseline before a mutation. Do not invent a previous value, run a manual
setter alongside a script that owns the same setting, restart source mocks as
a generic reset, or use volume deletion for recovery. BMKG/PVMBG source
restarts reseed their in-memory data; the Canonical Store may persist.

For isolated P01/P04 runs, do not point this main-stack gateway at an isolated
runner implicitly. A separately recorded port and target configuration is
required before using the browser as evidence for another stack.

## Gateway behavior

- NGINX joins only backend, bmkg-source, and pvmbg-source. It cannot reach the
  Canonical Store, events broker, or test-store network.
- Only the method/path families in the three specifications are proxied.
  Unknown API methods and paths return 404 and never fall through to the UI.
- The service prefix is stripped once. Query strings, request bodies,
  Content-Type, the intended credential header, and X-Correlation-ID are
  forwarded. The upstream status, body, content type, X-Correlation-ID, and
  Retry-After are not intercepted or replaced.
- Connect timeout is 2 seconds; send/read inactivity timeouts are 15 seconds.
  These are demo transport settings, not total deadlines. They allow the
  required 3-second PVMBG delay.
- API caching, proxy retries, response interception, gateway rate limits,
  request access logs, error log output, and debug logging are disabled.
  This avoids storing bodies, credentials, and query strings in container
  logs. Diagnose request discrepancies against the direct service and its
  logs. A gateway 502/504 is a transport failure, not an upstream business
  response.
- Docker DNS is re-resolved every 10 seconds through the container resolver.
  Upstream groups use shared memory and do not use fixed container addresses.
  After an upstream container is recreated, retry after its service record is
  available; do not restart the gateway just to refresh DNS.

## Pinned assets

Pins selected on 22 October 2026:

| Artifact | Pin |
| --- | --- |
| NGINX Open Source | Stable 1.30.6, Alpine 3.24, official image index digest SHA-256 c59d1f8b1e0e42da75be973424cd72e23f7f8adfb54e982f4d1cd0216a5a5576 |
| Swagger UI | Official swagger-ui-dist 5.34.0, released 19 October 2026 |
| Swagger UI npm tarball | SHA-256 15cdd55151558cf29ee70fb485332251a9911ebe27414b243ddd10fef9942394; npm SHA-512 integrity H6tAIjr3XqE31PbkZl0OFya1ziRLDfTwh3J6OHdumPtPCLfjTnyLeagl/55kFNZcTOz9a9QI9WP0FXldwiGJag== |

The Dockerfile pins the NGINX multi-platform image digest and verifies the
official npm tarball with Dockerfile ADD checksum support before extraction.
The running image contains NGINX and static files only; it has no Node
runtime. No CDN, external fonts, analytics, or online specification
validator is used. Authorization persistence is disabled; that setting is
not credential masking.

## Source references

- [NGINX upstream module](https://nginx.org/en/docs/http/ngx_http_upstream_module.html) and [proxy module](https://nginx.org/en/docs/http/ngx_http_proxy_module.html)
- [Swagger UI configuration](https://swagger.io/docs/open-source-tools/swagger-ui/usage/configuration/), [v5.34.0 release](https://github.com/swagger-api/swagger-ui/releases/tag/v5.34.0), and [swagger-ui-dist 5.34.0 package](https://www.npmjs.com/package/swagger-ui-dist/v/5.34.0)
- [Docker Compose file merging](https://docs.docker.com/compose/how-tos/multiple-compose-files/merge/) and [Dockerfile ADD checksum](https://docs.docker.com/reference/dockerfile/#add---checksum)
- [OpenAPI Specification 3.0.3](https://spec.openapis.org/oas/v3.0.3.html)
- [M1 Swagger demo design and acceptance record](../../docs/m1-swagger-demo-design.md)
