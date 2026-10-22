# Demo Inputs — Copy/Paste into Swagger

This tracked file is the safe template. Run
`python demo/swagger/generate_demo_copy_paste.py` to create
`demo/swagger/DEMO-COPY-PASTE.local.md` with credentials from the active local
environment files. That generated file is ignored by Git and contains
secrets; keep it local. Login tokens and a hazard ID still come from live
responses.

> The generated login bodies and BMKG key are sensitive. Pause screen sharing
> while copying them or authorizing Swagger; do not show the generated file to
> the audience.

Open Swagger at `http://127.0.0.1:8089`, choose the matching API document, and
use these operation inputs. **GET requests have no body.**

## 1. Log in and read source data

### BMKG

In **Authorize → `BmkgApiKey`**, paste this configured key:

{{INLINE:BMKG_API_KEY}}

Run `GET /seismic-events` and `GET /tsunami-warnings` with all parameters
blank and no body.

### PVMBG reader

Run `POST /auth/login`:

```json
{
  "client_id": "{{PVMBG_READER_CLIENT_ID}}",
  "client_secret": "{{PVMBG_READER_CLIENT_SECRET}}"
}
```

Copy `access_token` from the response. In **Authorize → `PvmbgAccessToken`**,
paste only that token. Run `GET /volcanic-reports` with `since` blank and no
body.

### BNPB Media summary

Run `POST /auth/login`:

```json
{
  "client_id": "{{BNPB_MEDIA_CLIENT_ID}}",
  "client_secret": "{{BNPB_MEDIA_CLIENT_SECRET}}"
}
```

Copy `access_token`. In **Authorize → `BnpbAccessToken`**, paste only the
token. Run `GET /v1/hazards` with these parameter values:

| Name | Value |
| --- | --- |
| `source` | `PVMBG` |
| `hazard_type` | `VOLCANIC` |
| `severity` | *(blank)* |
| `occurred_from` | *(blank)* |
| `occurred_to` | *(blank)* |
| `limit` | `10` |
| `cursor` | *(blank)* |
| `view` | `summary` |

Expected: `200`. Copy a real `hazard_id` from the response. Run
`GET /v1/hazards/{hazard_id}` with that copied path value and `view=summary`.

## 2. P1 — PVMBG schema V1 → V2

Run `POST /auth/login` on the PVMBG document:

```json
{
  "client_id": "{{PVMBG_ADMIN_CLIENT_ID}}",
  "client_secret": "{{PVMBG_ADMIN_CLIENT_SECRET}}"
}
```

Authorize with the returned access token. Run `POST /admin/schema-version`:

```json
{
  "version": 1
}
```

Read `GET /volcanic-reports`; version 1 omits `confidence_level`. Then run
`POST /admin/schema-version` with:

```json
{
  "version": 2
}
```

Read reports again; version 2 includes `confidence_level` from 0 to 1. Wait
for ingestion, then rerun the BNPB list above with a Field Team identity and
`view=full`. Swagger does not prove ingestion; use P01/T02 for stored
canonical evidence. Restore the schema version recorded before the demo.

## 3. P2 — delay and outage

With the PVMBG admin token, run `POST /admin/response-delay`:

```json
{
  "min_ms": 3000,
  "max_ms": 3000
}
```

Run `GET /volcanic-reports` and `GET /seismic-events` (BMKG key still
authorized) to show PVMBG delay versus BMKG. P02/T03 supplies the latency
measurement. Restore configured startup bounds only if the runtime still
matches them:

```json
{
  "min_ms": {{PVMBG_RESPONSE_DELAY_MIN_MS}},
  "max_ms": {{PVMBG_RESPONSE_DELAY_MAX_MS}}
}
```

If an admin setter already changed the in-memory values, restore the recorded
live baseline instead.

Run `POST /admin/outage`:

```json
{
  "enabled": true
}
```

Run PVMBG `GET /volcanic-reports` and `GET /health`; expect `503`. Recover:

```json
{
  "enabled": false
}
```

Retry both GET requests; expect `200`. P02/T05 proves recovery and container
continuity. Accepted setters return `204` with an empty body.

## 4. P3 — Media denial and Field/Ops access

With the Media token authorized, run `GET /v1/hazards` with the section 1
parameters and set `view=full`. Expected: `403`.

Login with the configured Field Team identity:

```json
{
  "client_id": "{{BNPB_FIELD_CLIENT_ID}}",
  "client_secret": "{{BNPB_FIELD_CLIENT_SECRET}}"
}
```

Authorize with its returned token; repeat the same list with `view=full`.
Expected: `200`. Internal Ops can also read full views:

```json
{
  "client_id": "{{BNPB_OPS_CLIENT_ID}}",
  "client_secret": "{{BNPB_OPS_CLIENT_SECRET}}"
}
```

## 5. P3 — refresh after natural expiry

Login once with the Field Team body above. Save its tokens privately and run
the full-view list; expect `200`. Wait the returned `expires_in` (normally 60
seconds), then repeat with the old access token; expect `401`. Run
`POST /auth/refresh`:

```json
{
  "refresh_token": "<OLD_REFRESH_TOKEN>"
}
```

Authorize with the returned new access token and repeat the list; expect
`200`. Replaying the old access token and old refresh token should each return
`401`. P03/T08 is the primary evidence for this sequence.

## P4 and P5

Swagger has no service lifecycle, storage ownership, producer, broker, or
consumer controls. Use the existing P04/P05 procedures; the BNPB hazard list
can provide event context.
