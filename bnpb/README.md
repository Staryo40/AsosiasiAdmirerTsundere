# BNPB Services

Folder ini berisi service Go milik BNPB. Client downstream hanya berkomunikasi
dengan Client-Facing API pada port `8080`. Auth (`8084`) dan Aggregator (`8083`)
berada pada network internal Compose.

## Alur Auth dan Read

```text
client -> API /auth/login         -> Auth /internal/auth/login
client -> API /auth/refresh       -> Auth /internal/auth/refresh
client -> API /v1/hazards         -> Auth /internal/auth/introspect
                                  -> Aggregator /internal/hazards
```

API meneruskan `X-Correlation-ID` pada seluruh panggilan internal. API memakai
`X-BNPB-Auth-Key` ke Auth dan `X-BNPB-Internal-Key` ke Aggregator. Kedua kunci
harus berbeda dari kredensial BMKG, PVMBG, dan ketiga client downstream.

## Endpoint Publik

| Method dan path | Fungsi |
| --- | --- |
| `POST /auth/login` | Menukar `client_id` dan `client_secret` dengan pasangan token. |
| `POST /auth/refresh` | Merotasi refresh dan access token tanpa login ulang. |
| `GET /v1/hazards` | Membaca daftar hazard dengan filter dan cursor. |
| `GET /v1/hazards/{hazard_id}` | Membaca satu hazard. |
| `GET /health` | Liveness API, independen dari dependency. |
| `GET /ready` | Ready hanya jika Auth dan Aggregator dapat dijangkau. |

Login:

```json
{"client_id":"media","client_secret":"<secret>"}
```

Respons login dan refresh:

```json
{
  "access_token": "<opaque-token>",
  "token_type": "Bearer",
  "expires_in": 60,
  "refresh_token": "<opaque-token>",
  "refresh_expires_in": 28800
}
```

Token disertakan pada request data:

```http
Authorization: Bearer <access_token>
```

## Token dan Sesi

- Access token dan refresh token adalah nilai acak 256-bit, bukan JWT.
- Auth hanya menyimpan SHA-256 token, identitas, role, dan waktu kedaluwarsa.
- Access token default berlaku 60 detik.
- Refresh session mempunyai batas absolut delapan jam.
- Refresh merotasi kedua token dan langsung mencabut access token sebelumnya.
- Dua refresh bersamaan dengan token yang sama menghasilkan tepat satu sukses.
- Restart Auth menghapus seluruh sesi; client harus login kembali.

Auth menyediakan endpoint internal berikut dan semuanya memerlukan
`X-BNPB-Auth-Key`:

- `POST /internal/auth/login`
- `POST /internal/auth/refresh`
- `POST /internal/auth/introspect`

Auth tidak dipublikasikan ke host oleh Compose.

## Otorisasi Hazard

Parameter API tambahan `view` tidak diteruskan ke Aggregator:

| Role | `view=summary` | `view=full` |
| --- | --- | --- |
| `media` | Diizinkan | 403 |
| `field` | Diizinkan | Diizinkan |
| `ops` | Diizinkan | Diizinkan |

Tanpa `view`, API memakai `summary`. Summary dibentuk dengan allowlist tujuh
field: `hazard_id`, `source`, `hazard_type`, `severity`, `area_name`,
`occurred_at`, dan `ingested_at`. Penambahan field baru pada payload internal
tidak otomatis bocor ke Media.

Filter list yang didukung: `source`, `hazard_type`, `severity`,
`occurred_from`, `occurred_to`, `limit`, dan `cursor`. Parameter asing,
duplikat, kosong, atau tidak konsisten ditolak dengan 400.

## Konfigurasi

Salin `.env.example` menjadi `.env`, lalu isi sedikitnya:

```dotenv
POSTGRES_PASSWORD=<secret>
RABBITMQ_DEFAULT_PASS=<secret>
BNPB_AUTH_INTERNAL_KEY=<random-minimum-32-bytes>
BNPB_AGGREGATOR_INTERNAL_KEY=<random-minimum-32-bytes>
BNPB_MEDIA_CLIENT_SECRET=<random-minimum-32-bytes>
BNPB_FIELD_CLIENT_SECRET=<random-minimum-32-bytes>
BNPB_OPS_CLIENT_SECRET=<random-minimum-32-bytes>
```

TTL dapat diubah dengan:

```dotenv
BNPB_AUTH_ACCESS_TOKEN_TTL=60s
BNPB_AUTH_REFRESH_SESSION_TTL=8h
```

## Menjalankan dan Menguji

```powershell
docker compose up -d --build
docker compose ps
Invoke-RestMethod http://localhost:8080/health
Invoke-RestMethod http://localhost:8080/ready
```

Test Go:

```powershell
go test -race ./internal/auth ./internal/api
go test ./...
go vet ./...
```

Endpoint data memakai Aggregator yang sehat dan kontrak internal pada
`contracts/v1/openapi.yaml`. Untuk menjalankan alur lengkap bersama kedua mock,
gunakan `docker compose up -d --build` dari root repositori. API tidak pernah
mengakses PostgreSQL secara langsung.
