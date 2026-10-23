# Demo P1–P5 — panduan ringkas

Urutan: **persiapan → P1 → P2 → P3 → P4 → P5 → pemulihan**.
Swagger untuk request, psql untuk isi DB, script yang sudah ada untuk load dan
lifecycle. Tidak perlu script/dashboard baru. Semua perintah dari root repo,
shell Bash/Zsh/WSL, proyek utama `aat`. Jalankan skenario bergantian.

## 0. Persiapan sebelum sesi

1. Siapkan Docker, Python 3.9+, `.env` ketiga service, serta dependency
   [P01](../testing/P01/README.md)/[P04](../testing/P04/README.md). Siapkan image
   sebelum sesi; catat commit/tag yang dinilai, perubahan lokal, dan proyek.
   Gunakan terminal tanpa override proyek/endpoint pengujian sebelumnya.
2. Mulai stack, periksa health dan `broker-init` exit 0, buat input lokal:

   ```bash
   rtk proxy docker compose -f compose.yaml -f compose.override.yaml -f compose.swagger.yaml up -d --build
   rtk proxy docker compose --profile demo ps --all
   rtk proxy python3 demo/swagger/generate_demo_copy_paste.py
   ```

3. Buka <http://127.0.0.1:8089> dan [input Swagger](swagger/DEMO-COPY-PASTE.md).
   File `DEMO-COPY-PASTE.local.md` berisi secret; hentikan sharing saat login/
   Authorize. Token BNPB dan PVMBG berbeda; BMKG menggunakan API key.
4. Sepakati baseline: PVMBG V1, outage OFF, batas delay tercatat, consumer-3
   belum aktif. Jika setting pernah diubah, tetapkan baseline eksplisit lewat
   admin; tidak ada GET setting. Jangan restart mock terhadap store lama atau
   reset volume untuk memperoleh baseline bersih; siapkan stack fresh terpisah bila perlu.
5. Swagger: cek health BMKG/PVMBG serta health/readiness BNPB. Simpan bukti
   seed startup ≥20/source, lalu bandingkan respons source berselisih 10 detik
   untuk menunjukkan event baru. Hitungan DB bukan bukti seed source.
6. Buka psql dan jalankan setup read-only dari [runbook SQL §2](PSQL-DEMO-QUERIES.md#2-open-the-operator-session):

   ```bash
   rtk proxy docker compose exec postgres sh -c 'PGOPTIONS="-c default_transaction_read_only=on" exec psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
   ```

Simpan bukti berlabel P1–P5 beserta commit/proyek. Runner P01–P04 menghasilkan
file di `testing/results/`; simpan transkrip/screenshot langkah manual dan P5.
Pertahankan hasil gagal; jangan simpan secret/token. Bagi peran tiap anggota,
sisakan waktu tanya jawab dalam batas sesi 60 menit.

## 1. P1 — mapping dan schema evolution tanpa restart

**Tujuan:** kedua source menjadi HazardEvent dan confidence baru tetap tersimpan.
Gunakan stack utama yang sama sepanjang langkah berikut.

| Urut | Lakukan | Bukti yang ditunjukkan |
| --- | --- | --- |
| 1 | Snapshot container dengan perintah di bawah; psql **Q01/Q02/Q10**. | Identitas awal, JSONB, migrasi dan fingerprint schema. |
| 2 | Swagger BMKG: `/seismic-events`, `/tsunami-warnings`. PVMBG reader/admin: `/volcanic-reports` V1. BNPB Field/Ops: `/v1/hazards?source=BMKG&limit=10&view=full`, lalu `source=PVMBG`. Kosongkan cursor/filter waktu yang tidak dipakai. | Pasangan source→kanonik: ID sumber, severity, koordinat, waktu, attributes. |
| 3 | psql **Q05/Q06** untuk ID tadi. | Warning dikorelasikan lewat `related_event_id`, severity warning mengalahkan magnitude; nama/koordinat gunung dari referensi BNPB. |
| 4 | Pilih report V1 **baru** dan hazard-nya; simpan source/full API. Jalankan **Q07** hingga `witness_outside_overlap = t`, lalu simpan batas waktu toggle. | `old_id` tanpa confidence tersimpan dan aman dari overlap. Lakukan sebelum toggle pada sheet Swagger pendek. |
| 5 | Swagger PVMBG admin: `POST /admin/schema-version`, body `{"version":2}` → **204**. Baca report baru dan tunggu ingest. | Confidence ada di source V2. |
| 6 | psql **Q08/Q09**, simpan `new_id`; baca detail kedua ID lewat BNPB `view=full`. Ulangi **Q10** dan snapshot. | V1 tanpa confidence dan V2 numeric 0–1 terbaca bersama; fingerprint dan seluruh identitas/start/restart tetap sama. |

Snapshot sebelum/sesudah, di shell:

```bash
rtk proxy docker compose ps --all --quiet | rtk proxy xargs docker inspect --format '{{.Name}} {{.Id}} {{.Image}} {{.State.StartedAt}} {{.RestartCount}}'
```

**Simpan bukti saat V2 aktif untuk P4**, lalu pulihkan schema baseline; restore
V1 dapat menghilangkan confidence pada row yang masih masuk overlap. Jika V2 belum masuk,
periksa **Q11/Q18**. Jelaskan extra masuk `attributes`, benturan nama ke
`source_extras`, dan perubahan tipe/field inti tidak valid tetap ditolak.

## 2. P2 — sumber lambat, load, outage/recovery

Jalankan satu per satu. Script mengendalikan setter; jangan toggle manual saat
runner berjalan. Catat baseline delay; bila berbeda dari `.env`, tambahkan
`--restore-delay-min` dan `--restore-delay-max` yang sesuai pada T03/T04.

```bash
rtk proxy python3 testing/P02/T03-head-of-line/run.py
rtk proxy python3 testing/P02/T04-sustained-load/run.py
rtk proxy python3 testing/P02/T05-outage-recovery/run.py
```

| Tahap | Tunjukkan |
| --- | --- |
| T03 | PVMBG benar-benar 3000 ms; BMKG-only p95 **<300 ms** dan ingest BMKG baru maju. |
| T04 | **≥50 koneksi selama ≥60 detik**; throughput, p50/p95/p99, unexpected error **<1%**, 429 terpisah, tidak crash/restart. Default measurement 65 detik di luar warm-up. |
| T05 | Selama outage, Swagger BNPB: baca `source=BMKG`, lalu `source=PVMBG`, dengan `limit=10&view=full`. BMKG tersedia; PVMBG memakai cache dengan `sources.PVMBG.status/stale_since`. Setelah recovery: sehat dan data baru tanpa restart. |

T05 default outage 60 detik. Jika penguji meminta 10–20 menit, **ganti** perintah
T05 dengan `rtk proxy python3 testing/P02/T05-outage-recovery/run.py --outage-seconds 600`
atau `1200`. Endpoint admin `/admin/outage` tetap siap untuk arahan manual penguji.

Tampilkan `result.json`, `connections.json`, dan `outage-observations.json` dari
folder runner. **Q11/Q12** mendukung penjelasan polling/ingest; hindari scan DB
berulang saat load. Durasi satu request Swagger bukan p95. Pastikan delay pulih
ke baseline dan outage OFF. Detail: [P02](../testing/P02/README.md).

## 3. P3 — trust domain, role, expiry/refresh

```bash
rtk proxy python3 testing/P03/T06-cross-domain-credentials/run.py
rtk proxy python3 testing/P03/T07-media-field-authorization/run.py
rtk proxy python3 testing/P03/T08-token-expiry-refresh/run.py
```

| Tahap | Tunjukkan |
| --- | --- |
| T06 | Kredensial diterima source asal, ditolak **401/403** pada source lain, dua arah. |
| T07 + Swagger | Media summary **200**, full **403**; Field/Ops full **200**. Tambahkan manual `GET /v1/hazards/{hazard_id}`: Media summary/full, lalu Field/Ops full. T07 hanya memeriksa list. |
| T08 | TTL default **60 detik**, expiry alami → **401**, refresh tanpa login kedua, token baru **200**, access/refresh lama **401**. Tampilkan timeline/hash script. |

JSON Media hanya: `hazard_id`, `source`, `hazard_type`, `severity`, `area_name`,
`occurred_at`, `ingested_at`; tanpa `source_ref_id`, koordinat, `attributes`.
Ganti Authorize ketika berganti role. Tidak perlu plugin auto-refresh Swagger.

Review secret **sebelum sesi**, simpan hasil redaksi dan tunjukkan `.env.example`:

```bash
rtk proxy python3 testing/P03/T09-secret-hygiene/run.py
```

T09 membaca secret aktif dan histori Git; pencarian literal secret aktif bukan
audit lengkap. Detail: [P03](../testing/P03/README.md).

## 4. P4 — deployment independen dan ownership storage

Tampilkan bukti tersimpan **hasil Q02/Q09/Q10 dari P1**: JSONB, dua row/API V1–V2 terbaca,
tanpa migrasi. Jalankan suite sekali:

```bash
rtk proxy python3 testing/P04/run_all.py --deadline 120
```

| Hasil | Tunjukkan |
| --- | --- |
| T10 | Consumer-1 stop→build→recreate; API tetap **200**, peer tetap, volume consumer sama. |
| T11 | Aggregator bisa akses DB melalui DNS/IP; non-owner tidak; DB tanpa host port. Trace correlation ID menunjukkan API→Aggregator→store. |
| T12 | Suite juga memverifikasi V1/V2 dan schema storage tetap secara otomatis. |

**P04 membuat stack terisolasi `aat-p4-…` dan port dinamis**, kemudian teardown
mempertahankan volume. Tampilkan JSON/output proyek itu; Swagger/psql utama
menunjuk `aat`. Jelaskan batas service dan ≥2 alternatif storage, misalnya
PostgreSQL+JSONB vs document store. Detail: [P04](../testing/P04/README.md).

Untuk trace langsung di stack utama, kirim satu read Swagger dengan header
`X-Correlation-ID: demo-p4-read`, lalu tampilkan log yang sama beserta timing:

```bash
rtk proxy docker compose logs --since 1m --no-color api auth aggregator | rtk proxy rg -F 'demo-p4-read'
```

## 5. P5 — fanout, consumer recovery, subscriber baru

Kembali ke stack utama; consumer-3 belum aktif. Tidak perlu endpoint baru.

1. Simpan identitas producer; buka log publikasi di terminal terpisah:

   ```bash
   rtk proxy docker inspect --format '{{.Id}} {{.Image}} {{.State.StartedAt}} {{.RestartCount}}' aat-aggregator-1
   rtk proxy docker compose logs -f --since 30s aggregator
   ```

2. Di terminal lain, jalankan script dan simpan transkrip terminal:

   ```bash
   rtk proxy python3 -u testing/P05/p5_demo.py --project aat --repo-dir .
   ```

   Tunjukkan consumer-1/2 maju; consumer-2 dihentikan, queue bertambah dan
   consumer-1 tetap maju; consumer-2 pulih dan backlog habis; consumer-3
   ditambahkan. **Q13–Q17** menunjukkan sisi producer. Backlog consumer ada
   di RabbitMQ; counter saja belum membuktikan pesan yang sama.

3. Sesudah script selesai, pastikan log consumer-3 memuat `broker_connected`:

   ```bash
   rtk proxy docker compose logs --since 5m consumer-3
   ```

   **Setelah** koneksi terlihat, di psql simpan batas waktu; tunggu event baru
   lalu pilih pesan yang dibuat dan dipublikasikan setelah batas tersebut:

   ```sql
   SELECT clock_timestamp() AS joined_at \gset

   SELECT message_id, hazard_id, revision, correlation_id, created_at, sent_at
   FROM outbox
   WHERE created_at > :'joined_at'::timestamptz
     AND sent_at IS NOT NULL
     AND payload->>'event_type' = 'hazard.created'
   ORDER BY sequence LIMIT 1;
   ```

   Jika kosong, tunggu dan ulangi SELECT kedua. Salin `message_id`, lalu:

   ```bash
   rtk proxy docker compose logs --since 5m --no-color --no-log-prefix aggregator consumer-1 consumer-2 consumer-3 | rtk proxy rg -F 'PASTE_MESSAGE_ID'
   ```

   Tunjukkan publikasi sukses (INFO, category kosong) dan `message_received`
   untuk ID sama di **ketiga consumer**. Filter ulang memakai `correlation_id`
   untuk melihat pemrosesan hazard; Swagger dapat menampilkan detail hazard itu.

4. Ulangi inspect producer dari langkah 1: ID, image, start time, restart count
   sama; log menunjukkan publikasi berlanjut. Jelaskan at-least-once, dedupe,
   serta kebijakan backlog. Subscriber baru tidak dijanjikan histori sebelum
   queue terikat. Detail: [P05](../testing/P05/README.md).

## 6. Tutup dan pulihkan

Pastikan outage OFF, schema/delay kembali ke baseline, consumer-2 aktif; catat
apakah consumer-3 dipertahankan atau dihentikan untuk sesi berikutnya. Periksa
health/readiness dan ingest baru. Simpan bukti sebelum menutup terminal.
Jangan gunakan `down -v` atau restart seluruh stack untuk menyamarkan kegagalan.

**YAGNI:** detail-role dan pencocokan message cukup manual. Perluas T07/P05
hanya jika ingin otomatisasi; tidak ada script baru yang wajib dibuat.
Rujukan: [checklist](../CHECKLIST-DEMO.md), [SQL](PSQL-DEMO-QUERIES.md),
[Swagger](swagger/README.md). Panduan ini bukan bukti bahwa skenario sudah PASS.
