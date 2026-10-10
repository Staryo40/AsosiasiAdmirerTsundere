# P1 — Interoperabilitas data

Suite ini menguji bagian **4. P1 — Interoperabilitas data** dalam checklist kriteria penyelesaian Problem 1 pada spesifikasi.

## Menjalankan

Dari root repository, gunakan Python 3.9+, Docker Compose, Git, serta berkas `.env` ketiga instansi. Pasang validator yang juga dipakai workflow kontrak:

```powershell
python -m pip install -r testing/P01/requirements.txt
python testing/P01/run_all.py
```

Untuk menghindari data lama atau mengganggu demo lain, test menggunakan proyek terisolasi. seluruh blok dijalankan dalam sesi PowerShell yang sama:

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

Overlay memakai port loopback 18080–18082 dan enam subnet 10.251.230.0/24 sampai 10.251.235.0/24. Pastikan subnet/port bebas; sesuaikan overlay bila bertabrakan. Gunakan satu proyek P1 terisolasi pada satu waktu. `down` mempertahankan volume; gunakan nama proyek baru bila mock sudah pernah di-restart. Jangan menghapus volume stack kerja untuk mengatasi kegagalan acceptance.

Pada Bash, gunakan separator `:` untuk `COMPOSE_FILE`. Variabel endpoint dan Compose harus menunjuk ke **stack yang sama**, karena identitas container dan metadata schema diambil dari proyek Compose aktif.

Untuk menjalankan satu bagian:

```powershell
python testing/P01/run_all.py --only T01
python testing/P01/run_all.py --only T02
python testing/P01/T02-runtime-schema-evolution/run.py --deadline 120
python -m unittest discover -s testing/P01 -p test_acceptance.py -v
```

`--deadline` membatasi tiap observasi polling; bukan durasi total suite. Default 45 detik. T02 menunggu record baru V1, checkpoint melampaui overlap, kemudian record baru V2. Interval generator/polling yang besar memerlukan deadline lebih panjang. Token OPS di-refresh otomatis; token reader/admin PVMBG diperbarui bila menerima 401.

## Cakupan dan bukti

| Checklist | Pengujian | Bukti / batas |
| --- | --- | --- |
| P1-01 | T01 golden fixtures; T02 mapping kedua sumber | Seluruh sebelas field, enum, koordinat, waktu UTC, attributes; validasi JSON Schema 2020-12 dengan assertion `date-time` |
| P1-02 | T01 korelasi warning; T02 GET endpoint seismik dan warning terpisah | `related_event_id`, severity warning menggantikan magnitude, bukan nilai maksimum keduanya |
| P1-03–04 | T02 V1 → V2 melalui POST admin | Payload sumber/kanonik sebelum dan sesudah; ID, image, started_at, restart_count seluruh container berjalan tetap sama |
| P1-04; P4-03 | T02 fingerprint | Hash berkas sumber, kolom storage publik, dan histori migrasi sebelum/sesudah tetap sama. Schema probe dilakukan operator di container PostgreSQL; client tetap membaca lewat API |
| P1-05 | T01 fixture additive; T02 confidence | Field tambahan masuk `attributes`; nol berbeda dari absen; JSON bersarang, null pada extra, integer >2^53 dan benturan nama cadangan |
| P1-06 | Manual | Gunakan mekanisme, alternatif, batasan, dan bukti di bawah sebagai bahan laporan; suite tidak menyatakan PDF final telah lengkap |
| P1-07 | T01 boundary table | 4.99/5.0/6.49/6.5; desimal tepat di bawah ambang; tiga warning level; empat alert level; confidence absen/0/1 |
| P1-08 | T01 unit dan PostgreSQL | Field inti hilang/null/object, confidence tidak sah, enum asing, timestamp tidak sah, JSON rusak/duplicate key; record ditolak masuk `ingest_rejects`, record valid dalam batch tetap tersimpan |
| P1-09 | T01 PostgreSQL | Poll ulang tidak menambah hazard/revisi; ID sumber sama pada dua instansi menghasilkan hazard berbeda |
| P1-10 | T01 cursor dan storage | First poll tanpa `since`, overlap UTC, timestamp sama diurutkan ID; timestamp invalid menahan checkpoint; warning masa depan tidak mendorong cursor melewati waktu poll |
| P1-11 | T01 PostgreSQL | Warning sebelum/sesudah event; hazard_id dan receipt pertama tetap; revisi sah memakai message_id berbeda; ulang warning tidak menambah revisi |
| P1-12 | T01; inspeksi kontrak saat riset | Warning memakai `issued_at`, bukan `estimated_arrival`. BMKG/PVMBG memakai `since` eksklusif; jaminan penanganan tie diuji di Aggregator, bukan sertifikasi seluruh filter endpoint mock |
| P1-13 | T01 | Warning Awas terkait menang, warning tidak terkait diabaikan; gunung asing ditolak tanpa koordinat buatan |

T01 menjalankan 13 kelompok test Go, termasuk lima test integrasi, dan lima test untuk memeriksa ketepatan oracle Python. Hilangnya test wajib, skip, atau exit non-zero membuat suite gagal. Unit/integration test memanggil kode pemilik Aggregator; test network polling juga melewati HTTP fixture. T02 membuktikan jalur jaringan mock nyata → Aggregator → PostgreSQL → Client-Facing API. Keduanya diperlukan; unit test saja tidak membuktikan demo live.

T02 mempertahankan record V1 yang sudah berada di luar overlap polling sebelum toggle. Record yang masih berada di dalam overlap boleh diperkaya ketika sumber menyajikannya kembali sebagai V2; itu bukan kegagalan schema evolution. Test integrasi deterministik menguji V1/V2 tersimpan bersamaan dan extra bertingkat setelah round-trip JSONB/API.

## Hasil

Exit `0` berarti bagian yang dijalankan lulus; exit non-zero berarti gagal. Bukti berada di `testing/results/P01/<timestamp>/` atau direktori `TEST_RESULTS_ROOT`:

- `T01.json`: kelompok/subtest, exit code, konteks database test, dan hasil oracle.
- `T01-go.ndjson`: keluaran mentah `go test -json`.
- `T02.json`: payload, toggle timestamp, versi skema asal/pemulihan, snapshot container, fingerprint sumber/storage, serta kegagalan jika ada.
- `summary.json`: outcome tiap bagian, commit dan kewajiban laporan manual.

Runner mencatat working tree kotor secara eksplisit; hash commit saja tidak membuktikan kode lokal identik dengan commit. ID image merekam container aktual; fingerprint berkas lokal tidak membuktikan build image dari berkas tersebut. Untuk bukti resmi, lakukan build pada checkout yang dicatat sebelum baseline. Respons tidak memuat token/password; seluruh environment container hanya dibaca dalam memori untuk konfigurasi yang diperlukan dan tidak ditulis ke bukti. Tinjau unknown payload sebelum membagikan evidence.

Kegagalan awal pada store lama harus dipertahankan. Bila mock di-restart dengan ID dari awal sementara store persisten mempertahankan ID lama, event baru dikarantina sebagai `source_identity_reused`; endpoint dapat terlihat healthy tanpa data baru. Harness menunggu **record baru yang benar-benar tersimpan**, sehingga tidak meluluskan cache lama. Gunakan stack baru untuk menguji skenario startup → toggle; pemulihan ID sesudah restart mock adalah batasan implementasi saat ini.

## Bahan laporan P1-05–06

Mekanisme yang diuji adalah JSON additive-only dengan validasi inti terpisah, mapping kanonik, serta typed-core + JSONB di Canonical Store milik Aggregator. Field tambahan tanpa padanan dipertahankan dalam `attributes`. Benturan `source_extras`, serta `tsunami_warnings` pada seismik, diberi namespace `attributes.source_extras`. Severity mengikuti warning terkait tingkat tertinggi; hanya tanpa warning memakai batas magnitude. Nama/koordinat gunung berasal dari referensi statis BNPB.

Didukung: tambahan field JSON yang valid, termasuk objek/array bertingkat, coexistence V1/V2, confidence absen atau angka 0–1, polling ulang, dan rekonsiliasi warning terlambat. Tidak didukung: field wajib hilang, tipe berubah, enum asing, confidence null/tidak sah, JSON rusak/duplicate key, volcano_id asing, serta source ID dipakai ulang untuk kejadian berbeda. Reject disimpan sebagai metadata/fingerprint; record tanpa waktu yang dapat dipakai menahan checkpoint. Warning menggunakan metadata aditif `issued_at`; full reconciliation periodik melengkapi polling overlap. Perubahan severity menerbitkan revisi dengan hazard_id tetap dan message_id baru.

JSONB memberi storage fleksibel, tetapi validitas JSON tidak menjamin validitas HazardEvent. Alternatifnya adalah schema registry atau kontrak Avro/Protobuf dengan aturan compatibility terpusat; pilihan tersebut menambah tooling dan koordinasi yang tidak diwajibkan PoC M1. Batasan JSONB termasuk representasi angka, Unicode tertentu, dan tidak mempertahankan urutan key/whitespace; test membandingkan makna JSON, bukan byte keluaran database. Suite ini tidak menguji seluruh batas numeric PostgreSQL atau seluruh crash window transaksi/outbox.

## Dasar riset

Riset Exa memakai tiga pencarian dengan masing-masing lima hasil (15 hasil pencarian, bukan 15 sumber independen); tiga sumber primer dibaca lagi. Panduan repository meminta kerja berurutan, sehingga riset dilakukan tanpa subagen.

| Sumber primer | Keputusan pengujian |
| --- | --- |
| [JSON Schema object](https://json-schema.org/understanding-json-schema/reference/object) | Required/type dipisahkan dari unknown properties; null berbeda dari field yang absen |
| [PostgreSQL 18 JSON types](https://www.postgresql.org/docs/18/datatype-json.html) | Round-trip JSONB harus diperiksa; urutan key dan representasi tekstual bukan oracle; uji integer besar |
| [Go subtests](https://go.dev/blog/subtests) | Named table-driven cases agar kegagalan batas mapping dapat ditelusuri |
| [jsonschema 4.25.1 validation](https://python-jsonschema.readthedocs.io/en/v4.25.1/validate/) | Validator draft yang tepat dan format checker aktif; harness menolak startup jika checker date-time tidak tersedia |

Context7 `resolve-library-id` → `query-docs` digunakan untuk Python standard library, Go testing/JSON/HTTP, jsonschema 4.25.1, pgx 5.10.0, dan Docker Compose. Dokumentasi Python 3.9.22 dipakai untuk batas kompatibilitas runner; verifikasi aktual menggunakan Python 3.13.5. Context7 Go 1.26.0 tersedia; toolchain proyek 1.26.8 diverifikasi melalui kode/API terpasang dan test. Pin mengikuti `bnpb/go.mod`, bukan catatan versi lama dalam dokumen desain.

Implementasi pengujian ini dibuat dengan bantuan LLM; reviewer kelompok tetap perlu memahami assertion, menjalankan suite, dan memasukkan deklarasi/form AI ke laporan sesuai ketentuan tugas.
