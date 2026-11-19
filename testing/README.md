# Acceptance Testing

Folder ini berisi pengujian penerimaan yang dapat diulang. Unit dan integration test tetap berada di folder service masing-masing.

## Struktur

```text
testing/
|-- common/                         # Modul Python bersama
|-- P02/                            # T03, T04, T05: konkurensi/ketersediaan
|-- P03/
|   |-- run_all.py                  # Orchestrator seluruh pengujian P3
|   |-- T06-cross-domain-credentials/
|   |-- T07-media-field-authorization/
|   |-- T08-token-expiry-refresh/
|   `-- T09-secret-hygiene/
`-- results/                        # Output lokal; diabaikan Git
```

## Aturan

- Jalankan script dari root repository dengan Python 3.9 atau lebih baru.
- Secret dibaca dari `bmkg/.env`, `pvmbg/.env`, dan `bnpb/.env`.
- Jangan menulis token, password, API key, atau internal key mentah ke hasil.
- Setiap skenario harus menghasilkan exit code `0` saat lulus dan non-zero saat
  gagal.
- Hasil mentah masuk `testing/results/`. Bukti yang hendak dikomit harus
  disanitasi dan ditinjau secara manual terlebih dahulu.
- Script yang mengubah delay, outage, atau konfigurasi runtime wajib memulihkan
  state melalui blok `finally`.

## Prasyarat

```text
docker compose up -d
docker compose ps --all
```

Jalankan seluruh pengujian P3 dengan:

```text
python testing/P03/run_all.py
```

T08 menunggu access token kedaluwarsa secara alami, sehingga suite memerlukan
sedikitnya sekitar satu menit pada TTL default 60 detik.

Runner hanya membutuhkan Python 3.9+ dan Git. Library HTTP dan JSON memakai
Python standard library sehingga tidak membutuhkan `requests`, Bash, atau jq.

Untuk P02 jalankan `python testing/P02/run_all.py`. Suite ini juga membutuhkan
Docker CLI untuk memeriksa container dan versi tooling. Panduan metode load,
injeksi outage/delay, cleanup, dan evidence tersedia di [P02/README.md](P02/README.md).

## Bukti deployment sharding, polling, katalog, dan delivery

Empat collector `evidence_*.py` di folder ini menggunakan Python 3.9+ tanpa paket tambahan. Jalankan dari root aplikasi. Collector hanya membaca endpoint, inventaris Kubernetes, atau hasil pengamatan; collector tidak memicu fault, mengisi antrean, menjalankan replay, atau menulis katalog.

Tulis setiap laporan ke path baru di `testing/results/`. Exit `0` berarti semua pemeriksaan mekanis collector lulus untuk input tersebut; exit `1` berarti bukti tidak lengkap atau gagal. Input log yang dinormalisasi harus berasal dari pengamatan aktual, menggunakan timestamp UTC dari pengamat yang tidak ikut mengalami *clock skew*. Simpan ekspor asli yang sudah disanitasi, identitas revisi/image deployment, serta log penguji secara terpisah. Jangan memasukkan token, kredensial, atau payload mentah ke laporan.

Gunakan HTTPS atau *port-forward* lokal yang tepercaya untuk endpoint berautentikasi. Kolom `git_revision` mencatat revisi checkout collector, bukan versi deployment yang diuji.

### 1. Topologi A2 dan kontinuitas baca

Siapkan `kubectl` dengan izin baca node, pod, PVC dan PV. Pilih tiga ID HazardEvent BMKG dari tiga shard yang berbeda. Pakai katalog yang tidak berubah selama pemeriksaan checksum dan paginasi.

```text
python testing/evidence_topology.py --context CONTEXT --base-url AGGREGATOR_URL --audit-token-file AUDIT_TOKEN_FILE --internal-key-file INTERNAL_KEY_FILE --expected-checksum ASSISTANT_SHA256 --hazard-id ID0 --hazard-id ID1 --hazard-id ID2 --fault-log node-loss.jsonl --watch-seconds 120 --output testing/results/topology.json
```

Tunggu `WATCH READY` sebelum operator menjalankan gangguan yang diizinkan. Tulis tiga observasi aktual secara berurutan ke JSONL: `node_disk_lost`, `old_primary_fenced`, `new_primary_ready`. Setiap baris memuat `event`, `at` (UTC), `node`, `unit`, dan `timeline` (integer PostgreSQL). Dua baris pertama memakai identitas primary lama; primary baru harus merupakan replika sebelumnya dan punya timeline lebih tinggi. Sisakan waktu pemantauan setelah promosi dan pulihkan RF2 sebelum pemeriksaan akhir. Gunakan `--require-partial` hanya jika skenario memang harus memperlihatkan hasil parsial.

Opsional: ikat ID unit ke pod konkret dengan `--placement` (lihat `--help`). Inventaris PVC/PV dan boot ID memeriksa konfigurasi cluster; simpan bukti host fisik, disk, serta log fencing/promosi secara terpisah. Probe membaca endpoint internal yang dipakai Client-Facing API, bukan menggantikan uji end-to-end klien.

### 2. Fencing dan takeover polling

Jalankan collector setelah operator menyelesaikan skenario `death`, `skew`, dan `freeze` yang diizinkan. Ini pemeriksa offline, bukan pemicu gangguan.

```text
python testing/evidence_polling.py --faults faults.jsonl --ingress ingress.jsonl --guard sql-guard.jsonl --reconciliation checkpoints.jsonl --freeze-expiry-seconds 20 --output testing/results/polling.json
```

Jalankan dengan `--help` untuk skema lengkap setiap ekspor. Gunakan log penerimaan upstream, bukan hanya log setelah respons selesai. Kaitkan `seismic`, `warnings`, dan `volcanic` dengan ID owner per inkarnasi dan ID periode dari pengamat yang tidak mengalami *clock skew*. Ekspor keputusan transaksi SQL aktual serta hasil rekonsiliasi checkpoint/ID dari sumber pembanding independen. Gunakan angka expiry yang benar-benar dipakai, bukan selalu `20`.

Collector memeriksa takeover ≤60 detik untuk setiap endpoint, dua pergeseran jam, dan penolakan tulis stale setelah VM melanjutkan. Jika VM dipadamkan sehingga inkarnasi lama tidak dapat melanjutkan, jangan mengarang penolakan SQL; simpan bukti fencing host secara terpisah. Tanpa rekaman yang diperlukan, pemeriksaan terkait tetap belum terbukti.

### 3. Katalog historis dan burst regional

Mulai dengan populasi BMKG kosong pada deployment uji. Jalankan `catalog-load` dua kali dari `bnpb/`: sekali dengan katalog resmi, kemudian dengan JSONL burst berisi `event_id` baru dari satu `region_name`. Jangan ubah placement atau campur trafik tulis lain di antara kedua run. Untuk uji fault topologi, gunakan deployment uji terpisah yang masih memegang checksum historis, bukan dataset yang sudah ditambah burst. Kedua pemuatan menulis data; jalankan hanya setelah mendapat izin operator. Berikan `-expected-checksum` resmi untuk pemuatan pertama dan checksum gabungan dari katalog serta ID burst untuk run kedua. Jangan menghitung referensi dari respons Audit yang sedang diuji.

Gunakan berkas JSONL yang sama untuk loader dan pembanding. Contoh dari `bnpb/`:

```text
go run ./cmd/catalog-load -artifact HIST.jsonl -base-url AGGREGATOR_URL -token-file CATALOG_TOKEN_FILE -audit-token-file AUDIT_TOKEN_FILE -expected-checksum ASSISTANT_SHA256 -resources-manifest resources.json -run-output H.json
go run ./cmd/placement-compare -input HIST.jsonl -resources resources.json > HC.json
go run ./cmd/catalog-load -artifact BURST.jsonl -base-url AGGREGATOR_URL -token-file CATALOG_TOKEN_FILE -audit-token-file AUDIT_TOKEN_FILE -expected-checksum POST_BURST_SHA256 -resources-manifest resources.json -run-output B.json
go run ./cmd/placement-compare -input BURST.jsonl -resources resources.json -burst-area "NAMA WILAYAH" > BC.json
```

`resources.json` memerlukan objek `resources` yang tidak kosong. Hilangkan `reference_checksum` dari manifest bersama karena kedua run memakai checksum berbeda. Gunakan redirect yang menjaga UTF-8; di Windows gunakan `cmd.exe` atau Bash untuk contoh di atas.

Jika dua manifest sudah telanjur memakai checksum berbeda, berikan manifest asli burst sebagai `--burst-resources`. Objek `resources` tetap harus sama.

Dari root aplikasi, gabungkan bukti:

```text
python testing/evidence_catalogue.py --historical-report H.json --burst-report B.json --historical-compare HC.json --burst-compare BC.json --resources resources.json --historical-metrics H.jsonl --burst-metrics B.jsonl --placement placement.json --reference ASSISTANT_SHA256 --out testing/results/catalogue.json
```

`placement.json`: `nodes` berisi nama node Kubernetes sebenarnya. `historical` dan `burst` masing-masing berisi snapshot `before` dan `after` yang mengapit waktu submit, dengan `observed_at`, `revision`, dan `copies[]` (`shard_id`, `unit`, `node`, `role`, `state`). Ambil snapshot dari inventaris aktual; jangan mengganti identitas node dengan alias lokal. Setiap JSONL metrik memerlukan sampel tiap node dalam waktu submit: `timestamp`, `node`, `cpu_percent`, `disk_read_bytes_per_second`, `disk_write_bytes_per_second`, `network_receive_bytes_per_second`, `network_transmit_bytes_per_second`, dan `disk_write_latency_ms`. Gunakan metrik node A2. Angka pembanding `fixed_hash`/`longitude_band` tetap analitis; perubahan jumlah revisi canonical bukan ukuran I/O disk.

### 4. RabbitMQ dan metadata Anggota 3

Gunakan akun baca RabbitMQ Management terpisah dari token Audit. Jalankan `during` setelah operator menyiapkan antrean dan pesan berstatus `retrying` serta `parked`. Jalankan `after` setelah operator mengamati penolakan saat antrean penuh, pengiriman ulang, dan pemulihan setidaknya satu pesan. Collector tidak menyiapkan atau mengubah skenario ini.

```text
python testing/evidence_delivery.py --phase during --broker-url MANAGEMENT_URL --broker-user MONITOR --broker-password-file BROKER_PASSWORD_FILE --audit-url AGGREGATOR_URL --audit-token-file AUDIT_TOKEN_FILE --source PVMBG --delivery-queue hazard.events.dashboard --delivery-id AMQP_ID --retrying-id RETRYING_ID --parked-id PARKED_ID --broker-events broker-during.jsonl --out testing/results/delivery-during.json
python testing/evidence_delivery.py --phase after --broker-url MANAGEMENT_URL --broker-user MONITOR --broker-password-file BROKER_PASSWORD_FILE --audit-url AGGREGATOR_URL --audit-token-file AUDIT_TOKEN_FILE --source PVMBG --delivery-queue hazard.events.dashboard --delivery-id AMQP_ID --recovered-id RECOVERED_ID --during testing/results/delivery-during.json --broker-events broker-after.jsonl --out testing/results/delivery-after.json
```

Untuk JSONL broker, setiap baris memuat `event`, `time`, `vhost`, `queue`, `message_id`, dan `evidence_sha256` (hash rekaman sumber). `retained_before` dan `retained_after` juga memuat `payload_sha256`; hitung dari byte yang benar-benar diamati dan jangan ekspor payload. `publish_nack` memerlukan `messages_ready` dan `cause: "max-length"` dari pengamatan. `message_received`, `message_duplicate_skipped`, dan `effect_committed` memerlukan `delivery_id`; peristiwa efek atau duplikat harus merujuk ke pengiriman yang benar. Salin observasi `retained_before` yang sama ke ekspor `after`. ID metadata yang pulih harus salah satu ID yang diamati di fase `during`; ID metadata tidak harus sama dengan ID AMQP. Hasil `during` tidak membuktikan pemulihan: gunakan laporan `after` sebagai hasil akhir.

**Verifikasi collector:** empat CLI diuji dengan fixture terisolasi. Setiap jalur sukses lulus, dan setiap collector menolak satu bukti cacat: cursor parsial yang berubah, dua owner pada periode yang sama, metrik satu node yang hilang, dan tidak adanya NACK saat antrean penuh. Tidak ada uji deployment, fault, broker nyata, atau dataset resmi pada verifikasi ini.
