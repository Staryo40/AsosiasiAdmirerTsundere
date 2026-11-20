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

Collector `evidence_*.py` di folder ini menggunakan Python 3.9+ tanpa paket tambahan. Jalankan dari root aplikasi. Collector hanya membaca endpoint, inventaris Kubernetes, atau hasil pengamatan; collector tidak memicu fault, mengisi antrean, menjalankan replay, atau menulis katalog.

Tulis setiap laporan ke path baru di `testing/results/`. Exit `0` berarti semua pemeriksaan mekanis collector lulus untuk input tersebut; exit `1` berarti bukti tidak lengkap atau gagal. Input log yang dinormalisasi harus berasal dari pengamatan aktual, menggunakan timestamp UTC dari pengamat yang tidak ikut mengalami *clock skew*. Simpan ekspor asli yang sudah disanitasi, identitas revisi/image deployment, serta log penguji secara terpisah. Jangan memasukkan token, kredensial, atau payload mentah ke laporan.

Gunakan HTTPS atau *port-forward* lokal yang tepercaya untuk endpoint berautentikasi. Kolom `git_revision` mencatat revisi checkout collector, bukan versi deployment yang diuji.

### 1. Topologi A2 dan kontinuitas baca

Siapkan `kubectl` dengan izin baca node, pod, PVC dan PV. Pilih tiga ID HazardEvent BMKG dari tiga shard yang berbeda. Pakai katalog yang tidak berubah selama pemeriksaan checksum dan paginasi.

```text
python testing/evidence_topology.py --context CONTEXT --base-url AGGREGATOR_URL --audit-token-file AUDIT_TOKEN_FILE --internal-key-file INTERNAL_KEY_FILE --expected-checksum ASSISTANT_SHA256 --hazard-id ID0 --hazard-id ID1 --hazard-id ID2 --fault-log node-loss.jsonl --watch-seconds 120 --output testing/results/topology.json
```

Tunggu `WATCH READY` sebelum operator menjalankan gangguan yang diizinkan. Tulis tiga observasi aktual secara berurutan ke JSONL: `node_disk_lost`, `old_primary_fenced`, `new_primary_ready`. Setiap baris memuat `event`, `at` (UTC), `node`, `unit`, dan `timeline` (integer PostgreSQL). Dua baris pertama memakai identitas primary lama; primary baru harus merupakan replika sebelumnya dan punya timeline lebih tinggi. Sisakan waktu pemantauan setelah promosi dan pulihkan RF2 sebelum pemeriksaan akhir. Gunakan `--require-partial` hanya jika skenario memang harus memperlihatkan hasil parsial.

Opsional: ikat ID unit ke pod konkret dengan `--placement` (lihat `--help`). Inventaris PVC/PV dan boot ID memeriksa konfigurasi cluster; simpan bukti host fisik, disk, serta log fencing/promosi secara terpisah. Probe membaca endpoint internal yang dipakai Client-Facing API, bukan menggantikan uji end-to-end klien.

### 1a. Pemulihan setelah kehilangan primary pada shard hasil repair (C6)

Gunakan satu ID HazardEvent BMKG dari shard yang telah diperbaiki. Siapkan count dan checksum referensi independen, serta pertahankan populasi BMKG selama pengamatan. Mulai collector sebelum gangguan kedua dan tunggu `WATCH READY`. Operator menjalankan skenario yang telah diizinkan secara terpisah.

```text
python testing/evidence_repair.py --base-url AGGREGATOR_URL --audit-token-file AUDIT_TOKEN_FILE --hazard-id ID --old-unit OLD_PRIMARY --repaired-unit REPAIRED_REPLICA --expected-count ASSISTANT_COUNT --expected-checksum ASSISTANT_SHA256 --cutover-log cutover.jsonl --output testing/results/repair.json
```

Catat observasi aktual berikut secara berurutan dalam JSONL. Setiap baris memuat `event`, `at` (UTC), `node`, `unit`, `timeline` (integer PostgreSQL), `system_id` (string desimal), dan `evidence_sha256` (SHA-256 rekaman sumber yang disanitasi).

| `event` | Observasi sumber dan field tambahan |
| --- | --- |
| `node_disk_lost` | Tester atau host mengonfirmasi hilangnya disk primary lama. |
| `old_primary_fenced` | Host mengonfirmasi primary lama tidak dapat kembali menulis. `acknowledged_lsn` adalah batas WAL yang mencakup seluruh transaksi yang telah diakui, berdasarkan jaminan durabilitas deployment. |
| `candidate_verified` | PostgreSQL melaporkan `replay_lsn` kandidat pada timeline lama sebelum promosi. |
| `new_primary_ready` | Patroni mengonfirmasi kepemimpinan kandidat pada timeline baru dan platform mengonfirmasi rute primary aktif. |
| `guard_rebound` | SQL mengonfirmasi guard aktif pada resource dan timeline baru. Catat `previous_epoch` dan `epoch` sebagai integer. |

Collector mencocokkan urutan, anggota, timeline, batas WAL, dan kenaikan epoch dengan lokasi serta checksum yang diamati lewat Audit. Simpan bukti sumber untuk memeriksa fencing, batas WAL, dan guard secara independen. Collector memerlukan sampel selama kehilangan primary dan checksum lengkap setelah guard aktif. Hasilnya mencakup skenario satu shard yang diuji.

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

## Urutan verifikasi Member 1 — repair, analitik, dan readiness

| Urutan | Fixture dan cakupan |
| --- | --- |
| 10 — Repair | PostgreSQL 18 loopback yang baru dan terisolasi, sesuai [panduan BNPB](../bnpb/README.md#m2-member-1-steps-19a--source-and-local-evidence-boundary). Uji CAS dan status persisten, retry, identitas fisik serta batas WAL. Fixture loopback tidak membuktikan pemulihan Kubernetes, fencing host, atau RF2 lintas node. |
| 11 — Analitik | Gunakan fixture aktif yang mencakup kelima kategori, kejadian pada batas waktu, perubahan kategori setelah warning, update/retract, dan satu shard tidak tersedia. Bandingkan seluruh halaman Audit yang lengkap dengan API analitik dalam jendela mutasi stabil. `since` Audit berarti waktu ingest, bukan waktu kejadian; jangan meneruskan `from` sebagai `since`. |
| 12 — Readiness | Gunakan fixture shard untuk data lengkap, degradasi parsial, dan tidak ada unit valid. Uji perbedaan `/health`, `/ready`, dan status poll owner, juga batas admission serta propagasi korelasi lintas HTTP. |
| 13 — Regresi | Dari `bnpb/`, setelah uji terarah berurutan: `go test ./...`, `go vet ./...`, `go build ./cmd/...`, `go test -race ./...`, lalu uji bertag `integration`. `regression_test.go` dan `p1_mapping_test.go` adalah regresi lokal. `p1_integration_test.go` mensyaratkan `postgres-test/bnpb_test` di jaringan Compose profil `tests`; suite Go memakai fixture BMKG/PVMBG dan server HTTP in-process, bukan proses mock hidup. Suite routed memakai `BNPB_M2_LOCAL_TEST_DATABASE_URL` dan tidak menggantikan fixture Compose. Jangan arahkan keduanya ke Canonical Store. |

Untuk bukti terdistribusi, catat revisi dan digest image, hash serta count referensi asisten, konfigurasi efektif, resource Appendix B, identitas node/boot/host dan disk lokal. Gunakan `evidence_topology.py` untuk inventaris, observasi baca berbatas waktu, urutan loss/fence/promote, lokasi, dan checksum sebelum/sesudah satu gangguan. Gunakan `evidence_repair.py` untuk skenario C6. `evidence_catalogue.py` mencatat beban natural/burst, `evidence_polling.py` mencatat takeover, dan `evidence_delivery.py` mencatat bukti broker serta metadata Anggota 3. Bukti kontinuitas tulis dan RF2 pada seluruh populasi memerlukan pengamatan tambahan.

Setelah ada izin deploy dan fault yang eksplisit, simpan artefak terpisah untuk:

1. **Kontinuitas dan fencing:** ukur baca melalui satu entry point dan tulis melalui API sumber resmi selama jendela fault/repair. Rekam respons sukses, parsial, penolakan terkontrol, latensi, dan ID korelasi. Rekam konfirmasi fence independen sebelum cutover, identitas cluster/shard, riwayat timeline, batas WAL yang mencakup semua write terakui, replay pada tujuan, dan pengosongan sesi lama. Hilang koneksi atau HTTP `/primary` saja bukan bukti. Gunakan populasi BMKG historis tetap untuk perbandingan referensi dan trafik tulis yang tidak mengubah populasi itu; laporkan kedua populasi.
2. **Pemulihan RF2 dan kehilangan berikutnya:** simpan placement dengan status copying/ready/retiring serta byte, I/O dan replay WAL pada node kosong sewaktu trafik berjalan. Verifikasi dua salinan yang benar-benar aman pada node berbeda untuk setiap shard, tidak satu node menyimpan seluruh populasi, baru jalankan fault kedua pada target yang diizinkan. Bandingkan lagi count dan SHA-256 Audit BMKG lengkap (`partial:false`, `unavailable_units:[]`) dengan referensi asisten, bukan dengan checksum parsial atau checksum yang dibuat dari respons yang sedang diuji.
3. **Backend sehat:** simpan `kubectl get pods` dan EndpointSlice Service dengan waktu dari pengamat independen. Sebelum fault, minimal tiga backend siap di tiga node dengan kernel berbeda. Korelasikan permintaan baru melalui Service dengan access log dari setiap pod; setelah backend ditandai tidak siap dan EndpointSlice berubah, tidak ada permintaan baru yang mencapai backend itu sementara backend sehat masih menjawab. Catat koneksi lama terpisah dari koneksi baru. Periksa respons follower ketika hanya sebagian shard atau poll owner yang terganggu.
4. **UI dan observabilitas:** simpan dua pilihan rentang, angka per kategori/severity/area, dan bukti UI masih tampil dengan penanda parsial. Tangkap dashboard metrik per layanan, satu trace lintas layanan yang benar-benar diekspor dengan durasi, serta log sanitasi dengan ID korelasi. Tangkap layar, log aksi, log aplikasi, dan hasil pengukuran Tester Agent practice hanya untuk skenario yang diizinkan secara eksplisit.
