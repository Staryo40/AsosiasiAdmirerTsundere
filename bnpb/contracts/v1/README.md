# Kontrak BNPB Versi 1

Kontrak BNPB mendefinisikan data kanonik, API internal Aggregator, dan pesan notifikasi M1 dengan perluasan enum M2. Berkasnya menggunakan OpenAPI 3.1.1 dan JSON Schema Draft 2020-12.

| Berkas | Fungsi |
| --- | --- |
| [openapi.yaml](openapi.yaml) | Kontrak API internal |
| [hazard-event.schema.json](schemas/hazard-event.schema.json) | Data kanonik lengkap |
| [hazard-summary.schema.json](schemas/hazard-summary.schema.json) | Ringkasan untuk Media |
| [hazard-notification.schema.json](schemas/hazard-notification.schema.json) | Pesan perubahan data |

## Data Kanonik

`HazardEvent` memiliki tepat sebelas atribut wajib. Nilai `null` dan atribut tambahan pada tingkat teratas ditolak.

| Klasifikasi | Atribut |
| --- | --- |
| Ringkasan | `hazard_id`, `source`, `hazard_type`, `severity`, `area_name`, `occurred_at`, `ingested_at` |
| Mentah | `source_ref_id`, `latitude`, `longitude`, `attributes` |

Pasangan yang sah hanya `BMKG/SEISMIC`, `PVMBG/VOLCANIC`, `SATELIT/WILDFIRE`, dan `SENSOR_PANTAI/COASTAL`. Tidak ada enum `TSUNAMI`: warning melekat pada gempa melalui `related_event_id`. Severity menggunakan `NORMAL`, `WASPADA`, `SIAGA`, atau `AWAS`. Identitas dan nama tidak kosong; lintang dibatasi −90 hingga 90, bujur −180 hingga 180. Waktu keluaran memakai RFC 3339 UTC berakhiran `Z`, dengan presisi pecahan detik dipertahankan.

Media hanya menerima Ringkasan; permintaan eksplisit terhadap Mentah ditolak dengan 403. Tim Lapangan dan BNPB Internal Ops memperoleh data sesuai otorisasi. API menegakkan hak akses; `x-classification` hanya anotasi.

Identitas mengikuti `(source, source_ref_id)`. Pembaruan mempertahankan `hazard_id` dan `ingested_at` pertama. Metadata revisi berada di luar `HazardEvent`; pengulangan isi tidak menghasilkan notifikasi baru.

## Pemetaan Data

BMKG memetakan identitas, wilayah, koordinat, dan waktu kejadian. `magnitude`, `depth_km`, serta `potential_tsunami` masuk ke `attributes`. Tanpa peringatan: magnitudo < 5,0 → `NORMAL`, 5,0 hingga < 6,5 → `WASPADA`, ≥ 6,5 → `SIAGA`. Peringatan melalui `related_event_id` menggantikan aturan magnitudo; tingkat tertinggi dipilih: Awas, Siaga, lalu Waspada.

Seluruh peringatan disimpan dalam `attributes.tsunami_warnings`, diurutkan menurut `(issued_at, warning_id)`, bukan `estimated_arrival`; atribut dihilangkan jika kosong. Peringatan tanpa pasangan menunggu kejadian, sedangkan peringatan terlambat memperbarui kejadian yang sama.

PVMBG memetakan `report_id`, `reported_at`, dan `alert_level`. Nama serta koordinat berasal dari referensi statis BNPB. `volcano_id`, `eruption_count_24h`, `ash_column_height_m`, dan confidence jika tersedia masuk ke `attributes`. `confidence_level` menerima angka 0–1, termasuk nol; jika absen, dihilangkan; `null` atau tipe lain ditolak. Pengambilan ulang skema lama hanya mengubah rekaman terkait.

Atribut tambahan, termasuk JSON bersarang dan bilangan besar, dipertahankan. Benturan dengan nama cadangan `source_extras` atau `tsunami_warnings` pada data seismik masuk ke `attributes.source_extras[nama_asli]`; nilai sumber bernama `source_extras` masuk ke `attributes.source_extras.source_extras`. Rekaman tidak valid dikarantina. Atribut wajib hilang, tipe berubah, enum baru, JSON rusak atau berkunci ganda, gunung tidak dikenal, dan identitas sumber yang dipakai ulang untuk kejadian berbeda tidak didukung.

## API Internal

Hanya Aggregator yang mengakses Canonical Store langsung. API membaca data tersimpan tanpa menunggu instansi atau broker.

| Rute | Respons |
| --- | --- |
| `GET /internal/hazards` | `items`, `next_cursor`, `sources`, `as_of` |
| `GET /internal/hazards/{hazard_id}` | `item`, `sources`, `as_of` |
| `GET /readyz` | Probe Kubernetes khusus jalur data: 200 jika minimal satu primary logis dapat dibaca dengan identitas dan skema yang sesuai; 503 jika tidak ada |
| `GET /ready` | Diagnostik readiness: status tiap unit logis dan status poll owner |
| `GET /health` | `service`, `status`; tetap tersedia saat instansi atau broker terganggu |
| `GET /internal/analytics` | Agregat kanonik per waktu/kategori, tingkat ancaman, dan wilayah, dengan penanda data parsial |

Rute data memakai `X-BNPB-Internal-Key`, terpisah dari kredensial instansi dan token klien. `Authorization` tidak menggantikannya. Kunci absen menghasilkan 401; kosong, berulang, atau tidak valid menghasilkan 403. Autentikasi mendahului validasi kueri; pembatasan platform dapat berjalan lebih dahulu.

`X-Correlation-ID` menerima 1–64 karakter ASCII berupa huruf, angka, titik, garis bawah, atau tanda hubung. Nilai hilang atau tidak valid diganti dan dikembalikan pada header respons, terpisah dari korelasi pemasukan data.

### Filter dan Paginasi

Filter: `source`, `hazard_type`, `severity`, `occurred_from`, `occurred_to`. Tanpa sumber, keempat sumber dipilih; jenis bahaya mempersempitnya ke pasangan yang sah. Batas waktu inklusif memakai RFC 3339 dengan zona eksplisit; awal ≤ akhir. `limit`: 1–500, bawaan 100. Parameter asing, berulang, kosong, atau tidak konsisten menghasilkan 400. Detail tidak menerima kueri.

Urutan menurun menurut `(occurred_at, hazard_id)` dengan perbandingan identitas bebas pengaruh lokal bahasa; halaman berikutnya mengambil pasangan lebih kecil. Cursor v1 adalah base64url tanpa karakter pengisi dari `{v:1,occurred_at,hazard_id,filters:{source,hazard_type,severity,occurred_from,occurred_to}}`, maksimum 2048 byte ASCII. Semua kunci filter disertakan: yang absen bernilai `null`, waktu dinormalisasi ke UTC, `limit` dikecualikan. Cursor rusak, berbeda versi, atau berbeda filter ditolak.

`items` selalu larik; `next_cursor=null` menandai halaman terakhir. `as_of` menyatakan waktu pembacaan. Cursor tidak memberi otorisasi atau menjamin data tetap sama selama pembaruan.

### Kebaruan Sumber

`sources` hanya memuat instansi terpilih atau asal detail. Endpoint BMKG adalah `seismic_events` dan `tsunami_warnings`; PVMBG adalah `volcanic_reports`. Pengambilan berhasil setelah respons ditangani dan data, penolakan, serta checkpoint dikomit; respons kosong tetap berhasil. Endpoint gagal menjadi `stale` jika pernah berhasil, atau `unavailable` jika belum.

Pengambilan macet melewati `max(3 × interval, timeout + interval)` ditandai dari keberhasilan terakhir ditambah batas tersebut, atau percobaan pertama jika belum berhasil. Instansi `healthy` jika semua endpoint sehat; `unavailable` jika endpoint utama belum berhasil dan tidak ada data tersimpan; kegagalan atau kondisi belum siap lainnya menjadi `stale`. Endpoint utama adalah `seismic_events` atau `volcanic_reports`; kegagalan peringatan saja tidak menghalangi data seismik.

Metadata instansi memakai komit berhasil paling awal untuk `last_successful_poll` jika semua endpoint pernah berhasil, percobaan terbaru untuk `last_attempt_at`, dan kegagalan belum pulih paling awal untuk `stale_since`. Waktu belum tersedia bernilai `null`. `error_category` mengutamakan endpoint utama, lalu urutan kontrak. `has_cached_data` dihitung sebelum filter. Pemulihan menghapus status kegagalan.

### Galat

Data kosong, usang, atau sebagian tersedia tetap menghasilkan 200. Detail tidak ditemukan menghasilkan 404. Semua sumber terpilih tidak tersedia tanpa data tersimpan, atau basis data terganggu, menghasilkan 503. Ketersediaan tidak ditentukan dari jumlah hasil filter.

Galat aplikasi berbentuk `{error:{code,message,correlation_id}}`. `source_unavailable` menyertakan `sources` dan `as_of`; `store_unavailable` tidak. Platform 429/500 memakai `error` berupa teks dan korelasi pada header. Pesan tidak membocorkan data, kredensial, atau diagnostik internal; 429 tidak menjamin `Retry-After`.

## Pesan Notifikasi

Pesan memiliki delapan atribut: `schema_version`, `message_id`, `hazard_id`, `revision`, `correlation_id`, `event_type`, `produced_at`, `hazard_event`. Versi bernilai 1; revisi pertama memakai `hazard.created`, berikutnya `hazard.updated`. Identitas bahaya sama dengan muatan kanonik; korelasi mengikuti pemasukan data. Waktu pesan tetap sesuai pembuatan outbox dan tidak mendahului penerimaan pertama.

Skema utama menerima atribut opsional tambahan bagi konsumen; `#/$defs/Producer` membatasi produsen pada delapan atribut. Versi atau jenis pesan yang tidak didukung ditolak. Muatan lengkap, termasuk Mentah, hanya untuk jalur internal.

Exchange `hazard.events` bertipe `fanout` menggunakan antrean konsumen terpisah. Pengiriman bersifat *at-least-once*: satu outbox per `(hazard_id, revision)` dikomit bersama perubahan data; transaksi tidak ditahan saat publikasi. Sukses memerlukan ACK produsen tanpa pengembalian wajib. Pengiriman ulang mempertahankan identitas, isi, dan waktu pesan. Konsumen menangani duplikasi, mengabaikan revisi lama atau sama, dan melakukan ACK setelah pemrosesan berhasil. Efek *exactly-once* tidak dijamin.

Ketiga konsumen (`consumer-1`, `consumer-2`, `consumer-3`) mempertahankan identitas dedupe dan catatan percobaan tanpa pemangkasan umur; efek dan dedupe dikomit dalam transaksi SQLite yang sama sebelum ACK. Replay lama yang sudah berhasil diakui sebagai duplikat, bukan dikirim ke antrean gagal. Tabel ini tumbuh sesuai riwayat pengiriman, sehingga kapasitas disk dan backpressure harus dipantau.

Perubahan sumber kebijakan broker telah diizinkan, tetapi belum diterapkan ke broker berjalan: antrean utama dibatasi 10.000 pesan dan antrean gagal 1.000 pesan, memakai `overflow: reject-publish` tanpa TTL atau expiry. Antrean penuh menolak publikasi agar outbox atau handoff gagal tetap dapat diulang, bukan menghapus pesan yang sudah diterima. Konfirmasi broker tetap bukan bukti pemrosesan konsumen; pengiriman broker langsung dan pemulihan infrastrukturnya belum diuji.

## Validasi

[Workflow kontrak](../../../.github/workflows/contracts.yml) memeriksa definisi skema, referensi lokal, dan OpenAPI, serta menjalankan `go test ./...`, `go vet ./...`, dan `go build ./cmd/...` dari direktori `bnpb`. Pada pull request, workflow juga memeriksa kompatibilitas HTTP jika spesifikasi dasar tersedia. Pemeriksaan ini tidak menjamin kompatibilitas notifikasi atau perilaku layanan saat dijalankan. Validator muatan harus memeriksa format waktu; akhiran `Z` saja tidak cukup.

Hasil setiap pemeriksaan tersedia dalam log eksekusi workflow `Contracts` di GitHub Actions.

## Kontrak implementasi Member 1 — Step 1–4

This section records integrated source behavior, not a claim of deployment or external acceptance. Steps 5–9a are present in source. The actual A2 cluster, native host resource-file bindings/credentials, assistant catalog/reference dataset, and Member 3 API remain unavailable; corresponding deployment, native-fence, dataset, and external acceptance evidence is not claimed.

### Identitas, revisi, removal, dan batas transaksi

| Batas | Kontrak tujuan / pemilik penerima |
| --- | --- |
| Identitas | `(source, source_ref_id)` tidak berubah sepanjang lifecycle. `hazard_id` dan `ingested_at` pertama dipertahankan; bukan ID unit fisik. BMKG warning memakai identitas gempa `related_event_id`, bukan `warning_id`. |
| Update | Isi identik tidak menaikkan revision/notifikasi. Perubahan bermakna menaikkan revision sekali dengan immutable `(hazard_id,revision,message_id,payload)`. Retry hasil commit tidak pasti wajib memakai input identity yang sama. |
| SATELIT | Anggota 3 menyimpan lineage ID kebakaran, bukan hash himpunan anggota yang berubah. Late detection/update/retract boleh mengubah pusat, provinsi, anggota, severity, dan `occurred_at` paling awal pada ID yang sama. Merge mempertahankan ID lineage terkecil menurut urutan byte; lineage lain ditarik. Split mempertahankan ID pada komponen yang masih memuat anchor deteksi tersimpan; komponen lain memakai ID kelahiran deterministik dari identitas input pemicu + anchor. Anchor yang hilang diganti anggota terkecil; aturan ini usulan handoff, bukan implementasi clustering atau persetujuan Anggota 3. |
| Removal | Komponen <2/hilang menarik event dari populasi aktif; tombstone internal mempertahankan identitas/revision/dedupe. Reaktivasi lineage yang sama menaikkan revision, bukan membuat identitas baru. Audit/analytics membaca populasi aktif saja; ID checksum berubah sesuai removal. |
| Notifikasi removal | Target handoff wire **v2** mempertahankan delapan field envelope; `schema_version:2`, `event_type:hazard.retracted`, revision ≥2 dan `hazard_event` berisi snapshot aktif terakhir pada ID yang sama (bukan row aktif baru). Consumer menghapus proyeksi aktif, mempertahankan tombstone revision/dedupe, mengabaikan revision lama/sama; reaktivasi memakai `hazard.updated` revision lebih tinggi. Producer tidak boleh beralih ke v2 sebelum semua consumer telah mendukung dan menguji v2. Runtime scope ini tetap v1: hanya created/revision=1 atau updated/revision>1; versi/jenis/revision yang tidak didukung ditolak sebelum efek/dedupe. V2 belum disetujui atau diaktifkan. |
| Guard M1 | `writeHazard` sekarang menolak perubahan `occurred_at` dan menyimpan koordinat PVMBG dari referensi statis. Guard occurrence ini **belum** kompatibel update kebakaran; wajib diganti hanya pada implementasi writer SATELIT, bukan dilonggarkan untuk BMKG/PVMBG. Enum acceptance bukan writer wildfire. |
| Input/write result | Input owner menyimpan bytes asli + ID input/urutan sebelum checkpoint; hasil durable per input: applied (ID/revision/shard), unchanged, atau retained failure (receipt/reason/retryable). Error tak pasti bukan hasil unchanged. Anggota 3 memiliki raw/replay; Aggregator memiliki canonical/outbox. |
| Transaksi | Satu shard memiliki event/revision/outbox/handled-input outcome atomik. Tidak ada transaksi SQL lintas cluster. State graph/offset global milik stream tidak boleh dianggap ikut atomik: simpan intent durable dan outcome per shard, ulangi idempotent sampai seluruh efek terkonfirmasi. |
| Checkpoint | Hanya prefix input kontigu yang seluruh efeknya atau retained failure utuh sudah durable. Gap/unavailable shard menahan prefix; tidak boleh memakai maksimum timestamp dari shard yang berhasil. BMKG memakai overlap timestamp dan tie identity; SSE offset bukan occurrence time/heartbeat tebakan. |
| Epoch | Satu poll owner sistemwide dengan worker endpoint terpisah. Semua mutasi dilindungi `(owner, app_epoch)`; epoch berbeda dari PG system identifier/timeline/primary generation dan placement revision. Aktivasi epoch harus selesai pada seluruh resource terlindungi sebelum owner melakukan poll/business write. |
| Storage boundary | Mutasi mengambil lock guard authority dan memvalidasi owner/epoch/otoritas belum expired dalam transaksi yang sama, lock ditahan hingga commit. Revoke/activate berkonflik dengan lock tersebut. Pemeriksaan deadline harus bersumber authority eksternal yang juga dapat mem-fence storage; `clock_timestamp()` node/cek DCS sebelum SQL tidak memenuhi kontrak skew/freeze. Gate feasibility di laporan menentukan kapan write boleh diaktifkan. |

### Read, partial, analytics, dan handoff

| Permukaan | Kontrak tujuan / degradasi |
| --- | --- |
| Public list/detail | Live keyset menurun `(occurred_at,hazard_id COLLATE "C")`; snapshot lokal per request, bukan snapshot global lintas halaman. Perubahan event-time SATELIT dapat memindahkan item antar halaman; traversal bukan histori immutable. Physical relocation tidak mengubah cursor/logical ID. |
| Cross-shard | Satu deadline request, fan-out paralel bounded, merge kandidat global. Shard tidak terjangkau: data reachable ditandai partial/unavailable; cursor tidak melewati batas lengkap terakhir. Tidak ada klaim lengkap dari hasil parsial. Implementasi fan-out berada setelah Step 4. |
| Audit | Dedicated audit token, bukan Ops. Events menaik `(ingested_at,hazard_id)`, since inklusif; checksum identitas aktif byte-sort/LF tanpa LF terakhir, logical rows sekali. Location memerlukan lokasi aktual, bukan desired placement. Unprocessed dari API metadata Anggota 3; API gagal → bounded 503, bukan items kosong. |
| Analytics → Anggota 2 | Populasi aktif/rentang occurrence sama dengan Audit yang direkonsiliasi pada run boundary stabil; independent shard reads bukan global snapshot. Kategori saling eksklusif: seismic dengan warning → tsunami, seismic lain → gempa; volcanic/wildfire/coastal masing-masing. Removal mengurangi count, update event-time memindahkan bucket. Incomplete shard → angka eksplisit incomplete. |
| Media/Field/Ops | Allowlist Media tetap tujuh field; raw/full eksplisit 403. Field/Ops mendapat full yang diizinkan. Enum baru tidak memberi hak tulis atau raw failed-payload. Field report 202 dan Ops inspection bukan bagian implementasi Step 1–4. |
| Freshness enum baru | SATELIT/SENSOR_PANTAI belum memiliki adapter/health contract runtime. Data tersimpan boleh dibaca dengan `status:stale`, timestamp null, endpoint kosong dan `error_category:source_not_configured`; tanpa data status unavailable. Jangan memberi status healthy berdasarkan enum atau mengarang freshness sensor. |
| Handoff Anggota 2 | Empat pasangan enum, projection, read/partial/analytics contract; A2 menyediakan node/unit nyata, Kubernetes/DCS versi, RBAC, external fencing/time/failure domains dan bukti connectivity. Sensor validity/mapping/freshness/receiver masih memerlukan klarifikasi sumber, bukan ditebak. |

| Handoff Anggota 3 | Empat pasangan enum diterima consumer; identity/revision/lineage/removal dan input/outcome/checkpoint di atas; metadata unprocessed persis Appendix A. Algoritme/fire/feed/replay/removal wire masih harus disepakati dan diimplementasikan pemilik sebelum aktivasi. |

### Fixed logical shard dan placement

Algoritme `sha256-32be-v1`: UTF-8 `source`, satu byte NUL `00`, UTF-8 `source_ref_id` apa adanya; SHA-256; empat byte pertama sebagai uint32 big-endian; modulo **3** → logical `shard-0`, `shard-1`, `shard-2`. Tidak trim/case-fold/normalisasi Unicode, tidak memakai hash Go acak, lokasi/provinsi/anggota kebakaran tidak masuk key. Input harus source sah dan referensi teks UTF-8 nonkosong tanpa control, maksimum 2048 byte. Warning memakai key `(BMKG,related_event_id)`.

Migration menyimpan definisi tiga logical shard dan mapping placement versioned, tanpa mengarang nama node/DNS terpasang. Kolom generated logical ID pada canonical/source/warning menggunakan SHA-256 PostgreSQL yang sama; write M1 tidak berubah routing pool. Placement mencatat semua copy (`copying`, `ready`, `retiring`); semua state dihitung untuk larangan tiga shard pada satu node. Revision adalah token opaque monoton, bukan counter delta +1; trigger INSERT/UPDATE mengubahnya termasuk untuk SQL langsung. Snapshot usang harus abort/retry seluruh transaksi. DELETE/TRUNCATE ditolak karena route change tidak membuktikan copy fisik sudah hilang. Jalur runtime kini `LoadConfig → OpenStore → transaksi migrasi/bootstrap → placement_loaded`; konfigurasi awal memerlukan dua ready copy pada node berbeda per shard. Bootstrap adalah metadata, bukan physical sharding/replication, routed business transaction, atau repair Step 5/10.

Migrasi 005 memperluas CHECK tanpa mengubah row M1; 006 menambah metadata/kolom shard tanpa mengganti ID event; 007 memperbaiki guard placement secara append-only tanpa mengubah fungsi hash atau isi inventory. Inventory v6 yang sudah melanggar batas node membuat migrasi gagal atomik untuk rekonsiliasi, bukan dihapus otomatis. Migrasi mengikuti satu transaksi startup existing, sehingga DDL lock tetap ditahan sampai commit; ini preservasi data, bukan klaim zero-lock online migration. Binary lama menolak versi schema baru; cutover binary/schema/consumer harus terkoordinasi, jangan jalankan writer lama setelah source baru diaktifkan. Rollback gagal/cancel memakai cleanup context terpisah; uncertain commit diselesaikan melalui durable identity.

Implementasi dibantu AI; pemilik manusia wajib memahami perubahan dan mengisi Form Penggunaan AI pada laporan. Riset/keputusan feasibility dan bukti lokal dicatat dalam laporan riset workspace yang sudah ada, bukan dianggap persetujuan antaranggota atau bukti cluster.

### Placement bootstrap

Dipilih **satu environment variable JSON `BNPB_SHARD_PLACEMENT`**, berasal dari ConfigMap hanya untuk Aggregator; Compose lokal meneruskannya dengan default kosong. Tidak ada endpoint admin baru, kredensial tambahan, watcher, atau dependency.

Body tepat `{ "revision": <expected revision>, "copies": [...] }`; setiap copy tepat `{ "shard_id": 0|1|2, "unit": "<unit nyata>", "node": "<nama Kubernetes nyata>", "role": "primary"|"replica", "state": "copying"|"ready"|"retiring" }`. Bentuk ini deskripsi tipe, bukan nilai konfigurasi siap deploy. Gunakan node/unit aktual; jangan menyalin nama fixture sebagai lokasi nyata.

- Maksimum 64 KiB UTF-8; field tidak dikenal/hilang/null, key ganda (termasuk bersarang), trailing JSON, tipe salah, revision negatif dan placement invalid ditolak sebelum koneksi DB. Kosong/absen berarti **tidak mengubah** placement tersimpan, bukan menghapusnya.
- Migrasi dan bootstrap satu transaksi startup `READ COMMITTED`, dengan advisory lock migrasi dan row lock revision. Error/commit tidak pasti menggagalkan startup; retry memakai konfigurasi yang sama. Initial empty inventory hanya menerima RF2 ready pada tiga shard; tidak ada klaim disk/replica sudah benar hanya dari metadata.
- Snapshot copy dibandingkan lengkap menurut `(shard_id,unit)` bytewise, bukan urutan JSON. Isi sama dan expected revision ≤ revision aktual adalah no-op: restart dan startup bersamaan tidak menulis ulang atau menambah revision. Isi berbeda wajib memakai revision aktual; revision masa depan selalu ditolak.
- Perubahan sah tetap memakai guard/CAS/immutable-copy/no-forgetting yang sama. Tidak ada penurunan hak Media/Field/Ops atau privilege mutasi melalui API pembaca. Pengelola platform yang boleh mengubah ConfigMap adalah trust boundary bootstrap.
- Log terstruktur `placement_loaded` memuat revision aktual dan jumlah copy, bukan JSON konfigurasi/DSN. Revision ini digunakan untuk perubahan ConfigMap berikutnya. ConfigMap-env hanya dibaca saat startup: pembaruan memerlukan restart/rollout; bukan live reload.

### Authority dan fencing — keputusan desain Step 2

Patroni 4.1.5 + Kubernetes DCS dipilih untuk HA PostgreSQL; Kubernetes ConfigMap CAS mengoordinasikan satu owner aplikasi/epoch; host-side Linux KVM/libvirt menyediakan fencing independen; guard SQL melindungi setiap transaksi terlindungi. Ini keputusan/protokol sumber, bukan bukti HA atau host fencing telah dipasang atau diterima.

| Batas | Kontrak |
| --- | --- |
| Authority aplikasi | Record systemwide `{epoch,owner,owner_domain_uuid,owner_endpoint,state,renewal_seq,grant_set}` pada ConfigMap yang diperbarui dengan `metadata.resourceVersion` CAS. `resourceVersion` opaque; epoch bigint naik terpisah. State `pending → active → closing → closed`; epoch closed tidak dibuka lagi. |
| Endpoint owner katalog | `BNPB_AUTHORITY_OWNER_ENDPOINT` opsional untuk polling-only mode; jika diisi harus berupa origin HTTP(S) maksimum 2048 byte tanpa userinfo, query, fragment, atau path selain `/`. Nilainya dipertahankan pada record pending/active/closing dan dihapus saat closed. Jika `BNPB_CATALOG_TOKEN` aktif, endpoint wajib tersedia. Discovery hanya untuk routing; penerima tetap memvalidasi authority aktif dan fencing sebelum mutasi. |
| Waktu | Host hypervisor di luar VM memakai `CLOCK_BOOTTIME`; grant terikat `(host_boot_id,domain_UUID,epoch,renewal_seq,absolute_deadline)`. Deadline bukan jam guest, `now()` PostgreSQL, atau client wall clock. |
| Konfigurasi host | Host membutuhkan konfigurasi binding resource JSON terbatas yang mencakup setiap UUID domain yang diizinkan, dengan target guard PostgreSQL fisik (DSN + schema) dan kredensial privileged khusus guard. Domain app-only boleh tanpa target guard. Binding yang hilang, tidak lengkap, atau tidak cocok menggagalkan startup/aktivasi; DSN tidak boleh dicatat. Path Secret/config yang akan dipasang dan nilai mapping A2/credential aktual belum disediakan. |
| Tutup guard | Revoke mencoba menutup guard tepat untuk domain/owner/epoch lama pada setiap target domain. Primary harus mengonfirmasi UPDATE guard; replica read-only harus mengonfirmasi nilai closed yang tereplikasi dalam batas waktu. Target tak dikenal/tidak tersedia bukan konfirmasi. Penutupan tidak boleh menimpa epoch lebih tinggi. |
| Graceful release dan takeover | Stop owner yang dipercaya membatalkan lalu join worker, menutup guard, dan boleh membiarkan domain owner tetap hidup hanya setelah penutupan resource terkonfirmasi. Expiry/takeover wajib mem-fence domain `owner_domain_uuid` lama karena proses polling lama tidak diasumsikan berhenti; domain lain hanya di-fence jika guard targetnya tidak dapat dipastikan tertutup. Domain storage/follower sehat tetap berjalan setelah guard tertutup; jangan menghentikan semua domain. |
| Timer independen | Timer systemd host `WakeSystem=yes`, `AccuracySec=1s`, `RandomizedDelaySec=0` menjalankan `virsh destroy <domain_UUID>` bila penutupan SQL belum terkonfirmasi sebelum batas. Timer baru dibatalkan setelah state closed durable dan resource barrier terverifikasi. Native timer bukan hard-real-time dan tidak membuktikan batas waktu aktual. |
| Fail closed | Kegagalan/ketidakpastian renewal, host-arm, CAS, binding, state atau lock tidak mengaktifkan owner baru. Hilangnya controller tidak membatalkan timer terpasang; high-water mencegah grant lama membuka kembali domain. Tidak ada fallback diam-diam ke DB clock atau DCS failsafe. |

Urutan aktivasi: reserve epoch pending lewat CAS; tutup guard/fence domain owner dan resource terlindungi lama sesuai barrier; arm grant/timer host; aktifkan guard baru pada seluruh resource; CAS active; baru izinkan polling dan business write. SQL closure/replica confirmation harus dicoba sebelum VM fence. Jika owner lama domain sama dengan target revoke, takeover tetap mem-fence domain itu. Controller tidak boleh mem-publish closed atau epoch baru sebelum semua resource aman.

Host operations serialize each domain independently. The shared state-file lock covers only reload/merge/persist, never PostgreSQL guard or libvirt calls. Before external closure I/O, the exact grant is durably marked `closing`; renewal cannot reopen it, and current-grant inspection returns bounded 503 until closure is confirmed. Completion reloads sibling records before persisting so concurrent domain closures cannot overwrite one another. A stale timer or ordinary lock timeout does not authorize fencing a renewed domain.

Host state without a `closing` field remains readable. Coordinate replacement of all host binaries/timer executables before using the new field: older binaries use strict decoding and cannot read it. Preserve the existing high-water file; do not reset it. Newly armed timer services use `Restart=on-failure` to retry uncertain closure; a verified stale invocation exits successfully. These source rules do not establish native hypervisor timing or distributed acceptance.

Host command source defines `init --state-dir --domains --resources-file` and `serve --state-dir --listen --token-file --tls-cert --tls-key --domains --resources-file`; these are flags, not an installation procedure or native execution proof. The host requires bounded resource-file bindings from every allowed owner-domain UUID to actual PostgreSQL guard target(s), with privileged guard-only credentials. A2 mappings, credentials, native Secret/config paths, and native build/run/fencing evidence are missing. Do not invent systemd paths, Secrets, or configuration values.

Desain menyebut grant 20s, renewal 5s, fence lead 10s, batas percobaan aktivasi 20s dan target recovery ≤45s; semua adalah anggaran protokol, bukan hasil pengukuran. Native timing, hypervisor/HA topology, skew/freeze safety, dan A2 mapping/guard credential belum diverifikasi. Jangan aktifkan authority bila binding/credential nyata belum tersedia: aktivasi harus fail closed.

## Integrated contract behavior — Steps 5–9a

These are source-level implementation contracts; installed infrastructure or distributed acceptance is not implied.

### Step 5 — durable routed receipts

Each source input keeps its stable input identity and ordinal/offset as a durable per-shard receipt. Canonical effects, source state, notification outbox and receipt outcome are atomic within one shard; there is no cross-database SQL transaction. Receipt status is durable (`retained` or `applied`); an adapted retained input may transition once to applied. Replays with the same input identity/bytes do not duplicate effects; conflicting bytes are rejected. The central poll checkpoint advances only over a contiguous prefix whose per-input outcomes are durable and stops at the first gap/unavailable shard.

Migration 013 (`013_receipt_binding.sql`) raises the canonical schema head to 13. Before any routed chunk effects, a fenced control-store transaction reserves each `(endpoint,input_hash)` against its SHA-256 raw-byte digest and logical destination (`-1` means unroutable control ledger). An entire routed catalogue request is preflighted before its first effect, including conflicts in later configured chunks. Bindings are immutable; the validated control writer receives only `SELECT` and `INSERT`. An interrupted binding can be resumed with identical bytes, never rebound to another shard. Before the first binding of an old identity, indexed lookups check every legacy handled-input ledger; an unavailable ledger blocks that new binding rather than assuming absence. Existing bindings do not repeat this legacy scan. Per-shard effects/outcomes remain separate transactions; the binding is not a cross-database commit.

Stop and drain pre-fix routed writers before migrating all stores and activating the new binary; mixed old/new writers do not participate in the same reservation protocol. No automatic live binary cutover is claimed. Catalogue identity conflicts return 409 `catalogue_input_conflict`; ordinal overflow is rejected with 400 before mutation.

In routed mode, the control store holds at most one pending response per polling endpoint. Migration 011 (`pending_poll_receipts`) stores ordered record bytes and the original receipt and checkpoint metadata before shard ingestion. A replacement owner resumes that receipt before any new fetch. The receipt is deleted only after a successful final checkpoint; an uncertain acknowledgement may replay the receipt and does not create a second input identity. Grant the restricted control runtime role `SELECT`, `INSERT`, and `DELETE` on this table; shard runtime roles do not use it.

### Step 6 — activation and mutation fencing

Kubernetes ConfigMap `resourceVersion` CAS mengoordinasikan owner/epoch; guard PostgreSQL mengikat otoritas pada generation fisik tiap resource. Host deadline `CLOCK_BOOTTIME` dan timer host independen adalah desain fencing guest freeze, bukan bukti topologi PostgreSQL HA, timing native, takeover terbatas, atau operasi Kubernetes sudah diverifikasi. Owner domain wajib dicatat; release graceful membatalkan dan join worker, sedangkan takeover mem-fence domain owner lama. Tutup guard dan konfirmasi replica dilakukan sebelum VM fence; hanya resource yang gagal dikonfirmasi yang selain domain owner lama perlu di-fence.

A contender gives a newly observed pending epoch a 20s monotonic activation window before attempting recovery. Publishing grants for the same epoch does not restart that window. This is not authority to write and is not a global timeout; takeover still requires guard closure and verified host fencing. A native fence can confirm a domain that is already shut off, but still disables autostart and fails when it cannot verify the final state.

Recovery of an unpublished pending grant set reads each host's durable high-water before requesting a grant. An exact live, expired, or already-closed epoch proceeds to identity-checked revoke and closed-state confirmation; it is never reopened. Only a domain that has not recorded that epoch may be armed for closure. A higher/conflicting epoch, malformed identity, or unavailable host blocks activation.

Migration 012 binds each protected write transaction to the current PostgreSQL WAL insertion timeline under the guard lock. A promoted store cannot reuse the former primary's guard merely because replication copied its row. A2 must still fence the old primary, verify the acknowledged WAL boundary, drain old connections, and publish the safe replacement route.

After renewing host grants, the active owner checks every primary's live resource and installed guard generation under one `BNPB_DB_WRITE_TIMEOUT` budget. A confirmed generation/resource mismatch for the same owner/epoch invalidates its local token; the supervisor cancels and joins workers, closes SQL/host authority gracefully, and uses the existing retry loop to activate a fresh epoch and capture new generation tokens before restarting workers. It does not promote PostgreSQL or bypass guard/host closure. Query failure, a standby response, or a different guard owner is not classified as a confirmed generation change and retains the existing fail-closed renewal path. The same-process graceful closure avoids intentionally fencing the VM that must resume polling.

A newly activated routed poll worker waits one configured endpoint interval before fetching. The delay is elapsed time, not a wall-clock slot, and does not replace host fencing or SQL guards. Routed source calls use direct HTTP/1 without connection reuse or redirects to avoid implicit transport resends. The live-owner check runs immediately before the data GET, after any PVMBG login. A routed 401 invalidates the token for the next scheduled cycle; M1/local retains its immediate renewal. If a passive origin received a request whose response was not staged, a later cycle reads from the unchanged checkpoint. Prove upstream exclusivity and takeover time using origin receipt logs; do not infer exactly-once HTTP from durable effects.

### Step 7 — outbox and consumers

Owner-scoped dispatch stores durable message identity and immutable publication bytes. A publish/mark-sent uncertainty retries the same identity and payload; consumer deduplication/effects persist in each consumer's own database and survive restart. Durable dedupe is intentionally unbounded: pruning would re-enable an old duplicate and is not safe without a proven replay horizon/capacity policy.

While an owner remains active, a failed outbox read excludes only that shard for the dispatch cycle. Available shards still publish in shard-local sequence; the read failure remains visible and no failed shard is acknowledged.

### Step 8 — cross-shard reads

Public reads fan out over bounded shard snapshots under one request deadline, merge deterministically by logical identity and return live pages (not a global snapshot). Opaque v2 cursors use a logical boundary, not physical DSNs. If any shard is unavailable, reachable candidates may be returned with `partial`/`unavailable_units`; the cursor repeats the last complete incoming boundary, including a start marker on an incomplete first page, so the client retries instead of skipping unseen rows. An incomplete detail lookup is not a false 404. Source freshness reflects available poll/cache metadata and remains explicitly unavailable when unknown.

Public reads and Audit events/checksum use stable logical shard endpoints intended for the Patroni `/primary` route. After control-store initialization, shards start in parallel with independent startup budgets. Each startup connection probe is bounded by `BNPB_DB_WRITE_TIMEOUT`. A connection outage or shard timeout leaves that shard unavailable until live schema, identity and writer checks pass on the same store instance. Cancellation of the overall startup and explicit schema or authentication errors fail initialization. A routed snapshot checks the expected logical identity and `pg_is_in_recovery()` on its own SQL transaction before returning data. A wrong shard or standby is unavailable for that snapshot; the cursor does not advance across missing data. Direct physical-copy probes serve location and measurements, not failover reads. Deployment must fence the old primary and expose the caught-up replacement. SQL role alone is not proof of the Patroni leader lock or WAL completeness.

### Step 9 — Audit API

The four Aggregator-owned endpoints are `GET /internal/audit/events`, `/internal/audit/checksum`, `/internal/audit/location`, and `/internal/audit/unprocessed`. All require a dedicated Bearer Audit token, independent of the internal API key and role tokens. Audit errors, including 429, use exactly `{"error":{"code":"…","message":"…"}}`; they do not include `correlation_id`. Audit responses do not use the shared correlation-ID middleware contract. JSON timestamps are RFC 3339 UTC (`Z`).

Events are logical active rows, ordered ascending by `(ingested_at,hazard_id)`. Optional `since` is an inclusive `ingested_at` lower bound; omitted `source` selects all sources. `limit` defaults to 100 and is capped at 1000. Checksum counts logical events (including repeated `source_ref_id` values on distinct events), byte-sorts all selected identifiers, joins them with LF and hashes those bytes with SHA-256; there is no trailing LF. With zero identifiers, the hashed byte sequence is empty. Checksum work uses `BNPB_AUDIT_CHECKSUM_TIMEOUT`, default 5s and bounded from 1ms through 1m; it does not change the separate public read timeout default of 200ms.
`as_of` is the checksum request's UTC start time, not a shared snapshot watermark. Compare full checksums on a stable dataset after the shard routes and replicas have caught up.
Migration 010 adds the Audit access indexes in C byte order: `(ingested_at, hazard_id COLLATE "C")`, `(source, ingested_at, hazard_id COLLATE "C")`, `(source_ref_id COLLATE "C")`, and `(source, source_ref_id COLLATE "C")`. These aid event traversal and checksum sorting at the cost of additional index space and write work.

Cross-shard events/checksum reads may return reachable data with `partial=true` and `unavailable_units`; a partial cursor repeats the last complete boundary (or an opaque start marker on an incomplete first page). `/location` verifies row presence, live PostgreSQL role, and logical identity in the same probe, then reconciles the observed `bnpb.node_name` / `bnpb.unit_name` with placement. Logical primary routes do not report standbys. It returns distinct verified locations and marks uncertainty `partial`; verified absence is 404. A2 must still attest that the server-side settings match real Kubernetes nodes.

`/unprocessed` is a metadata-only proxy to the Member 3 owner API. Missing, unreachable, malformed, oversized, or out-of-filter results yield 503, not an empty success. Reject out-of-filter results rather than reusing the owner's opaque cursor for a locally filtered page. The real Member 3 API has not been observed here.

The metadata owner must keep `reason` safe for the dedicated Audit credential; never put the raw message in this field. Validate the deployed lifecycle and ensure failed-payload inspection remains Internal Ops-only.

### Step 9a — catalog loader and measurement endpoints

`POST /internal/catalog/seismic` accepts at most 1000 raw records in a 4 MiB request body with a dedicated Catalog Bearer token, distinct from Audit and internal keys. The configured `BNPB_AUTHORITY_OWNER_ENDPOINT` is discovered from owner authority state; a follower forwards the original request bytes and relevant headers transparently to the owner at most once. Retries must preserve the same input ID, ordinal and original bytes. The owner stores those original bytes as receipt identity and internally splits the accepted batch according to `BNPB_INGEST_CHUNK_RECORDS` (default 100, bounded by storage transaction budget); catalog chunks omit poll-state writes and do not synthesize upstream poll success. Durable retained input outcomes return 409 `catalogue_input_rejected`; uncertain storage/authority outcomes return bounded 503, never a fabricated success. `GET /internal/storage/measurements` reports per-physical-pool PostgreSQL measurements, querying configured pools in parallel and verifying logical store identity. Canonical revision count is exact for the observed outbox rows; `pg_stat_all_tables` insert/update counters are sampled/approximate. `relation_bytes` is PostgreSQL relation size, not node disk/WAL size; missing or unreachable units are explicit. No measured distributed skew is claimed.

In routed mode, measurements are partial unless distinct live `(shard, node, unit, role)` samples reconcile to the ready RF2 placements. Missing copies are named when known; unverifiable inventory is not complete evidence. A complete topology sample does not prove that standby replay is caught up, that node names reflect actual Kubernetes placement, or that every logical shard contains catalogue records.

The actual CLI is `cmd/catalog-load`: `-artifact`, `-base-url`, `-token-file`, `-audit-token-file`, `-expected-checksum`, `-resources-manifest`, and `-run-output`. It streams JSONL or a JSON array through a private temporary snapshot and uploads only those hashed bytes under their receipt ID. This uses temporary disk proportional to the artifact, not memory proportional to it. Each record is limited to 128 KiB, each request to 4 MiB, and batches to 20 records and the wire budget. The report includes the UTC submission window, artifact hash and before/after BMKG Audit checksums; node metrics must be collected separately. The separate `cmd/placement-compare` accepts `-input`, `-resources` and optional `-burst-area`; it compares `fixed_hash` and `longitude_band` on identical input. These analytical counts are not runtime measurements or evidence from the assistant catalogue.

The local environment template includes optional configuration names for Audit, Catalog, restricted runtime writer and Authority features; leave optional M2 entries commented out unless the complete matching dependencies and credentials are available. `BNPB_CANONICAL_DATABASE_URL` and shard `DATABASE_URL`s are privileged migration/authority DSNs; `BNPB_CANONICAL_WRITER_DATABASE_URL` and shard `WRITER_DATABASE_URL`s are restricted runtime read/write connections to the same targets and use distinct credentials. No real credentials or node names belong in the template.

### Step 10 — Penempatan dan perbaikan standby

Metadata `shard_repair_jobs` tetap merupakan tambahan control store yang tidak menaikkan schema head sendiri; schema head saat ini **13**, termasuk receipt binding. Hanya jalur control store dengan hak istimewa milik Aggregator yang dapat mereservasi metadata perbaikan; akun writer biasa dan klien HTTP tidak mendapat hak untuk mengubah placement. Revisi placement dikunci dalam transaksi yang sama dengan pencatatan repair intent dan reservasi baris `copying`. Semua salinan yang tercatat tetap memakai jatah shard node, termasuk salinan `copying` dan `retiring`. ID shard logis dan identitas fisik lama tetap dicatat.

`BNPB_SHARD_REPAIR_CANDIDATES` is a Secret-backed JSON array for the routed deployment. It binds a candidate to a shard, named local PVC and StatefulSet, and the explicitly allowed source members. Member DSNs, direct Patroni API URLs and node/unit identities come from Member 2; never derive them from Audit location response data or put them in logs. The worker only raises the predeclared standby's scale from 0 to 1 using a Kubernetes revision check. Patroni on that blank disk performs the physical backup and WAL follow. The controller does not call `/reinitialize`, remove old data, or expose a replacement primary.

- Maksimum 64 KiB; satu entri per shard. Kunci: `shard_id`, `sources`, `known_replica` (opsional jika placement tidak punya replica ready), `target`, `namespace`, `stateful_set`, `patroni_config_map`, `claim`. Setiap member memuat `unit`, `node`, `dsn`, `patroni_url`, `scope`, `name`. Sumber perbaikan dipilih dari primary `ready` pada placement saat ini dan harus cocok dengan member dalam daftar `sources` yang diizinkan; controller tidak memakai sumber di luar daftar tersebut. DSN probe perlu akses baca `store_identity`, fungsi identitas/replay PostgreSQL, dan statistik replikasi primary; tidak perlu hak DML bisnis.
- Gunakan PVC yang dirujuk langsung oleh template Pod; `volumeClaimTemplates` tidak didukung karena StatefulSet dapat mengganti volume yang telah diverifikasi. Jika snapshot penempatan dengan revisi saat ini diterima tanpa mengubah salinan, snapshot tersebut tetap disimpan sebagai konfigurasi bootstrap terakhir.
- ServiceAccount Aggregator memerlukan GET terhadap StatefulSet, ConfigMap, PVC, dan PV yang diizinkan, serta GET/PATCH terhadap subresource `statefulsets/scale` yang ditetapkan. Batasi RBAC pada nama dan scope yang diperlukan. A2 harus menyediakan PVC yang sudah terikat ke PV lokal pada node yang ditentukan, dengan direktori **kosong**. Jangan gunakan ulang disk produksi, memberi controller hak delete, atau menerbitkan kandidat `copying` sebagai rute baca maupun primary.
- ConfigMap kandidat harus `immutable: true`, dengan `patroni.yaml` berisi JSON yang juga sah sebagai YAML: scope/name sesuai target, `tags.nofailover: true`, dan metode `basebackup` bawaan atau `create_replica_methods: ["basebackup"]` eksplisit. Kunci lama `create_replica_method` ditolak. Kontainer `postgres` harus membaca file itu dan memasang PVC data pada path yang sama dengan `PGDATA` dan `postgresql.data_dir`. Pertahankan `role=store` untuk Tester Agent dan gunakan label Patroni yang terpisah (`kubernetes.role_label: pg-role`).
- `tmp_role_label` dan `bootstrap_labels` tidak boleh mengubah `role=store`. Setel label tambahan di ConfigMap yang diverifikasi, bukan melalui override environment.
- Proses supervisi memberi satu jendela kerja 20 detik per siklus, lalu menunggu 5 detik. Backup dapat berlanjut pada siklus berikutnya. Setelah primary mengonfirmasi replikasi sinkron, controller mengambil batas flush WAL dan menunggu replay hingga batas itu sebelum mencatat target `ready` dan salinan lama `retiring`. Pada siklus berikutnya, controller memeriksa kembali identitas, timeline, status replikasi sinkron, dan posisi replay target. Target yang hilang atau mulai dari disk kosong kembali ke `copying`. Perubahan sumber atau timeline memulai intent dengan revisi placement baru dan membatalkan penyelesaian dari intent lama. Semua salinan fisik tetap tercatat.

Standby repair keeps `nofailover` enabled. The repair WAL barrier records catch-up at verification time. Recovery after a later primary loss requires the acknowledged WAL boundary for that loss.

1. A2 must fence the old PostgreSQL primary and prevent it from restarting as a writer. Establish the acknowledged WAL boundary and its timeline from the deployment's durability guarantees.
2. Verify the candidate's shard, system identifier, WAL history, and replay through that boundary before making it eligible for promotion. Confirm that the promoted member holds Patroni leadership and is writable.
3. Publish the promoted member on the logical primary route and drain connections to the previous backend. Publish the new roles with a placement CAS. Aggregator must close and rebind its mutation guard to the new generation before resuming guarded writes.
4. Exercise the sequence after losing the primary of a repaired shard. Compare the complete Audit checksum and count with the pre-loss values.

Automatic promotion remains open. The current deployment contribution lacks the Patroni HA configuration, old-primary fence, and logical primary routing needed to execute this handoff.

### Step 11 — Handoff analitik untuk Anggota 2

`GET /internal/analytics?from=<RFC3339>&to=<RFC3339>&bucket=hour|day|week` memakai `X-BNPB-Internal-Key`; kredensial tetap di sisi server, bukan di peramban. Respons `200` memuat `from`, `to`, `bucket`, `as_of`, `total`, `series`, `severity`, `areas`, `partial`, dan `unavailable_units`. `from` dan `to` menerima stempel waktu RFC 3339 dengan maksimal sembilan digit pecahan detik dan harus tetap dapat direpresentasikan dalam RFC 3339 setelah dinormalisasi ke UTC. Rentang wajib `[from,to)` pada `occurred_at`; awal harus sebelum akhir, maksimum 366 hari dan 1.000 bucket. Bucket bawaan `day`; `week` mulai Senin pukul 00.00 UTC. Parameter asing, kosong, atau berulang ditolak.

- `series`: entri `{start,category,count}` yang tidak nol; klien mengisi bucket kosong hanya untuk unit yang tercakup. Kategori analitik: `EARTHQUAKE`, `TSUNAMI`, `VOLCANIC`, `WILDFIRE`, `COASTAL`. Gempa dengan `attributes.tsunami_warnings` kanonik yang berasal dari relasi `related_event_id` dihitung sekali sebagai `TSUNAMI`; `potential_tsunami` saja tetap `EARTHQUAKE`. Kategori analitik bukan enum `HazardEvent`.
- `severity`: objek dengan empat kunci `NORMAL`, `WASPADA`, `SIAGA`, `AWAS`, termasuk hitungan nol. `areas`: entri `{area_name,count}` dengan nama kanonik dan urutan byte. Masing-masing dimensi berjumlah `total` atas populasi tercakup.
- Baca satu primary per logical shard, dengan satu batas waktu permintaan. Shard gagal tidak menyumbang angka dan muncul di `unavailable_units`; jika `partial:true`, tampilkan penanda data belum lengkap pada semua grafik dan total, termasuk jika `total:0`. Jika `partial:false`, jumlah nol memang mewakili populasi kosong pada pembacaan itu.
- Satu permintaan analitik berat per instance diizinkan di luar jatah snapshot biasa; kelebihan menerima 429. Anggaran kueri berasal dari `BNPB_AUDIT_CHECKSUM_TIMEOUT` dan dibatasi agar masih ada waktu untuk mengirim respons. Nama wilayah dikembalikan utuh. Hasil yang melebihi anggaran grup per unit atau `BNPB_INGEST_MAX_RESPONSE_BYTES` menghasilkan 503 eksplisit, bukan hitungan yang dipotong.
- Update menggantikan versi aktif dan dapat memindahkan bucket, severity, atau wilayah. Setelah pemilik menerapkan retract dan mengeluarkan event dari populasi aktif, event tidak dihitung di Analytics maupun Audit. Ini bukan aktivasi writer/retract Anggota 3.

Untuk rekonsiliasi, gunakan jendela tanpa mutasi pada populasi aktif yang dibandingkan (termasuk perubahan warning dan retract). Traversal `/internal/audit/events` dengan token Audit sampai `next_cursor:null`, tanpa memakai `since` sebagai filter kejadian. Terapkan rentang occurrence yang sama, lalu cocokkan `total` dan seluruh dimensi, bukan hanya checksum ID. Kedua hasil harus lengkap; saat penulisan bersamaan, `as_of` tidak menyinkronkan snapshot antarshard/muka API dan bukan token snapshot. Appendix A tidak diubah.

### Step 12 — Sinyal operasional Aggregator

`/health` memeriksa liveness. `/readyz` memeriksa primary logis secara paralel dan mengembalikan 200 begitu satu transaksi baca memverifikasi identitas, peran primary, dan skema. Probe membatalkan pemeriksaan sisanya; bila tidak ada unit yang lolos dalam batas waktu, probe mengembalikan 503. Probe ini tidak menunggu poll owner, upstream, atau broker. `/ready` tetap diagnostik seluruh unit dengan `process_status`, `ready`, `logical_shards` (entri `id`, `unit`, `status`), dan `poll_owner` (`local`, `owner`, `follower`, atau `unavailable`). Respons tidak berisi DSN, nama host fisik, atau kredensial.

`/metrics` memakai jatah request terpisah dari jalur data; label metrik memakai pola rute yang terbatas, bukan ID per kejadian atau path aktual. HTTP Go meneruskan `X-Correlation-ID` dan nilai `traceparent` v00 yang valid tanpa perubahan, termasuk saat follower meneruskan permintaan katalog ke owner. Penerusan header ini tidak membuat span. Anggota 2 masih harus mengintegrasikan pengumpulan metrik, instrumentasi dan ekspor span, serta dashboard.

Manifest Aggregator mewajibkan `BNPB_SHARD_0_DATABASE_URL` dari Secret khusus Aggregator. Downward API mengisi `BNPB_K8S_NODE_NAME` dari `spec.nodeName`; ConfigMap mengisi `BNPB_AUTHORITY_NODE_DOMAINS` sebagai objek JSON yang memetakan nama node Kubernetes ke UUID domain fencing khusus yang diizinkan. Gunakan `BNPB_AUTHORITY_OWNER_DOMAIN_UUID` di luar Kubernetes; jika keduanya disetel, nilai literal yang berbeda dari pemetaan node ditolak. Secret juga harus menyediakan `host-fence-token` dan `host-fence-ca.crt`. A2 masih harus menyediakan nilai aktual, RBAC, rute, disk lokal beserta binding, dan integrasi telemetri. Ini kontrak sumber, bukan hasil deployment atau pengujian.
