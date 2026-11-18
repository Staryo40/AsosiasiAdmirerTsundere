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
| `GET /ready` | 200 jika skema dan basis data siap; selain itu 503 |
| `GET /health` | `service`, `status`; tetap tersedia saat instansi atau broker terganggu |

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

## Validasi

[Workflow kontrak](../../../.github/workflows/contracts.yml) memeriksa definisi skema, referensi lokal, dan OpenAPI, serta menjalankan `go test ./...`, `go vet ./...`, dan `go build ./cmd/...` dari direktori `bnpb`. Pada pull request, workflow juga memeriksa kompatibilitas HTTP jika spesifikasi dasar tersedia. Pemeriksaan ini tidak menjamin kompatibilitas notifikasi atau perilaku layanan saat dijalankan. Validator muatan harus memeriksa format waktu; akhiran `Z` saja tidak cukup.

Hasil setiap pemeriksaan tersedia dalam log eksekusi workflow `Contracts` di GitHub Actions.

## Kontrak implementasi Member 1 — Step 1–4

Keputusan integrator, bukan persetujuan Anggota 2/3. Sumber normatif: M2 Perubahan Kontrak, P1/P2/P4, Lampiran A; M1 pemetaan kanonik. Status handoff Anggota 2 dan 3: **belum dikonfirmasi**. Bagian ini membedakan kontrak tujuan dari kemampuan runtime. Step 5 (transaksi lintas input/shard), fencing, feed, retraction, Audit, dan repair belum diaktifkan oleh pekerjaan Step 1–4.

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

Keputusan author kini ditetapkan: **Patroni 4.1.5 + Kubernetes DCS** untuk HA PostgreSQL; **Kubernetes ConfigMap CAS** terpisah untuk satu owner aplikasi/epoch; **fence lease host-side Linux KVM/libvirt** untuk waktu/expiry; dan **guard SQL pada setiap transaksi terlindungi**. Tidak ada etcd tambahan, Pacemaker, atau konsensus buatan. Ini keputusan implementasi untuk Step 5/6, **bukan** kode fencing terpasang, persetujuan anggota lain, atau kelulusan runtime.

| Batas | Mekanisme konkret terpilih |
| --- | --- |
| Authority aplikasi | Record systemwide `{epoch,owner,state,renewal_seq,grant_set}` pada ConfigMap yang diperbarui dengan `metadata.resourceVersion` CAS. `resourceVersion` tetap opaque; epoch bigint naik terpisah. State `pending → active → closing → closed`; epoch closed tidak dibuka lagi. ConfigMap hilang/reset tidak boleh mereset epoch; rekonsiliasi high-water SQL/host atau fail closed. |
| Waktu | Host hypervisor yang tidak ikut VM skew/freeze memakai `CLOCK_BOOTTIME`; grant terikat `(host_boot_id,domain_UUID,epoch,renewal_seq,absolute_deadline)`. Deadline tidak dihitung dari jam guest, `now()` PostgreSQL, atau client wall clock. Delayed renewal tidak boleh diubah menjadi `host-now + TTL`; closed/expired grant ditolak. |
| Fence independen | Timer host systemd `WakeSystem=yes`, `AccuracySec=1s`, `RandomizedDelaySec=0` menjalankan libvirt `virsh destroy <domain_UUID>` bila revoke SQL belum dikonfirmasi sebelum batas. Timer sudah di-arm sebelum epoch active dan berjalan walau owner/controller guest berhenti. Native libvirt/systemd adalah fasilitas platform, bukan service bisnis di luar Kubernetes. Watchdog guest/QEMU bukan bukti fence independen. |
| Domain target | Semua domain yang dapat menjalankan primary/promotable copy dari resource terlindungi tercakup grant set; termasuk checkpoint/outbox/ingress/stream state yang kelak dilindungi. Satu domain mungkin memuat dua shard: lease/fence mencakup keduanya. Placement revision, app epoch, PG generation dan hypervisor boot ID tidak disamakan. |
| Mutasi | Stored owner-API transaction mengunci guard, memeriksa exact `(owner,epoch,active,pg_generation)`, memutasi effects/outbox/input/checkpoint dan menahan lock hingga commit. Role writer tidak memperoleh jalur DML bypass. Expiry eksternal ditegakkan oleh revoke guard atau penghentian resource, bukan DCS-read lalu SQL tanpa proteksi. |
| Revoke normal | Controller menutup grant/guard dengan lock yang berkonflik sebelum deadline, termasuk membatalkan pekerjaan tertunda. Timer domain dibatalkan hanya setelah seluruh guard pada domain itu tertutup dan transaksi lama selesai. Resource yang macet/tidak terjangkau tetap di-fence; readiness/routing removal saja tidak cukup. |
| Frozen resource | Jika guard tidak bisa ditutup, timer pada host menghentikan domain sebelum authority hard deadline. App/PG yang resume tidak mendapat izin write dari status lama. Libvirt autostart/auto-restart writable dinonaktifkan; domain yang sudah fenced hanya boleh masuk recovery/replica sebelum epoch/generation baru diaktifkan. |
| Fail closed | Renewal/host-arm/CAS yang gagal atau tidak pasti tidak mengaktifkan owner. Hilangnya authority/controller tidak membatalkan timer yang sudah di-arm; expired-with-no-takeover tetap tertutup/fenced. Tidak ada penggantian diam-diam ke DB clock atau DCS failsafe. |

Urutan aktivasi: CAS reserve epoch `pending`; tutup/fence owner dan PG generation lama pada seluruh resource; pastikan host grant/timer ter-arm dengan epoch dan deadline baru; aktifkan guard SQL baru pada seluruh resource; CAS `active`; baru izinkan poll/publikasi/business write. Host menyimpan high-water epoch/renewal sequence sehingga re-arm dari pesan owner lama tidak dapat membuka closed grant. Revoke/fence completion mendahului bisnis owner baru, bukan dipicu write pertama.

Renewal: serial per epoch dengan CAS; peroleh grant deadline absolut baru dari host authority, arm semua domain yang dipersyaratkan, lalu publikasikan renewal lengkap. Arm yang tidak lengkap tidak memperluas authority systemwide; timer lama boleh menyebabkan penolakan/downtime aman. Tidak ada grant baru setelah expiry pada epoch yang sama; refresh memerlukan epoch baru dan revoke/fence barrier.

Anggaran desain: lease 20s, renewal setiap 5s, mulai revoke/fence paling lambat 10s sebelum hard deadline; target fence executor selesai ≤5s, menyisakan margin 5s. Setelah expiry, target activation/promotion ≤20s dan poll pertama ≤5s: target keseluruhan ≤45s, **bukan hasil pengukuran**. systemd bukan hard-real-time; host load, suspend/wake, libvirt latency dan anti-restart harus membuktikan bound ini sebelum aktivasi. Jika tidak, desain ditolak pada platform itu—tidak menurunkan keselamatan untuk mengklaim ≤60s.

Asumsi eksplisit: node uji adalah guest Linux KVM/libvirt; hypervisor/fence executor dipercaya dan tetap berjalan ketika guest dibekukan/jamnya digeser. Host freeze, kegagalan hardware wake, atau host-wide timer starvation tidak diselesaikan oleh software timer: platform itu memerlukan fence independen yang terverifikasi sebelum diizinkan. Tidak ada klaim libvirt, watchdog, control-plane quorum atau fasilitas host tersebut sudah tersedia pada workstation/cluster saat ini.

Dengan demikian pilihan author I2 selesai pada **level desain/protokol**. Penerapan guard writer, host lease executor dan bukti skew/freeze/failover tetap merupakan acceptance implementasi Step 5/6 dan pengujian cluster berizin; bukan flag keputusan yang dilempar kembali ke Member 2.
