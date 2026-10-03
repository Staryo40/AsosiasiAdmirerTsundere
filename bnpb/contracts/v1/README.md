# Kontrak BNPB Versi 1

Kontrak BNPB mendefinisikan data kanonik, API internal Aggregator, dan pesan notifikasi sesuai spesifikasi M1. Berkasnya menggunakan OpenAPI 3.1.1 dan JSON Schema Draft 2020-12.

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

`BMKG` berpasangan dengan `SEISMIC`; `PVMBG` dengan `VOLCANIC`. Severity menggunakan `NORMAL`, `WASPADA`, `SIAGA`, atau `AWAS`. Identitas dan nama tidak kosong; lintang dibatasi −90 hingga 90, bujur −180 hingga 180. Waktu keluaran memakai RFC 3339 UTC berakhiran `Z`, dengan presisi pecahan detik dipertahankan.

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

Filter: `source`, `hazard_type`, `severity`, `occurred_from`, `occurred_to`. Tanpa sumber, kedua instansi dipilih; jenis bahaya mempersempitnya. Batas waktu inklusif memakai RFC 3339 dengan zona eksplisit; awal ≤ akhir. `limit`: 1–500, bawaan 100. Parameter asing, berulang, kosong, atau tidak konsisten menghasilkan 400. Detail tidak menerima kueri.

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
