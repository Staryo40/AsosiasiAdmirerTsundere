# P4 — Independensi service dan storage fleksibel

Suite ini memeriksa kriteria implementasi **Problem 4 — Arsitektur Microservice & Flexible Storage** dalam spesifikasi M1 dan checklist P4-01–04 serta P4-06–08. Kewajiban laporan P4-05 tetap membutuhkan penilaian manusia; exit `0` hanya menyatakan skenario runtime yang dipilih lulus.

## Menjalankan

Dari root repository, gunakan Python 3.9+, Git, Docker Engine/Compose yang mendukung `include` dan `!override`, serta berkas `bmkg/.env`, `pvmbg/.env`, dan `bnpb/.env` yang sudah dikonfigurasi:

```powershell
python -m pip install -r testing/P04/requirements.txt
python testing/P04/run_all.py
python -m unittest discover -s testing/P04 -p test_checks.py -v
```

Runner membuat **proyek baru `aat-p4-<ID>` pada setiap eksekusi**, membangun seluruh stack melalui satu perintah Compose, dan memilih port host loopback secara dinamis. Nama image BMKG/PVMBG juga dipisahkan. Endpoint ditentukan dari port container aktual sehingga pengujian HTTP, identitas container, dan SQL mengacu pada stack yang sama. Variabel `COMPOSE_FILE`, `COMPOSE_PROJECT_NAME`, `COMPOSE_PROFILES`, `API_BASE`, `BMKG_BASE`, `PVMBG_BASE`, dan `TEST_RESULTS_ROOT` dari sesi sebelumnya tidak menentukan target pengujian ini.

Overlay menggunakan subnet eksplisit `10.251.240.0/24`–`10.251.245.0/24` karena pool subnet default Docker dapat habis pada mesin dengan banyak proyek. Pastikan subnet bebas; sesuaikan overlay bila berbenturan dengan network Docker/LAN/VPN. Jalankan **satu stack P4 pada satu waktu**, termasuk stack yang dipertahankan dengan `--keep-stack`.

```powershell
python testing/P04/run_all.py --only T10
python testing/P04/run_all.py --only T11
python testing/P04/run_all.py --only T12 --deadline 120
python testing/P04/run_all.py --keep-stack
```

`--deadline` membatasi setiap observasi eventual, bukan seluruh durasi suite. T12 menunggu beberapa record baru dan checkpoint melampaui polling overlap; interval generator/polling yang besar membutuhkan deadline lebih panjang. Startup/build dibatasi 900 detik, perintah lain 300 detik, dan probe database 15 detik. `--keep-stack` mempertahankan stack untuk diagnosis.

Secara default, blok `finally` menjalankan `down` pada proyek pengujian ini, tanpa menghapus volume. Nama volume yang dipertahankan tercatat di `summary.json`. Runner tidak menjalankan reset SQL, menghapus histori, atau menghentikan stack demo `aat`. Setiap pengulangan memakai volume baru supaya ID mock yang kembali ke awal tidak bertabrakan dengan Canonical Store lama. Hentikan suite lain yang sedang mengubah kode atau konfigurasi selama pengujian ini.

Untuk memeriksa stack yang dipertahankan, gunakan nama proyek dan dua berkas Compose yang tercatat dalam `summary.json`:

```powershell
docker compose -p aat-p4-<ID> -f compose.yaml -f testing/P04/compose.acceptance.yaml ps --all
docker compose -p aat-p4-<ID> -f compose.yaml -f testing/P04/compose.acceptance.yaml logs --tail 100 SERVICE
docker compose -p aat-p4-<ID> -f compose.yaml -f testing/P04/compose.acceptance.yaml down
```

## Cakupan dan oracle

| Checklist | Skenario | Bukti yang diwajibkan |
| --- | --- | --- |
| P4-01, P4-06 | T10 | Sembilan komponen wajib hidup/healthy dengan container berbeda; stop → build → force-recreate `consumer-1`; ID/start time target berubah; ID, image, start time, restart count, dan running state **seluruh peer** tetap sama, termasuk job `broker-init` yang sudah selesai |
| P4-02 | T10 | Request BMKG melalui Client-Facing API menghasilkan HTTP 200 dan HazardEvent yang tidak kosong saat stop, offline, build, dan start; kegagalan request menggagalkan skenario; volume consumer tetap sama |
| P4-04, P4-07 | T11 | Store memiliki satu network internal dengan anggota hanya PostgreSQL/Aggregator, tanpa host port; service lain tidak menerima konfigurasi/kredensial database, tidak memasang volume store atau Docker socket |
| P4-04, P4-07 | T11 | `pg_isready` pada **network namespace asli** setiap service: Aggregator dapat mencapai DNS `postgres` dan IP langsung; BMKG, PVMBG, API, Auth, broker, serta kedua consumer tidak mendapat respons pada keduanya; positive control diulang sesudah probe negatif |
| P4-04 | T11 | Request terautentikasi dengan correlation ID unik; log API menunjukkan HTTP keluar ke `/internal/hazards`, log Aggregator menunjukkan penerimaan request yang sama dengan status 200 dan latensi |
| P4-03 | T12 | Oracle P1 menguji live V1 → V2 tanpa restart/redeploy; record lama tanpa confidence dan record baru dengan confidence tetap terbaca melalui list/detail API; kedua ID dan seluruh attributes cocok dengan dua row SQL yang disimpan bersama |
| P4-03, P4-08 | T12 | Kolom, constraints, indeks, trigger, fungsi, dan histori migrasi sebelum/sesudah tetap sama; `attributes` bertipe JSONB tanpa kolom confidence; unique `(source, source_ref_id)`, indeks sumber/waktu, dan volume persisten tersedia |
| P4-05 | Manual | Laporan final menjelaskan alasan batas service serta membandingkan minimal dua storage; tabel di bawah merupakan bahan laporan, bukan sertifikasi bahwa laporan final sudah memenuhi syarat |

T10 memakai `consumer-1` karena API tidak bergantung padanya. Rebuild Auth tentu dapat memengaruhi request yang memerlukan introspeksi; P4 tidak mewajibkan semua request tetap sukses untuk semua kemungkinan dependency yang mati. Build cache boleh digunakan: keberhasilan perintah build, target tunggal, dan recreation container dicatat; image ID yang identik karena source tidak berubah bukan kegagalan.

T11 menjalankan probe sementara memakai **image PostgreSQL yang sama** dengan store, dengan `--network container:<ID>`. Probe tidak menerima password atau mount store dan tidak menambah keanggotaan network baru. Exit `2` dari `pg_isready` berarti tidak ada respons; exit `1` berarti server dapat dijangkau tetapi menolak koneksi dan **tidak** membuktikan isolasi. Exit `3`, kegagalan Docker, atau timeout menggagalkan test. Pembatasan berlaku pada service aplikasi; operator dengan akses Docker dapat melakukan inspeksi/admin. Query SQL pada T12 adalah observasi operator read-only, bukan akses database dari service nonpemilik.

T12 memakai primitive oracle mapping/toggle di `testing/P01/acceptance.py`, termasuk validasi JSON Schema dan refresh token. Ini mempertahankan satu definisi kebijakan mapping; P01 harus tersedia. Row SQL diamati **saat V2 masih aktif**, sebelum pemulihan schema version melalui `finally`. Pemulihan ke V1 menghapus confidence dari respons sumber, dan record yang masih dalam polling overlap dapat diperbarui kembali; pengujian perubahan aditif tidak menuntut preservasi field setelah sumber menariknya. Fingerprint tidak membuktikan ketiadaan DDL sementara yang dibuat lalu dibatalkan, maupun seluruh kemampuan schema evolution. Scope yang diuji adalah perubahan aditif `confidence_level` dan struktur storage yang tetap sama di batas observasi. Preservasi nilai dibandingkan secara semantik karena JSONB tidak menjamin urutan key/whitespace.

Pemeriksaan `ingested_at` memakai waktu PostgreSQL yang diamati langsung (`clock_timestamp`), karena layanan Docker berbagi jam VM yang dapat berbeda dari jam Windows. Timestamp receipt di masa depan relatif terhadap store tetap gagal. Ini tidak mengubah jam sistem atau oracle P1; waktu awal/akhir evidence tetap memakai jam observer. T12 menyimpan basis jam yang digunakan dan tetap memvalidasi HazardEvent, aturan mapping PVMBG, serta timestamp kejadian terhadap sumber.

## Hasil dan batas klaim

Bukti berada di `testing/results/P04/<timestamp>/`: `T10.json`, `T11.json`, `T12.json`, dan `summary.json`. Setiap kegagalan skenario menghasilkan exit non-zero dan tetap disimpan. Summary mencatat commit, working tree kotor, versi Python/Compose, proyek, endpoint nonsecret, hasil skenario, status cleanup, volume, serta kewajiban manual. Pemilihan `--only` tidak menyatakan skenario lain sudah diuji.

Environment container dan keluaran Compose/log dibaca dalam memori; hanya field bukti yang dipilih ditulis. Password/token/DSN tidak disimpan. Bukti T12 mengandung payload sumber dan atribut dinamis: tinjau dan sanitasi sebelum dibagikan. Commit hash saja tidak menyatakan working tree identik dengan commit. Container image ID dan source fingerprint juga tidak menggantikan build provenance formal.

Unit test harness mencoba restart peer, kebocoran network/port/password/mount, executable probe gagal, row SQL hilang/duplikat, confidence null/bool/di luar rentang, serta kegagalan build/startup. Ini memeriksa agar oracle tidak meluluskan kondisi yang salah; unit test tidak menggantikan eksekusi Docker/live acceptance. Suite ini bukan load test P2, bukan verifikasi seluruh delivery/crash window P5, dan bukan benchmark storage.

## Bahan laporan P4-05

| Batas service | Alasan untuk sistem ini |
| --- | --- |
| BMKG dan PVMBG | Instansi otonom dengan skema, delay, kontrol runtime, dan kredensial berbeda |
| Aggregator + kepemilikan Canonical Store | Satu pemilik aturan polling, korelasi, mapping, checkpoint, dan transaksi canonical/outbox; schema storage tidak menjadi kontrak yang diakses service lain |
| Client-Facing API | Menegakkan proyeksi/scope dan menyajikan data melalui API Aggregator; perubahan presentation tidak perlu deployment poller |
| Auth | Mengelola identitas, sesi, expiry/refresh, dan introspeksi; perubahan token tidak mengubah mapping/storage |
| Broker dan consumer terpisah | Broker mengirim event; masing-masing consumer memiliki efek/state sendiri dan lifecycle independen; failure consumer tidak menjadi dependency sinkron query API |

Shared `internal/platform` berisi infrastruktur teknis, bukan pemanggilan business logic lintas service. Membuat service terpisah untuk setiap fungsi mapping atau outbox dispatcher akan menambah dependency jaringan dan memecah kepemilikan transaksi; dispatcher tetap modul Aggregator. Memasukkan semua komponen dalam satu proses akan menggabungkan batas deployment yang P4 ingin pisahkan.

| Storage | Konsekuensi pada kebutuhan M1 |
| --- | --- |
| **Dipilih: PostgreSQL typed-core + JSONB** | Constraint/unique/index tetap tersedia untuk field inti dan pagination; field tambahan berada dalam JSONB tanpa kolom baru; transaksi meliputi canonical/checkpoint/outbox. Validasi domain tetap tugas aplikasi; JSONB bukan validator HazardEvent. Indeks dokumen ditambahkan hanya jika workload membutuhkannya |
| Document store | Record V1/V2 dapat berbeda bentuk bila validator kompatibel; akses dokumen alami. Validasi inti, unique identitas, query waktu, dan transaksi/outbox tetap perlu dirancang. Mengganti storage menambah driver/operasi baru dan belum memberi keuntungan terukur untuk PoC ini |
| Key-value | Menyimpan payload per hazard ID sederhana, tetapi filter sumber/waktu, pagination, korelasi, dan atomisitas state/outbox memerlukan struktur indeks serta konsistensi tambahan |

Perbandingan ini adalah analisis desain untuk spesifikasi, bukan hasil benchmark produk. Masukkan alasan, bukti runtime, dan keterbatasannya ke laporan final.

## Dasar riset

Riset Exa dilakukan berurutan sesuai panduan repository: tiga pencarian dengan lima hasil per pencarian (**15 hasil pencarian**, bukan 15 sumber independen); empat halaman primer dibaca lagi. Sumber komersial/SEO pihak ketiga tidak dijadikan dasar oracle.

| Sumber primer | Pengaruh pada pengujian |
| --- | --- |
| [Docker Compose up](https://docs.docker.com/reference/cli/docker/compose/up/) | Pilih satu service, gunakan `--no-deps`, dan verifikasi recreation serta peer runtime |
| [Docker port publishing](https://docs.docker.com/engine/network/port-publishing/) | Published port dapat membuka jalur dari network lain; periksa bindings selain DNS dan network membership |
| [PostgreSQL 18 JSON types](https://www.postgresql.org/docs/18/datatype-json.html) | Periksa atribut JSONB di row nyata dan bandingkan makna JSON, termasuk field absen versus angka nol |
| [Microsoft microservices architecture](https://learn.microsoft.com/en-us/azure/architecture/guide/architecture-styles/microservices) | Pemisahan berdasarkan tanggung jawab dan kepemilikan data; buktikan deployment independen dan gunakan API pemilik |

Context7 `resolve-library-id` → `query-docs` digunakan untuk Python standard library 3.9.22, Docker Compose, dan PostgreSQL 18 sebelum implementasi. Dokumentasi Context7 Compose mengikuti branch utama; opsi diverifikasi dengan CLI yang terpasang. Python 3.9.22 merupakan batas kompatibilitas yang tersedia di Context7; runtime lokal memakai Python 3.13.5. Suite menggunakan image PostgreSQL 18.6 yang dipin proyek dan dependency validator P1 yang sama. Keluaran dokumentasi tidak dianggap bukti acceptance.

Implementasi pengujian dibuat dengan bantuan LLM; reviewer kelompok perlu memahami oracle, menjalankannya, dan mencantumkan penggunaan AI sesuai ketentuan tugas.

## Verifikasi lokal 9 Oktober 2026

| Pemeriksaan | Hasil | Direktori bukti lokal |
| --- | --- | --- |
| T10: rebuild independen dan API tetap melayani | PASS; 17 request saat stop, 3 offline, 40 build, 45 start; sembilan peer tetap sama | `testing/results/P04/20261009-194626-433783/T10.json` |
| T11: isolasi store dan API pemilik | PASS; DNS/IP negatif untuk tujuh service, positif untuk Aggregator; correlation ID lintas API/Aggregator cocok | `testing/results/P04/20261009-194626-433783/T11.json` |
| T12: coexistence V1/V2 dan skema tetap | PASS; dua row SQL cocok dengan API; confidence absen pada V1 dan bernilai `0.2610854` pada V2 | `testing/results/P04/20261009-195522-440445/T12.json` |
| Unit test harness | 9 test PASS | `python -m unittest discover -s testing/P04 -p test_checks.py -v` |
| Lint dan whitespace diff | PASS | `ruff check testing/P04`; `git diff --check` |

T12 dijalankan ulang terpisah setelah perbaikan urutan observasi dan referensi jam. Summary run sebelumnya tetap mencatat kegagalan T12; bukti gagal tidak ditimpa. Seluruh stack pengujian sudah dibersihkan tanpa menghapus volume; stack demo `aat` tetap healthy. Working tree ketika pengujian kotor; hasil ini belum merupakan sertifikasi commit final atau pemenuhan laporan P4-05.
