# P02 - Konkurensi dan ketersediaan

Prasyarat: Python 3.9+, Git, Docker CLI/Compose, stack root sudah running,
credential `.env` sesuai container, dan kedua sumber sehat dengan data awal.
Jalankan dari root repository:

```text
python testing/P02/run_all.py
```

Suite berjalan berurutan selama sekitar 3-4 menit. Exit code 0 berarti seluruh
test lulus; kegagalan tetap menyimpan `result.json` dan mengembalikan non-zero.
P03 tetap dapat dijalankan seperti sebelumnya.

| Test | Default | Ambang penerimaan |
| --- | --- | --- |
| T03 | 6 worker, warm-up 5s, pengukuran 30s; PVMBG 3000ms | p95 respons BMKG-only sukses <300ms; event BMKG baru masuk |
| T04 | 50 worker, warm-up 5s, sustained 65s; PVMBG 3000ms | >=50 socket TCP teramati sepanjang rentang sampel >=60s; unexpected error <1%; semua kelompok mendapat respons sukses; container tidak restart/crash |
| T05 | Outage 60s, sampel setiap 2s, recovery maksimal 90s | BMKG sehat dan event baru; PVMBG HTTP 200 dengan cache serta stale_since; pulih dengan event baru tanpa restart |

Jalankan satu skenario:

```text
python testing/P02/T03-head-of-line/run.py
python testing/P02/T04-sustained-load/run.py
python testing/P02/T05-outage-recovery/run.py
```

Untuk bukti outage panjang 10 menit sesuai kontrak mock di checklist:

```text
python testing/P02/T05-outage-recovery/run.py --outage-seconds 600
```

Endpoint dapat diubah dengan `API_BASE`/`PVMBG_BASE` di environment atau
`--api`/`--pvmbg` pada runner individual. Gunakan `--help` untuk opsi lengkap.
Runner memeriksa container dengan `docker compose` dari root repository.

## Injeksi kegagalan dan pemulihan

T03/T04 mengatur PVMBG ke tepat 3000ms dan memverifikasinya dengan request langsung.
Delay dipulihkan melalui `finally` ke nilai `.env` (default 500..3000ms).
Karena tidak ada endpoint GET untuk membaca delay runtime sebelumnya, bila
operator sebelumnya mengubah delay melalui endpoint admin, berikan nilai
pemulihan eksplisit:

```text
python testing/P02/T03-head-of-line/run.py --restore-delay-min 500 --restore-delay-max 3000
```

T05 mensyaratkan outage awal OFF dan selalu mengirim `enabled:false` melalui
`finally`, termasuk ketika assertion gagal. Penghentian paksa proses/mesin
dapat melewati cleanup; gunakan admin outage/delay untuk pemulihan manual.
Jangan menjalankan beberapa scenario bersamaan karena semuanya mengubah mock.

## Tooling dan cara mengukur

Tool load memakai Python standard library `http.client`, satu worker thread
per koneksi HTTP/1.1 keep-alive. Semua worker menjalankan request tanpa think
time setelah warm-up. Komposisi worker dibagi bergantian BMKG, PVMBG, gabungan
(50 worker: 17/17/16); latensi dipisahkan per kelompok.

`connections.json` merekam endpoint lokal/peer socket TCP yang sudah terkoneksi
setiap 0,5 detik. Ini bukti socket dari generator, bukan sekadar jumlah thread.
Jumlah minimum dan rentang observasinya diuji terpisah. Sampling menunjukkan
koneksi pada saat sampel; tidak membuktikan keadaan setiap mikrodetik antar
sampel. Tutup/reconnect dan kegagalan transport tetap dicatat di request mentah.

Model ini closed: respons melambat akan menurunkan request rate. Hasil hanya
mewakili ukuran dataset dan kondisi mesin pada saat pengujian. Baseline jumlah
item serta konfigurasi pagination dicatat; batas list 100 record per request.
Warm-up tidak masuk sustained. Request yang dimulai saat sustained tetapi selesai
setelah akhir interval tetap masuk latensi/error rate; late completion hanya
dikeluarkan dari perhitungan throughput interval. Ini mencegah timeout lambat
di akhir pengukuran tersembunyi dari error rate.

Laporkan throughput total/sukses, p50/p95/p99 respons sukses dan semua respons,
hitungan/status 429, rejection rate, serta kedua rasio unexpected error:
`unexpected / semua request` dan `unexpected / (semua request - 429)`.
Ambang <1% memakai rasio kedua, sehingga banyak 429 tidak menyamarkan error.
Kegagalan koneksi/timeout/payload diberi status 0 dan dihitung sebagai error.
Semua kelompok harus mendapat respons data sukses; seluruh respons 429 gagal.

Credential hanya dipakai dalam memory. Sesi Field diterbitkan kembali sebelum
TTL habis tanpa memutar refresh token yang sedang dipakai thread lain;
request login tidak dimasukkan ke statistik endpoint data. Ini mekanisme
generator untuk load, sedangkan expiry/refresh diuji khusus oleh P03/T08.

## Evidence

```text
testing/results/P02/<timestamp>/
|-- T03/result.json
|-- T03/requests.jsonl
|-- T03/connections.json
|-- T04/result.json
|-- T04/requests.jsonl
|-- T04/connections.json
|-- T05/result.json
|-- T05/outage-observations.json
`-- summary.json
```

`result.json` menyimpan config test, versi tooling, platform/CPU count,
metadata Git, config container non-secret, ID/start time/restart count,
hasil sebelum/sesudah, metrik, dan alasan kegagalan. Raw request hanya berisi
status, durasi, kelompok, worker, dan waktu relatif; token/header tidak ditulis.
Evidence diabaikan Git. Tinjau/sanitasi sebelum memasukkan bukti ke laporan.

## Bahan laporan P2-06

Jelaskan mekanisme dengan rujukan kode dan hasil aktual:

- `bnpb/internal/aggregator/poll.go`: tiga loop polling independen (gempa,
  warning, vulkanik), dijadwalkan dari selesainya request. PVMBG lambat tidak
  menahan loop BMKG dan tidak menumpuk task setiap tick.
- `bnpb/internal/aggregator/read.go`: jalur baca snapshot dengan admission
  terbatas dan penolakan 429 ketika kapasitas penuh.
- `bnpb/internal/aggregator/store.go`: pool baca dan tulis terpisah dengan
  batas koneksi dan timeout; ingest dan request baca mempunyai batas resource.
- `bnpb/internal/aggregator/freshness.go`: status sumber/endpoints, waktu sukses,
  stale_since dan unavailable memungkinkan cache tetap dibaca saat sumber mati.
- `bnpb/internal/aggregator/poll.go`: kegagalan dicatat per endpoint,
  retry memakai bounded backoff/jitter dan polling pulih setelah sumber hidup.

Cantumkan trade-off snapshot (data tidak real-time), penolakan 429, timeout,
batas pool, closed-model load, kondisi dataset/mesin, serta angka aktual dari
evidence. Hasil test tidak menggantikan penjelasan mekanisme dalam laporan.
