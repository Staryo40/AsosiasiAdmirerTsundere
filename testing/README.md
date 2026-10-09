# Acceptance Testing

Folder ini berisi pengujian penerimaan yang dapat diulang. Unit dan integration test tetap berada di folder service masing-masing.

## Struktur

```text
testing/
|-- common/                         # Modul Python bersama
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
