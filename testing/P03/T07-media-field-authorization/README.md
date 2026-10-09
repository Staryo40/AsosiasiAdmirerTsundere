# T07 - Media and Field Authorization

Membuktikan bahwa Media hanya menerima tujuh field Ringkasan dan bahwa
permintaan eksplisit `view=full` ditolak dengan HTTP 403. Field dan Ops harus
dapat meminta representasi penuh.

Allowlist Ringkasan:

- `hazard_id`
- `source`
- `hazard_type`
- `severity`
- `area_name`
- `occurred_at`
- `ingested_at`
