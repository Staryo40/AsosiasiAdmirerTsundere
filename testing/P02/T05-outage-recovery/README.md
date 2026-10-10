# T05 - Outage and recovery

```text
python testing/P02/T05-outage-recovery/run.py
```

Kedua sumber harus healthy dan punya data pada baseline. Outage PVMBG
diaktifkan 60s; BMKG harus tetap healthy dengan event baru. PVMBG wajib
memberi cache dengan freshness stale/unavailable dan stale_since untuk stale.
Timeout/500 tidak lulus. Outage dipulihkan dalam finally, lalu polling harus
healthy dan menerima event baru dari kedua sumber dalam 90s tanpa restart.

Untuk outage 10 menit gunakan --outage-seconds 600. Lihat README P02 untuk
opsi, cleanup, dan format evidence.
