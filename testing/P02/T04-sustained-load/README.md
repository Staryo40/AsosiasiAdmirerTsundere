# T04 - Sustained load

```text
python testing/P02/T04-sustained-load/run.py
```

50 worker memiliki 50 koneksi TCP HTTP/1.1 keep-alive. Setelah warm-up,
interval sustained berjalan 65s agar rentang observasi socket tetap >=60s.
Metrik sukses dan semua status disimpan per kelompok serta agregat.
429 dihitung terpisah; error transport/payload dan status lain adalah error.
Unexpected error harus <1% dari seluruh request non-429. Container diperiksa
sebelum/sesudah agar crash/restart tidak tersembunyi.

Menurunkan --workers atau --duration boleh untuk diagnostik, tetapi hasil tidak
lulus kriteria T04. Lihat README P02 untuk metode, keterbatasan, dan evidence.
