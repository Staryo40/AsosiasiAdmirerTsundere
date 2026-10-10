# T03 - Head-of-line blocking

```text
python testing/P02/T03-head-of-line/run.py
```

PVMBG dipaksa 3000ms. Enam koneksi ke Client-Facing API membaca BMKG,
PVMBG, dan gabungan bersamaan. Warm-up 5s terpisah dari pengukuran 30s.
Test memakai p95 respons data BMKG sukses, mensyaratkan <300ms, dan
membuktikan event BMKG baru masuk ketika upstream PVMBG lambat.

Lihat README P02 untuk cleanup delay, opsi CLI, dan format evidence.
