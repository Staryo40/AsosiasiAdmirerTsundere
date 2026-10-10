# P5 — Message Broker dan Consumer

Membuktikan kriteria penyelesaian Problem 5 (P5.1–P5.4) terhadap stack yang sedang berjalan, memakai trafik organik BMKG/PVMBG yang sesungguhnya, bukan data sintetis.

## Yang diuji

- **P5.1/P5.2** — satu publish mencapai ≥2 consumer independen; producer tidak mengetahui keberadaan consumer.
- **P5.3** — consumer dihentikan tidak menahan producer atau consumer lain; event yang terbit selama downtime tersusul (tidak hilang) setelah dinyalakan kembali.
- **P5.4** — consumer baru (consumer-3) ditambahkan secara live tanpa me-restart consumer lain atau mengubah kode producer.

## Menjalankan

Prasyarat: stack sudah berjalan (`docker compose up -d --build` dari root repositori) dan `broker-init` sudah **Exited (0)** — lihat [`bnpb/README.md`](../../bnpb/README.md#running-the-application).

```bash
cd <root repositori>
python3 testing/P05/p5_demo.py | tee testing/P05/evidence-$(date +%Y%m%d-%H%M%S).log
```

Opsi (lihat juga docstring `p5_demo.py`): `--project NAME` (default `aat`), `--wait-baseline`/`--wait-down`/`--wait-p54` dalam detik — perkecil bila interval polling/generator di `.env` diperpendek untuk demo yang lebih cepat.

Skrip ini mengubah state nyata: ia benar-benar menghentikan dan menyalakan ulang `consumer-2`, dan membawa `consumer-3` naik lewat profile `demo`.

## Keluaran

Script mencetak tiap tahap dengan header section, snapshot `/stats` tiap consumer (JSON), potongan log dengan correlation ID, kedalaman queue RabbitMQ saat consumer mati, lalu satu baris `[PASS]`/`[FAIL]` per pemeriksaan. Baris terakhir adalah ringkasan `N/9 checks passed.`, dengan exit code non-zero bila ada yang gagal.

`tee` pada perintah di atas menyimpan transkrip lengkap ke `testing/P05/evidence-<timestamp>.log` — file inilah yang menjadi bukti P5.1–P5.4 untuk bagian 7.4 laporan Problem 5.
