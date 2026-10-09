# P3 - Autentikasi dan Hak Akses

| Test | Tujuan |
| --- | --- |
| T06 | Kredensial BMKG dan PVMBG ditolak ketika ditukar lintas domain. |
| T07 | Media hanya menerima Ringkasan; akses `view=full` ditolak server. |
| T08 | Expiry alami, refresh tanpa login ulang, rotasi, dan penolakan token lama. |
| T09 | Secret aktif tidak hardcoded/ter-commit dan `.env.example` lengkap. |

Jalankan seluruh skenario setelah implementasi:

```text
python testing/P03/run_all.py
```
