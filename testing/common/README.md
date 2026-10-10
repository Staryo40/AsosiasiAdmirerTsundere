# Common PowerShell Helpers

`testlib.py` menyediakan helper lintas-platform untuk parser `.env`, request
HTTP, autentikasi, assertion, hash token, metadata Git, dan JSON evidence.

Helper memakai Python standard library dan tidak mencetak secret atau token.

`p2lib.py` menambahkan generator load HTTP/1.1 dengan socket evidence,
kontrol mock PVMBG, pemeriksaan freshness, dan lifecycle container untuk P02.
Jalankan uji pengukurannya dengan `python -m unittest discover -s testing/common -p test_p2lib.py`.
