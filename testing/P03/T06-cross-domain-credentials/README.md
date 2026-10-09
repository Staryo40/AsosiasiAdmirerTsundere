# T06 - Cross-domain Credentials

Membuktikan kredensial valid pada domain asalnya dan ditolak dengan HTTP
401/403 ketika dipakai pada domain lain.

Kasus minimum:

1. BMKG API key ke BMKG: 200.
2. BMKG API key sebagai Bearer ke PVMBG: 401/403.
3. PVMBG reader token ke PVMBG: 200.
4. PVMBG reader token sebagai `X-BMKG-Key` ke BMKG: 401/403.

Evidence tidak boleh memuat API key atau bearer token mentah.
