# T08 - Token Expiry and Refresh

Membuktikan expiry access token Tim Lapangan secara alami pada TTL runtime,
refresh otomatis tanpa login ulang, keberhasilan token baru, dan penolakan
access/refresh token lama.

Token tidak disimpan mentah. Evidence hanya boleh menyimpan hash SHA-256,
timestamp, aksi, dan status HTTP.
