# T09 - Secret Hygiene

Memeriksa bahwa:

- `bmkg/.env`, `pvmbg/.env`, dan `bnpb/.env` tidak dilacak Git;
- ketiga file tersebut dicakup `.gitignore`;
- setiap service mempunyai `.env.example`;
- nilai secret aktif tidak muncul di tracked files;
- histori Git dipindai dengan alat yang meredaksi temuannya sebelum disimpan.
