# Dashboard analitik (A2-06)

Aplikasi **React + Vite** dengan gaya visual mengikuti [`paradigma-design.md`](../paradigma-design.md).
Di produksi, **nginx** menyajikan build statis (`dist/`) dan mem-proxy `/auth/` serta `/v1/` ke Client-Facing API
di cluster (tanpa CORS, secret tidak di-bundle).

## Pengembangan lokal

```text
cd dashboard
npm install
npm run dev
```

Buka <http://localhost:5173>. Proxy Vite mengarahkan `/auth` dan `/v1` ke `localhost:8080` (jalankan API via Compose atau port-forward).

## Build & image

```text
npm run build
docker build -t aat/analytics-dashboard -f dashboard/Dockerfile dashboard
```

Push tag `analytics-dashboard` ke registry yang dipakai di `k8s/kustomization.yaml`.

## Kubernetes

Termasuk di `kubectl apply -k k8s`. NodePort: **`http://<node-ip>:30081`**.

Login: `client_id` / `client_secret` klien mana pun (Media, Field Team, atau Internal Ops). Angka dan grafik berasal dari `GET /v1/analytics` yang dihitung Aggregator; bila sebagian unit penyimpanan tidak terjangkau, setiap kartu diberi penanda "Data belum lengkap". Token hanya di memori React (state), hilang saat refresh tab.