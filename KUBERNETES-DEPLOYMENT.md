# Deployment Kubernetes (Milestone 2)

Seluruh sistem BNPB berjalan di cluster Kubernetes ([k3s](https://docs.k3s.io/)) dengan minimal dua
node, pada namespace `sistem-informasi-kebencanaan-bnpb`. Setelah persiapan di bawah selesai, deploy
cukup dengan:

```text
kubectl apply -k k8s
```

Semua perintah dijalankan dari root repositori.

## Topologi

| Node | Peran |
| --- | --- |
| `node-a` | control plane + worker |
| `node-b` | worker |

Node boleh berupa VM atau mesin Linux native, asalkan kernelnya terpisah. Kedua node harus saling
terhubung lewat satu jaringan privat dan memiliki akses internet. Skenario mematikan node hanya
dikenakan pada worker yang bukan control plane (`node-b`).

Manifest ada di folder `k8s/`. Konfigurasi biasa ada di `k8s/config/*.env`, sedangkan secret ada di
`k8s/secrets.env` (tidak masuk Git).

## Kebutuhan

- Dua mesin Linux (disarankan Ubuntu Server 24.04) dengan IP privat tetap, minimal 2 vCPU dan 4 GB RAM.
- `kubectl` dan Docker di mesin yang dipakai untuk build dan deploy.
- Akun container registry (Docker Hub) untuk menyimpan image.

## 1. Memasang k3s

Di **node-a**, ganti `<ip-node-a>` dan `<interface>` (interface yang memegang IP privat, cek dengan
`ip -br -4 addr`):

```bash
curl -sfL https://get.k3s.io | sh -s - server \
  --node-ip <ip-node-a> \
  --flannel-iface <interface> \
  --tls-san <ip-node-a> \
  --write-kubeconfig-mode 0644 \
  --disable traefik \
  --disable servicelb

sudo cat /var/lib/rancher/k3s/server/node-token
```

Di **node-b**, dengan token dari perintah terakhir:

```bash
curl -sfL https://get.k3s.io | \
  K3S_URL=https://<ip-node-a>:6443 K3S_TOKEN=<token> \
  sh -s - agent --node-ip <ip-node-b> --flannel-iface <interface>
```

`--flannel-iface` memastikan lalu lintas antar-pod lewat jaringan privat. Traefik dan ServiceLB
dimatikan karena layanan dibuka lewat NodePort.

## 2. Menghubungkan kubectl

Salin `/etc/rancher/k3s/k3s.yaml` dari node-a ke mesin lokal, ganti `127.0.0.1` di dalamnya dengan
`<ip-node-a>`, lalu arahkan `KUBECONFIG` ke file tersebut:

```bash
scp <user>@<ip-node-a>:/etc/rancher/k3s/k3s.yaml ~/.kube/bnpb-k3s.yaml
sed -i 's/127.0.0.1/<ip-node-a>/' ~/.kube/bnpb-k3s.yaml
export KUBECONFIG=~/.kube/bnpb-k3s.yaml
kubectl get nodes -o wide
```

Kedua node harus berstatus `Ready`.

## 3. Membuat secret

Isi `k8s/secrets.env` dengan nilai acak untuk setiap kunci di `k8s/secrets.env.example`.

Bash:

```bash
grep -E '^[A-Z0-9_]+=$' k8s/secrets.env.example | while read -r line; do
  echo "${line}$(openssl rand -hex 32)"
done > k8s/secrets.env
```

PowerShell:

```powershell
$rng = [Security.Cryptography.RandomNumberGenerator]::Create()
Get-Content k8s/secrets.env.example | Where-Object { $_ -match '^[A-Z0-9_]+=$' } | ForEach-Object {
  $bytes = New-Object byte[] 32; $rng.GetBytes($bytes)
  $_ + (($bytes | ForEach-Object { $_.ToString('x2') }) -join '')
} | Out-File -Encoding ascii k8s/secrets.env
```

Satu nilai dipakai kedua pihak (misalnya `BMKG_API_KEY` untuk BMKG dan Aggregator), sehingga
kredensial antarkomponen selalu cocok.

## 4. Image

Node k3s tidak bisa melihat image Docker di mesin lokal, jadi image di-build secara lokal, di-push ke
registry, lalu ditarik oleh setiap node. Semua image disimpan di satu repository private dengan tag per
komponen. Nama repository diatur pada bagian `images` di `k8s/kustomization.yaml`; ganti `newName`
jika memakai repository lain.

| Tag | Dockerfile | Context |
| --- | --- | --- |
| `bmkg` | `bmkg/Dockerfile` | `bmkg` |
| `pvmbg` | `pvmbg/Dockerfile` | `pvmbg` |
| `aggregator` | `bnpb/cmd/aggregator/Dockerfile` | `bnpb` |
| `api` | `bnpb/cmd/api/Dockerfile` | `bnpb` |
| `auth` | `bnpb/cmd/auth/Dockerfile` | `bnpb` |
| `consumer-1`, `consumer-2`, `consumer-3` | `bnpb/cmd/consumer-N/Dockerfile` | `bnpb` |
| `broker-init` | `bnpb/broker/init.Dockerfile` | `bnpb` |

Build dan push, dengan `<repo>` berupa `<user-dockerhub>/<nama-repository>`:

```text
docker build -t <repo>:bmkg -f bmkg/Dockerfile bmkg
docker build -t <repo>:pvmbg -f pvmbg/Dockerfile pvmbg
docker build -t <repo>:aggregator -f bnpb/cmd/aggregator/Dockerfile bnpb
docker build -t <repo>:api -f bnpb/cmd/api/Dockerfile bnpb
docker build -t <repo>:auth -f bnpb/cmd/auth/Dockerfile bnpb
docker build -t <repo>:consumer-1 -f bnpb/cmd/consumer-1/Dockerfile bnpb
docker build -t <repo>:consumer-2 -f bnpb/cmd/consumer-2/Dockerfile bnpb
docker build -t <repo>:consumer-3 -f bnpb/cmd/consumer-3/Dockerfile bnpb
docker build -t <repo>:broker-init -f bnpb/broker/init.Dockerfile bnpb
docker push --all-tags <repo>
```

Repository bersifat private, jadi cluster butuh token baca (Docker Hub: *Personal access token*
Read-only):

```text
kubectl apply -f k8s/namespace.yaml
kubectl -n sistem-informasi-kebencanaan-bnpb create secret docker-registry dockerhub-pull --docker-server=https://index.docker.io/v1/ --docker-username=<user-dockerhub> --docker-password=<token>
```

`k8s/serviceaccount.yaml` memasang secret ini ke semua pod di namespace.

Workflow `.github/workflows/images.yml` menyediakan alternatif build lewat GitHub Actions ke GHCR dan
hanya dijalankan manual.

## 5. Deploy

```text
kubectl apply -k k8s
kubectl -n sistem-informasi-kebencanaan-bnpb get pods -o wide -w
```

Semua pod harus `Running` dan `Ready`, kecuali `broker-init` yang berakhir `Completed`. Job ini terhapus
otomatis dua menit kemudian dan berjalan lagi pada `apply` berikutnya, karena provisioning broker aman
diulang.

Pod memakai `imagePullPolicy: IfNotPresent`: node memakai image yang sudah ada dan hanya menarik dari
registry jika belum ada, sehingga pod yang dipindahkan saat node mati tidak bergantung pada registry.
Akibatnya, image baru dengan tag yang sama tidak ditarik otomatis. Untuk memperbarui satu komponen,
muat image barunya ke setiap node (lihat [Tanpa registry](#tanpa-registry)), lalu jalankan ulang pod:

```text
kubectl -n sistem-informasi-kebencanaan-bnpb rollout restart deployment/<nama>
```

### Endpoint

| Layanan | Alamat |
| --- | --- |
| Client-Facing API | `http://<ip-node>:30080` |
| Aggregator / Audit API | `http://<ip-node>:30083` |
| RabbitMQ management | `kubectl -n sistem-informasi-kebencanaan-bnpb port-forward svc/rabbitmq 15672`, lalu <http://localhost:15672> |

`<ip-node>` boleh IP node mana pun.

## 6. Tester Agent

```text
kubectl apply -f https://raw.githubusercontent.com/susTuna/if4031-tester-agent/ebb55261711123a9a211739ffbf140a275caa6da/manifest.yaml
kubectl exec -n tester-agent daemonset/tester-agent -- /app list
kubectl exec -n tester-agent daemonset/tester-agent -- /app run practice <nama-skenario>
kubectl logs -n tester-agent daemonset/tester-agent -f
```

## Perintah yang sering dipakai

```text
# Ubah jumlah Aggregator (P2.6: 3 -> 5 -> 2 saat load testing)
kubectl -n sistem-informasi-kebencanaan-bnpb scale deployment aggregator --replicas=5

# Menambah consumer-3 tanpa mengubah producer
kubectl -n sistem-informasi-kebencanaan-bnpb scale statefulset consumer-3 --replicas=1

# Log semua replika satu komponen
kubectl -n sistem-informasi-kebencanaan-bnpb logs -l app.kubernetes.io/name=aggregator --prefix -f

# Menghapus seluruh sistem beserta datanya
kubectl delete -k k8s
```

## Tanpa registry

Image juga bisa dimuat langsung ke **setiap** node tanpa registry, misalnya saat registry membatasi
permintaan (`429 Too Many Requests`) atau internet tidak tersedia. Sertakan image pihak ketiga agar
node tidak perlu menariknya:

```text
docker save -o images.tar <repo> postgres:18.6-alpine rabbitmq:4.3.5-management-alpine
scp images.tar <user>@<ip-node>:/tmp/
ssh -t <user>@<ip-node> "sudo k3s ctr images import /tmp/images.tar && rm /tmp/images.tar"
```
