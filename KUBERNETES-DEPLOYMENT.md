# Kubernetes Deployment (Milestone 2)

The whole BNPB system runs on a Kubernetes cluster ([k3s](https://docs.k3s.io/)) with at least two
nodes, in the `sistem-informasi-kebencanaan-bnpb` namespace. Once the preparation below is done, the
system is deployed with a single command:

```text
kubectl apply -k k8s
```

Run every command from the repository root.

## Topology

| Node | Role |
| --- | --- |
| `node-a` | control plane + worker |
| `node-b` | worker |

Nodes can be VMs or native Linux machines as long as each has its own kernel. Both nodes must reach
each other over one private network and have internet access. Scenarios that take a node down are
only applied to the worker that is not the control plane (`node-b`).

Manifests live in `k8s/`. Plain configuration is in `k8s/config/*.env`; secrets are in
`k8s/secrets.env`, which is not tracked by Git.

## Requirements

- Two Linux machines (Ubuntu Server 24.04 recommended) with fixed private IPs, at least 2 vCPU and
  4 GB RAM each.
- `kubectl` and Docker on the machine used to build and deploy.
- A container registry account (Docker Hub) to hold the images.

## 1. Install k3s

On **node-a**, replace `<ip-node-a>` and `<interface>` (the interface that holds the private IP; check
with `ip -br -4 addr`):

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

On **node-b**, using the token printed by the last command:

```bash
curl -sfL https://get.k3s.io | \
  K3S_URL=https://<ip-node-a>:6443 K3S_TOKEN=<token> \
  sh -s - agent --node-ip <ip-node-b> --flannel-iface <interface>
```

`--flannel-iface` keeps pod-to-pod traffic on the private network. Traefik and ServiceLB are disabled
because services are exposed through NodePort.

## 2. Connect kubectl

Copy `/etc/rancher/k3s/k3s.yaml` from node-a to the local machine, replace `127.0.0.1` inside it with
`<ip-node-a>`, then point `KUBECONFIG` at the file:

```bash
scp <user>@<ip-node-a>:/etc/rancher/k3s/k3s.yaml ~/.kube/bnpb-k3s.yaml
sed -i 's/127.0.0.1/<ip-node-a>/' ~/.kube/bnpb-k3s.yaml
export KUBECONFIG=~/.kube/bnpb-k3s.yaml
kubectl get nodes -o wide
```

Both nodes must be `Ready`. The kubeconfig holds admin credentials: keep it outside the repository
and never share it.

## 3. Create the secrets

Fill `k8s/secrets.env` with a random value for every key in `k8s/secrets.env.example`.

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

One value is used by both sides (for example `BMKG_API_KEY` for BMKG and the Aggregator), so the
credentials between components always match.

## 4. Images

k3s nodes cannot see Docker images on the local machine, so images are built locally, pushed to a
registry, and pulled by each node. All images live in a single private repository with one tag per
component. The repository name is set in the `images` section of `k8s/kustomization.yaml`; change
`newName` to use a different repository.

| Tag | Dockerfile | Context |
| --- | --- | --- |
| `bmkg` | `bmkg/Dockerfile` | `bmkg` |
| `pvmbg` | `pvmbg/Dockerfile` | `pvmbg` |
| `aggregator` | `bnpb/cmd/aggregator/Dockerfile` | `bnpb` |
| `api` | `bnpb/cmd/api/Dockerfile` | `bnpb` |
| `auth` | `bnpb/cmd/auth/Dockerfile` | `bnpb` |
| `consumer-1`, `consumer-2`, `consumer-3` | `bnpb/cmd/consumer-N/Dockerfile` | `bnpb` |
| `broker-init` | `bnpb/broker/init.Dockerfile` | `bnpb` |

Build and push, where `<repo>` is `<dockerhub-user>/<repository-name>`:

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

The repository is private, so the cluster needs a read token (Docker Hub: read-only *Personal access
token*):

```text
kubectl apply -f k8s/namespace.yaml
kubectl -n sistem-informasi-kebencanaan-bnpb create secret docker-registry dockerhub-pull --docker-server=https://index.docker.io/v1/ --docker-username=<dockerhub-user> --docker-password=<token>
```

`k8s/serviceaccount.yaml` attaches this secret to every pod in the namespace.

The `.github/workflows/images.yml` workflow offers an alternative that builds through GitHub Actions
into GHCR; it only runs manually.

## 5. Deploy

```text
kubectl apply -k k8s
kubectl -n sistem-informasi-kebencanaan-bnpb get pods -o wide -w
```

Every pod must be `Running` and `Ready`, except `broker-init`, which ends as `Completed`. That job is
deleted automatically two minutes later and runs again on the next `apply`, because broker
provisioning is safe to repeat.

Pods use `imagePullPolicy: IfNotPresent`: a node uses the image it already has and only pulls from the
registry when it is missing, so a pod rescheduled after a node failure does not depend on the registry.
As a consequence, a new image under the same tag is not pulled automatically. To update one component,
load the new image into every node (see [Without a registry](#without-a-registry)), then restart the
pods:

```text
kubectl -n sistem-informasi-kebencanaan-bnpb rollout restart deployment/<name>
```

### Endpoints

| Service | Address |
| --- | --- |
| Client-Facing API | `http://<node-ip>:30080` |
| Aggregator / Audit API | `http://<node-ip>:30083` |
| RabbitMQ management | `kubectl -n sistem-informasi-kebencanaan-bnpb port-forward svc/rabbitmq 15672`, then <http://localhost:15672> |

`<node-ip>` can be the IP of any node.

## 6. Observability

Prometheus and Grafana run in the separate `monitoring` namespace so Appendix B
limits and the Tester Agent stay focused on application pods:

```text
kubectl apply -k k8s/monitoring
```

| Service | Address |
| --- | --- |
| Prometheus | `http://<node-ip>:30900` |
| Grafana | `http://<node-ip>:30300` (anonymous Viewer; admin/`bnpb-observability`) |

BNPB Go services expose `GET /metrics`. Pods with the `prometheus.io/scrape`
annotation are discovered automatically. Rebuild and reload those images before
expecting series such as `http_requests_total` and `http_request_duration_seconds`.

## 7. Tester Agent

```text
kubectl apply -f https://raw.githubusercontent.com/susTuna/if4031-tester-agent/ebb55261711123a9a211739ffbf140a275caa6da/manifest.yaml
kubectl exec -n tester-agent daemonset/tester-agent -- /app list
kubectl exec -n tester-agent daemonset/tester-agent -- /app run practice <scenario-name>
kubectl logs -n tester-agent daemonset/tester-agent -f
```

## Common commands

```text
# Change the number of Aggregator replicas (P2.6: 3 -> 5 -> 2 during load testing)
kubectl -n sistem-informasi-kebencanaan-bnpb scale deployment aggregator --replicas=5

# Add consumer-3 without changing the producer
kubectl -n sistem-informasi-kebencanaan-bnpb scale statefulset consumer-3 --replicas=1

# Logs of every replica of one component
kubectl -n sistem-informasi-kebencanaan-bnpb logs -l app.kubernetes.io/name=aggregator --prefix -f

# Remove the whole system together with its data
kubectl delete -k k8s
```

## Without a registry

Images can also be loaded straight into **every** node without a registry, for example when the
registry rate-limits requests (`429 Too Many Requests`) or there is no internet. Include the
third-party images so the nodes do not need to pull them:

```text
docker save -o images.tar <repo> postgres:18.6-alpine rabbitmq:4.3.5-management-alpine
scp images.tar <user>@<node-ip>:/tmp/
ssh -t <user>@<node-ip> "sudo k3s ctr images import /tmp/images.tar && rm /tmp/images.tar"
```
