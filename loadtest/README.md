# Load tests (P2.6–8)

Run k6 on the host or any machine that can reach the API NodePort (default `192.168.56.10:30080`).

```powershell
k6 run loadtest/k6-api-sustained.js
```

Environment variables:

| Variable | Default | Meaning |
| --- | --- | --- |
| `API_BASE` | `http://192.168.56.10:30080` | Client-facing API |
| `FIELD_ID` / `FIELD_SECRET` | `field-team` / from `k8s/secrets.env` | Bearer token for hazards |

Scale Aggregator replicas during the run: `kubectl scale deployment/aggregator -n sistem-informasi-kebencanaan-bnpb --replicas=5`