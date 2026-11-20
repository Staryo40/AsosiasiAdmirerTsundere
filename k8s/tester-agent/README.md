# Tester Agent (A2-04)

Install the official image unchanged:

```powershell
kubectl apply -f https://raw.githubusercontent.com/susTuna/if4031-tester-agent/ebb55261711123a9a211739ffbf140a275caa6da/manifest.yaml
kubectl exec -n tester-agent daemonset/tester-agent -- /app list
kubectl exec -n tester-agent daemonset/tester-agent -- /app run practice <scenario>
kubectl logs -n tester-agent daemonset/tester-agent -f
```