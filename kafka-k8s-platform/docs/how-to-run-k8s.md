# How to run — EV fleet telematics (Kafka + Kind + KEDA)

Complete PowerShell runbook: **prerequisites → Kind stack → observability → load → scale → tear down**.

**Recommended local mode:** Kind + KEDA (workers auto-scale on Kafka lag).  
**Compose full stack** = fixed replicas only (manual `--scale`) — see end of this file.

Cloud production: [`production-aws.md`](production-aws.md) · [`production-azure.md`](production-azure.md)

---

## 0. What runs where (Kind mode)

```
Your laptop
├── Docker Compose INFRA ONLY
│     postgres :5433
│     kafka    :19092   (advertised host.docker.internal:19092)
│     minio    :9010
│
├── Kind cluster "dap"
│     dap-api, dap-iot-edge
│     dap-ingest / etl / embed / index   (0..N via KEDA)
│     dap-prometheus, dap-grafana       (in-cluster metrics)
│     KEDA operator (namespace keda)
│
└── React console (host)
      npm run dev → http://localhost:5174
      Vite proxies /v1 → http://localhost:8080  (kubectl port-forward)
```

**Do not** run Compose `api/ingest/etl/...` while Kind owns those consumer groups.

---

## 1. Prerequisites (install once)

| Tool | Why | Install (Windows) |
|------|-----|-------------------|
| **Docker Desktop** | Kind node + infra containers | https://www.docker.com/products/docker-desktop |
| **kubectl** | Cluster CLI | Docker Desktop, or `winget install Kubernetes.kubectl` |
| **kind** | Local Kubernetes | `winget install Kubernetes.kind` |
| **Git** | Clone / push | `winget install Git.Git` |
| **Node.js 20+** | Fleet console | `winget install OpenJS.NodeJS.LTS` |
| **Python 3.12+** (optional) | Tests / DLQ replay | `winget install Python.Python.3.12` |
| **curl.exe** | API checks | Built into Windows 10+ |

Verify:

```powershell
docker version
kubectl version --client
kind version
node -v
git --version
```

If `kind` is not on PATH:

```powershell
Get-ChildItem "$env:LOCALAPPDATA\Microsoft\WinGet\Packages\Kubernetes.kind_*\kind.exe"
```

Hardware: **16 GB RAM** recommended; Docker Desktop memory ≥ **8 GB**.

```powershell
cd D:\Ravinder_Data\Projects\distributed-ai-platform\kafka-k8s-platform
```

---

## 2. Start infra only (Compose) — no app workers

```powershell
cd kafka-k8s-platform

# Tear down any previous full Compose app stack
docker compose -f docker-compose.yml -f deploy/docker-compose.observability.yml down --remove-orphans

# Infra only — Kafka / Postgres / MinIO for Kind pods
docker compose -f deploy/docker-compose.infra.yml up -d
docker compose -f deploy/docker-compose.infra.yml ps
```

Topics are created by the `create-topics` job in that file.

---

## 3. Create Kind cluster + install KEDA (once per machine)

```powershell
kind create cluster --name dap
kubectl config use-context kind-dap

kubectl apply --server-side -f https://github.com/kedacore/keda/releases/download/v2.16.1/keda-2.16.1.yaml
kubectl wait --for=condition=available deployment/keda-operator -n keda --timeout=180s
kubectl get pods -n keda
```

---

## 4. Build images, load into Kind, apply app + KEDA manifests

```powershell
.\scripts\kind-deploy.ps1
```

That script builds `dap/*:local`, loads them into Kind, and applies base/apps/keda.  
Workers start at **0 replicas**; KEDA scales when lag rises.

### 4b. Observability (Prometheus + Grafana in Kind)

```powershell
kubectl apply -f deploy/k8s/observability/prometheus-grafana.yaml
kubectl rollout status deploy/dap-prometheus -n dap --timeout=120s
kubectl rollout status deploy/dap-grafana -n dap --timeout=120s
```

Workers expose metrics on **:9100**; API on **:8080/metrics**. Prometheus scrapes in-cluster Services (`dap-*-metrics`).

---

## 5. Expose API + Grafana + Prometheus + console

```powershell
# Leave these running (separate terminals)
kubectl -n dap port-forward svc/dap-api 8080:8080
kubectl -n dap port-forward svc/dap-grafana 3000:3000
kubectl -n dap port-forward svc/dap-prometheus 9090:9090

cd console
npm install
npm run dev
# Open http://127.0.0.1:5174
```

| Surface | URL |
|---------|-----|
| Fleet console | http://127.0.0.1:5174 |
| API | http://localhost:8080/health |
| Grafana | http://localhost:3000 (`dap` / `dap`) — dashboard **DAP Kafka Pipeline** |
| Prometheus | http://localhost:9090 |

```powershell
curl.exe -s http://localhost:8080/health
curl.exe -s http://localhost:8080/v1/pipeline/overview
kubectl get pods -n dap
```

---

## 6. Generate throughput (activate vehicles)

More activated vehicles + faster publish interval = more Kafka messages = more lag = more worker pods.

```powershell
$tmp = "$env:TEMP\dap-vehicle.json"
1..30 | ForEach-Object {
  $vid = "EV-SCALE-$_"
  @{ vehicle_id = $vid; vin = ("VINSCALE{0:D4}" -f $_); make = "Demo"; model = "EV" } |
    ConvertTo-Json -Compress | Set-Content $tmp -Encoding ascii
  curl.exe -s -X POST "http://localhost:8080/v1/fleets/fleet-east-01/vehicles" `
    -H "content-type: application/json" --data-binary "@$tmp" | Out-Null
  curl.exe -s -X POST "http://localhost:8080/v1/fleets/fleet-east-01/vehicles/$vid/activate" | Out-Null
}
curl.exe -s http://localhost:8080/v1/pipeline/overview
```

Or use the console: Provision → **Activate stream**.

---

## 7. Watch scale up / scale down

**Terminal A — pods**

```powershell
kubectl get pods -n dap -w
```

**Terminal B — HPA (lag metric / threshold)**

```powershell
kubectl get hpa -n dap -w
# Example: keda-hpa-dap-ingest-scaler   9/5 (avg)  → wants more pods
```

**Terminal C — Kafka consumer lag**

```powershell
docker compose -f deploy/docker-compose.infra.yml exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh `
  --bootstrap-server kafka:9092 --describe --group dap-ingest

docker compose -f deploy/docker-compose.infra.yml exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh `
  --bootstrap-server kafka:9092 --describe --group dap-etl

docker compose -f deploy/docker-compose.infra.yml exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh `
  --bootstrap-server kafka:9092 --describe --group dap-embed
```

**ScaledObjects**

```powershell
kubectl get scaledobject -n dap
kubectl describe scaledobject dap-ingest-scaler -n dap
```

**Grafana / Prometheus (while port-forwards are up)**

- Grafana → **DAP Kafka Pipeline**: messages processed, lag-related panels
- Prometheus → `dap_messages_processed_total`, `up{job=~"dap-.*"}`

### Expected phases

| Phase | What you see |
|-------|----------------|
| Idle | ingest/etl/embed/index at **0** pods |
| Activate vehicles | iot-edge publishes → lag on topics |
| Scale **up** | KEDA sets replicas > 0; HPA may climb (e.g. ingest `9/5` → 2 pods) |
| Catch up | Lag drops; after `cooldownPeriod` (30–45s) replicas fall |
| Quiet | Workers return toward **0** |

---

## 8. Increase throughput so workers scale harder

KEDA formula (conceptually): **more lag vs `lagThreshold` → more replicas** (capped by `maxReplicaCount` and **topic partitions**).

### A) Produce more messages (best for demos)

| Knob | Where | Effect |
|------|--------|--------|
| Activate more vehicles | API / console | More publishers |
| Faster publish | ConfigMap `DAP_IOT_PUBLISH_INTERVAL_SECONDS` (now `1`) | Higher msgs/sec |
| Higher alert rate | `DAP_IOT_ALERT_PROBABILITY` (now `0.35`) | More embed/index work |

```powershell
kubectl -n dap edit configmap dap-config
# set DAP_IOT_PUBLISH_INTERVAL_SECONDS: "1"   (or keep 1)
# set DAP_IOT_ALERT_PROBABILITY: "0.5"
kubectl rollout restart deploy/dap-iot-edge -n dap
```

### B) Make consumers fall behind (more lag → more pods)

| Knob | Where | Effect |
|------|--------|--------|
| Smaller batches | `DAP_KAFKA_MAX_POLL_RECORDS: "8"` | Slower drain → lag rises |
| Lower `lagThreshold` | `deploy/k8s/keda/scaledobjects.yaml` | Same lag triggers more replicas |
| Raise `maxReplicaCount` | same file | Allow 5 → 8 etc. |

```powershell
# Edit thresholds / max replicas, then:
kubectl apply -f deploy/k8s/keda/scaledobjects.yaml
```

Example aggressive demo values:

```yaml
lagThreshold: "3"        # was 5 — scale sooner
maxReplicaCount: 8       # allow more pods
pollingInterval: 5
cooldownPeriod: 30
minReplicaCount: 0
```

### C) Kafka partitions limit parallelism

Topics are created with **6 partitions**. You cannot usefully run more consumer pods than partitions for that group (extra pods sit idle). To go beyond 6:

```powershell
# only if you recreate topics / use a higher partition count at create time
docker compose -f deploy/docker-compose.infra.yml exec -T kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server kafka:9092 --describe --topic dap.iot.telemetry
```

### D) Scale-down demo

```powershell
kubectl scale deploy/dap-iot-edge -n dap --replicas=0
kubectl get deploy -n dap -w
# after cooldown, workers drop toward 0

kubectl scale deploy/dap-iot-edge -n dap --replicas=1
```

---

## 9. Analyse clusters, pods, Kafka

```powershell
# Context / clusters
kubectl config get-contexts
kubectl config current-context          # should be kind-dap
kind get clusters

# Cluster nodes
kubectl get nodes -o wide

# Everything in dap
kubectl get all -n dap
kubectl get deploy,pods,svc,hpa,scaledobject -n dap -o wide

# Pod details / events
kubectl describe pod -n dap -l app=dap-ingest
kubectl get events -n dap --sort-by=.lastTimestamp | Select-Object -Last 30

# Logs
kubectl logs -n dap -l app=dap-iot-edge --tail=50
kubectl logs -n dap -l app=dap-ingest --tail=50
kubectl logs -n dap -l app=dap-etl --tail=50
kubectl logs -n keda deploy/keda-operator --tail=50

# KEDA
kubectl get pods -n keda
kubectl describe scaledobject -n dap dap-ingest-scaler

# Images inside Kind node
docker exec -it dap-control-plane crictl images | Select-String "dap/"

# Compose infra
docker compose -f deploy/docker-compose.infra.yml ps
docker compose -f deploy/docker-compose.infra.yml logs kafka --tail=30
```

---

## 10. Tear down — pods, cluster, Compose, images

Use the level you need.

### A) Stop DAP apps only (keep Kind + infra)

```powershell
kubectl delete -f deploy/k8s/keda/scaledobjects.yaml
kubectl delete -f deploy/k8s/apps/deployments.yaml
kubectl delete -f deploy/k8s/observability/prometheus-grafana.yaml
# namespace/config can stay:
# kubectl delete -f deploy/k8s/base/namespace-config.yaml
```

### B) Delete Kind cluster (pods + KEDA + Prom/Grafana in that cluster gone)

```powershell
# Ctrl+C any port-forward first
kind delete cluster --name dap
```

### C) Stop Compose infra

```powershell
docker compose -f deploy/docker-compose.infra.yml down
# or full project compose + volumes:
docker compose down -v
```

### D) Remove local DAP Docker images

```powershell
docker images "dap/*"
docker rmi dap/api:local dap/iot-edge:local dap/ingest:local dap/etl:local dap/embed:local dap/index:local
```

### E) Full clean (everything)

```powershell
kind delete cluster --name dap
docker compose -f deploy/docker-compose.infra.yml down -v
docker compose down -v
docker rmi dap/api:local dap/iot-edge:local dap/ingest:local dap/etl:local dap/embed:local dap/index:local
docker image prune -f
```

### F) Restart later (short path)

```powershell
docker compose -f deploy/docker-compose.infra.yml up -d
kind create cluster --name dap
kubectl apply --server-side -f https://github.com/kedacore/keda/releases/download/v2.16.1/keda-2.16.1.yaml
kubectl wait --for=condition=available deployment/keda-operator -n keda --timeout=180s
.\scripts\kind-deploy.ps1
kubectl apply -f deploy/k8s/observability/prometheus-grafana.yaml
kubectl -n dap port-forward svc/dap-api 8080:8080
kubectl -n dap port-forward svc/dap-grafana 3000:3000
```

---

## 11. Public / frontend access (local vs cloud)

### Today (local Kind)

| Piece | How it is exposed |
|-------|-------------------|
| `dap-api` Service | **ClusterIP** — inside the cluster only |
| Browser / curl / console | **`kubectl port-forward`** → `http://localhost:8080` |
| Grafana / Prometheus | Port-forward `:3000` / `:9090` |
| React console | Host `npm run dev` → `:5174` (Vite proxies `/v1` → API) |
| Workers | **Not** public — Kafka/Postgres only |
| Internet hostname | **None** on Kind |

```
Browser ──► localhost:5174 (Vite console on host)
                │ proxy /v1
                ▼
            localhost:8080  ◄── port-forward ──  ClusterIP dap-api
```

Port-forward is a **developer tunnel** to your laptop only.

### Production (how users reach the app)

```
Internet users
    │
    ▼
Cloud load balancer + TLS (ALB / NLB / Azure App Gateway / Front Door)
    │
    ▼
Ingress Controller (or Service type LoadBalancer)
    │
    ▼
Service dap-api / dap-console (ClusterIP)
```

| Option | Use when |
|--------|----------|
| **EKS / AKS + Ingress** | Full K8s (recommended for this project) |
| **ECS / Azure Container Apps** | Managed containers without operating nodes |
| **EC2 / Azure VM + Docker** | Small demo; you manage OS, TLS, restarts |

Details: [`production-aws.md`](production-aws.md) · [`production-azure.md`](production-azure.md).

Local options: keep port-forward, or install ingress-nginx + apply `deploy/k8s/apps/ingress.yaml` / `console.yaml`.

---

## 12. APIs (quick reference)

| Method | Path | Role |
|--------|------|------|
| GET | `/health` | Liveness |
| GET | `/metrics` | Prometheus metrics |
| POST | `/v1/auth/token` | Mint JWT from API key (needs `DAP_JWT_SECRET`) |
| POST | `/v1/admin/dlq/replay` | Replay DLQ payload to source topic |
| GET | `/v1/pipeline/overview` | Pipeline counts |
| GET/POST | `/v1/fleets` | List / create fleets |
| POST | `/v1/fleets/{id}/vehicles` | Provision |
| POST | `/v1/fleets/{id}/vehicles/{vid}/activate` | Start stream |
| GET | `/v1/fleets/{id}/vehicles` | Vehicles |
| GET | `/v1/fleets/{id}/telemetry` | Events |
| GET | `/v1/fleets/{id}/alerts` | Alerts |
| POST | `/v1/search` | Semantic alert search |

---

## 13. Production enhancements (additive — do not remove the demo path)

Observability install is in **§4b / §5**. Cloud cutovers: [`production-aws.md`](production-aws.md) · [`production-azure.md`](production-azure.md).

### Security (Phase B)

- Default: `DAP_AUTH_REQUIRED=false` (demo unchanged).
- Optional: set `DAP_API_KEYS`, `DAP_JWT_SECRET`, `DAP_AUTH_REQUIRED=true`.
- Apply example secret: `kubectl apply -f deploy/k8s/base/secrets.example.yaml` (change values first).
- Prod overlay wires Secret into API: `kubectl apply -k deploy/k8s/overlays/prod`
- Kafka SASL/TLS: document placeholders in secrets; Kind demo stays PLAINTEXT.

### Ingress + console (Phase C)

```powershell
# After Kind + ingress-nginx:
kubectl apply -f deploy/k8s/apps/console.yaml
kubectl apply -f deploy/k8s/apps/ingress.yaml
# Or keep:
kubectl -n dap port-forward svc/dap-console 5174:80
kubectl -n dap port-forward svc/dap-api 8080:8080
```

### CI + tests (Phase D)

```powershell
pip install -e "./packages/dap_common[dev]"
pytest packages/dap_common/tests -q
.\scripts\smoke-test.ps1
```

GitHub Actions: `.github/workflows/dap-ci.yml` (unit + Compose smoke).

**KEDA demo regression checklist:** Activate vehicles → `kubectl get hpa` shows lag metric → ingest replicas rise → deactivate / pause edge → replicas fall after cooldown.

### MQTT edge (Phase E) — optional beside simulator

```powershell
docker compose -f docker-compose.yml -f deploy/docker-compose.mqtt.yml --profile mqtt up -d
# Publish JSON to MQTT topic fleet/{fleet}/vehicle/{id}/telemetry
# mqtt-bridge forwards into dap.iot.telemetry
# Leave iot_edge running for Activate-based simulator demos
```

### Overlays, keys, DLQ replay (Phase F)

```powershell
kubectl apply -k deploy/k8s/overlays/dev
# prod-shaped (auth on, HF embeddings flag — adjust endpoints first):
# kubectl apply -k deploy/k8s/overlays/prod

# Partition keys: producers use vehicle_id (ordering per vehicle within a partition).
python scripts/dlq_replay.py --bootstrap localhost:19092 --dry-run
# or API: POST /v1/admin/dlq/replay
```

---

## 14. Troubleshooting

| Symptom | Fix |
|---------|-----|
| Kafka `Connection refused` from pods | Advertised listener must be `host.docker.internal:19092`, not `localhost` |
| `api_port` / `tcp://10.96...` crash | Deployments use `enableServiceLinks: false` |
| KEDA topic errors | Recreate topics (`kafka-init`) after Kafka recreate |
| Workers never scale | Stop Compose ingest/etl/embed (same consumer groups) |
| No lag | Confirm activate + `kubectl logs -l app=dap-iot-edge` shows `capture_started` |
| HPA stuck `<unknown>` | Wait for KEDA metrics; check `kubectl logs -n keda deploy/keda-operator` |
| port-forward fails | Ensure `dap-api` pod is Ready: `kubectl get pods -n dap` |
| 401 after enabling auth | Send `X-API-Key` or `Authorization: Bearer <jwt>` |
| Grafana Unauthorized / empty dash | Login `dap`/`dap`; open **DAP Kafka Pipeline**; wait for scrapes |
| Prometheus targets down | `kubectl get svc -n dap`; confirm metrics Services / pod `:9100` |
| Compose + Kind both running workers | Stop Compose apps — same consumer groups fight |

---

## Compose-only mode (no Kind — manual scale)

Useful for UI/API smoke tests **without** KEDA. Replicas are fixed.

```powershell
docker compose down -v
docker compose -f docker-compose.yml -f deploy/docker-compose.observability.yml up -d --build `
  --scale ingest=2 --scale etl=2 --scale embed=2

# Grafana http://localhost:3000  Prometheus http://localhost:9090  API :8080
```

**Does not** auto-scale on lag. For KEDA demos, tear this down and use Kind (§2–§5).

---

## Doc map

| Doc | Purpose |
|-----|---------|
| This file | Local prerequisites + Kind/KEDA + Prom/Grafana |
| [`production-aws.md`](production-aws.md) | AWS (EKS / ECS / EC2), GitHub → images → cluster |
| [`production-azure.md`](production-azure.md) | Azure (AKS / Container Apps / VM), same workflow |
| [`keda-regression-checklist.md`](keda-regression-checklist.md) | Activate → lag → scale regression |
