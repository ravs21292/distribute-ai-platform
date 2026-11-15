# EV Fleet Telematics Platform (Kafka + Kubernetes)

Continuous **IoT streaming** for electric vehicle fleets — not a file-upload demo.

## How sensors get into this platform

```
EV sensors (SoC, speed, battery temp, GPS, charging state)
        │
        ▼
On-vehicle TCU / telematics unit
        │
        ▼
Edge gateway (MQTT/HTTPS bridge in production)
        │  continuous publish
        ▼
Kafka: dap.iot.telemetry     ← platform intake bus
        │
        ├─► Ingest  → register fleet/vehicle, forward
        ├─► ETL     → clean + PII redact notes/alerts → Postgres
        ├─► Embed   → alert narratives → pgvector
        └─► Index   → coverage stats
```

Locally, `services/iot_edge` **simulates** the edge gateway for N vehicles publishing every few seconds.

## Layout

```
services/
  iot_edge/   # continuous EV sensor producer (edge simulator)
  ingest/     # consume dap.iot.telemetry
  etl/        # clean + PII + store telematics_events / alerts
  embed/      # embed alerts
  index/      # fleet pipeline stats
  api/        # fleet status, telemetry, semantic alert search
packages/dap_common/   # EvSensorReading model + Kafka contracts
```

## Data model (EV telematics)

| Entity | Fields (selected) |
|--------|-------------------|
| Fleet | id, name, classification, pii_policy |
| Vehicle | id, vin, last SoC/speed/GPS |
| TelematicsEvent | speed, SoC, SoH, temps, odometer, charging_state, power, GPS, alert_* , note (PII-safe) |
| AlertDocument | searchable text after PII redaction |
| AlertEmbedding | pgvector vectors |

## Console (React / Redux / TypeScript)

```powershell
cd console
npm install
npm run dev
```

Open http://localhost:5174 — onboard vehicle → Activate stream → watch live telemetry.

Flow:
1. Create/select fleet  
2. Provision vehicle → device token + MQTT plugin config  
3. **Activate stream** → API publishes `dap.edge.commands` → `iot-edge` starts continuous Kafka publish  
4. Events land in Postgres; panel refreshes every 4s  

Wait ~30s, then:

```powershell
curl.exe -s http://localhost:8080/v1/pipeline/overview
curl.exe -s http://localhost:8080/v1/fleets
curl.exe -s http://localhost:8080/v1/fleets/fleet-east-01/vehicles
curl.exe -s "http://localhost:8080/v1/fleets/fleet-east-01/telemetry?limit=5"
```

Search alerts:

```powershell
'{"query":"battery high temperature derate","fleet_id":"fleet-east-01","limit":5}' | Set-Content -Encoding ascii search.json
curl.exe -s -X POST http://localhost:8080/v1/search -H "content-type: application/json" --data-binary "@search.json"
```

## Kubernetes + KEDA (auto scale from lag)

Compose `--scale` is **manual**. To watch workers scale up/down from Kafka lag on your laptop:

```powershell
# infra only
docker compose -f deploy/docker-compose.infra.yml up -d

kind create cluster --name dap
kubectl apply --server-side -f https://github.com/kedacore/keda/releases/download/v2.16.1/keda-2.16.1.yaml
.\scripts\kind-deploy.ps1
kubectl apply -f deploy/k8s/observability/prometheus-grafana.yaml
kubectl -n dap port-forward svc/dap-api 8080:8080
kubectl -n dap port-forward svc/dap-grafana 3000:3000
```

Watch:

```powershell
kubectl get pods -n dap -w
kubectl get hpa -n dap -w
docker compose -f deploy/docker-compose.infra.yml exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group dap-ingest
```

Grafana: http://localhost:3000 (`dap` / `dap`). Prometheus: http://localhost:9090.

Full steps (prerequisites → Kind + KEDA → Prometheus/Grafana → scale → tear down):  
[`docs/how-to-run-k8s.md`](docs/how-to-run-k8s.md).

**Cloud production (GitHub → images → cluster → public users):**  
[`docs/production-aws.md`](docs/production-aws.md) (EKS / ECS / EC2) ·  
[`docs/production-azure.md`](docs/production-azure.md) (AKS / Container Apps / VM).

Production enhancements (auth, Ingress, MQTT, CI, overlays) are in  
[`docs/how-to-run-k8s.md`](docs/how-to-run-k8s.md) §13 — **additive only**; the Activate → Kafka → KEDA demo path is unchanged.
