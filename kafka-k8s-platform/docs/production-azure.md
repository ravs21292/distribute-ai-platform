# Production on Azure — EV fleet telematics (this project)

How to take `kafka-k8s-platform` from a laptop (Kind + Compose infra) to **Azure**, including **AKS vs Container Apps vs VM**, public access, and the **GitHub → container images → Kubernetes** path when you ship a new feature.

Local runbook: [`how-to-run-k8s.md`](how-to-run-k8s.md). AWS twin: [`production-aws.md`](production-aws.md).

---

## 1. Map local components → Azure services

| Local (Kind / Compose) | Azure production target | Notes |
|------------------------|-------------------------|--------|
| Kind cluster + Deployments | **Azure Kubernetes Service (AKS)** | Best match for `deploy/k8s/*` + KEDA |
| Compose Kafka | **Azure Event Hubs for Kafka** or **self-managed Kafka on AKS** / Confluent Cloud | Event Hubs Kafka API can replace bootstrap with protocol limits — validate consumer groups |
| Compose Postgres + pgvector | **Azure Database for PostgreSQL** Flexible Server (+ pgvector) | Point `DAP_POSTGRES_*` at Flexible Server |
| Compose MinIO | **Azure Blob Storage** | Object storage |
| App images | **Azure Container Registry (ACR)** | Push `dap/api`, `dap/ingest`, … |
| KEDA ScaledObjects | **KEDA on AKS** (often available as cluster add-on / Helm) | Same lag-based worker scale |
| Prometheus / Grafana | **Azure Monitor managed Prometheus + Managed Grafana**, or in-cluster | Keep scrapes of `:9100` / `/metrics` |
| React console | **Static Web Apps / Blob + CDN**, or in-cluster `dap-console` | SPA → public API URL |
| Secrets | **Azure Key Vault** + CSI / External Secrets | Never commit prod keys |
| TLS + public DNS | **App Gateway / AGIC** or **Front Door** + Key Vault certs | `api.yourdomain.com` |

---

## 2. Choose compute: AKS vs Container Apps vs VM

### Recommended: **AKS** (Azure Kubernetes Service)

Reuse the same manifests as Kind (`deploy/k8s/apps`, KEDA, Ingress).

```
Users → DNS → Application Gateway / Load Balancer Ingress
                    │
                    ▼
              AKS node pools
              + KEDA scales workers from Kafka lag
                    │
      ┌─────────────┼─────────────┐
      ▼             ▼             ▼
   Kafka/EH    Azure PostgreSQL   Blob
```

**Why AKS for this project:** Activate → Kafka → KEDA is already proven locally; AKS keeps that story intact.

### Alternative: **Azure Container Apps**

Managed containers without full cluster ops (similar role to AWS ECS).

| Piece | Pattern |
|-------|---------|
| API | Container App + HTTP ingress |
| Workers | Separate Container Apps; scale on KEDA-style or custom metrics if wired |
| Images | Same ACR images |
| Data plane | Still need Kafka-compatible bus + PostgreSQL |

You will **not** apply Kind-style Deployments unchanged; rewrite scale rules in Container Apps terms.

### Alternative: **Azure VM** (like EC2)

Small demos:

1. Linux VM in a VNet  
2. Docker / Docker Compose for API + workers  
3. **Azure Load Balancer** or nginx + TLS in front of `:8080`  
4. NSG: allow 443 from internet → LB only  

**How a VM serves public users**

```
Browser (HTTPS)
    │
    ▼
Azure Load Balancer / Application Gateway (public)
    │
    ▼
VM (Docker Compose or k3s)
    │
    ▼
dap-api :8080
```

Same caveat as EC2: Compose on a VM does **not** give you Kind/KEDA lag autoscaling unless you install Kubernetes on the VM or move to AKS.

---

## 3. End-to-end workflow: new feature → live on Azure (AKS)

```
Developer laptop
    │  git push
    ▼
GitHub
    │  GitHub Actions (extend dap-ci.yml)
    ├─► tests
    ├─► docker build
    └─► docker push → Azure Container Registry
            │
            ▼
     Update image tags (Kustomize / Helm / Flux / Arc)
            │
            ▼
     AKS rolls Deployments
            │
            ▼
     KEDA scales workers from Kafka consumer lag
            │
            ▼
     Users → https://api.example.com (App Gateway / Ingress)
```

### Step A — Develop and push

```powershell
cd kafka-k8s-platform
# verify on Kind first — docs/how-to-run-k8s.md
git checkout -b feature/my-change
git commit -am "Describe why"
git push -u origin HEAD
# PR → CI → merge
```

### Step B — Build and push images to ACR

```bash
az login
az acr create -g dap-rg -n dapregistry --sku Basic   # once
az acr login -n dapregistry

TAG=$(git rev-parse --short HEAD)
REGISTRY=dapregistry.azurecr.io

# Match real Dockerfiles / build args used in scripts/kind-deploy.ps1
for svc in api iot-edge ingest etl embed index; do
  docker build -t $REGISTRY/dap/$svc:$TAG .
  docker push $REGISTRY/dap/$svc:$TAG
done
```

Attach AKS to ACR: `az aks update -g dap-rg -n dap-aks --attach-acr dapregistry`.

### Step C — Point config at Azure backends

| Setting | Local | Azure |
|---------|-------|-------|
| Kafka bootstrap | `host.docker.internal:19092` | Event Hubs Kafka endpoint or AKS Kafka |
| Postgres | Compose | Flexible Server FQDN |
| Images | `dap/*:local` | `dapregistry.azurecr.io/dap/*:$TAG` |
| Auth | off for demo | `overlays/prod` + Key Vault secrets |

```bash
kubectl apply -k deploy/k8s/overlays/prod
```

### Step D — AKS + KEDA + Ingress + apps

```bash
az aks get-credentials -g dap-rg -n dap-aks

# KEDA (Helm or AKS application)
kubectl apply --server-side -f https://github.com/kedacore/keda/releases/download/v2.16.1/keda-2.16.1.yaml

kubectl apply -f deploy/k8s/base/
kubectl apply -f deploy/k8s/apps/
kubectl apply -f deploy/k8s/keda/
kubectl apply -f deploy/k8s/observability/prometheus-grafana.yaml  # or Azure Monitor
```

Expose with **Ingress + Application Gateway Ingress Controller (AGIC)** or nginx Ingress + public LoadBalancer — not port-forward.

### Step E — Steady-state release

1. Merge to `main`  
2. CI builds/pushes ACR with git SHA  
3. CD updates Deployment images on AKS  
4. Rollout status + `/health` smoke  
5. Regression: Activate vehicles → KEDA HPA moves ([`keda-regression-checklist.md`](keda-regression-checklist.md))

---

## 4. Example GitHub Actions fragment (build → ACR)

```yaml
# Sketch — use azure/login with OIDC federated credential
publish-acr:
  needs: unit
  runs-on: ubuntu-latest
  permissions:
    id-token: write
    contents: read
  steps:
    - uses: actions/checkout@v4
    - uses: azure/login@v2
      with:
        client-id: ${{ secrets.AZURE_CLIENT_ID }}
        tenant-id: ${{ secrets.AZURE_TENANT_ID }}
        subscription-id: ${{ secrets.AZURE_SUBSCRIPTION_ID }}
    - uses: azure/docker-login@v2
      with:
        login-server: dapregistry.azurecr.io
        username: ${{ secrets.ACR_USERNAME }}
        password: ${{ secrets.ACR_PASSWORD }}
    - name: Build and push
      working-directory: kafka-k8s-platform
      run: |
        # docker build + push dap/*:$GITHUB_SHA
```

Prefer **OIDC** federated identity over long-lived ACR admin passwords when possible.

---

## 5. Public access checklist (AKS)

| Item | Action |
|------|--------|
| DNS | Azure DNS / external DNS → public IP / App Gateway |
| TLS | Key Vault certificate or App Gateway listener cert |
| Ingress | `api.example.com` / `console.example.com` |
| Auth | Prod overlay JWT / API keys |
| Workers | ClusterIP only |
| Grafana | Private access / Azure AD SSO — not open anonymous admin |

---

## 6. Security and ops notes

- Use **Managed Identity** for AKS → ACR, Key Vault, Blob (avoid secrets in YAML).  
- PostgreSQL Flexible Server: private access + VNet integration.  
- If using Event Hubs Kafka protocol, validate partition counts vs `maxReplicaCount` (same partition ceiling as local Kafka).  
- Lock down `/v1/admin/dlq/replay` behind auth.  
- Cost: AKS + Kafka/EH + Flexible Server — keep KEDA min replicas at 0 where safe.

---

## 7. Quick decision guide

| Goal | Use |
|------|-----|
| Same K8s + KEDA as Kind | **AKS** |
| Managed containers, less cluster ops | **Container Apps** |
| Single-box demo | **VM** + Docker Compose + Load Balancer |
| Global static UI | **Static Web Apps / Blob CDN** → API Ingress |

---

## AWS ↔ Azure cheat sheet (this project)

| Concern | AWS | Azure |
|---------|-----|-------|
| Kubernetes | EKS | AKS |
| Registry | ECR | ACR |
| Managed containers (non-K8s) | ECS | Container Apps |
| VMs | EC2 | Azure VM |
| Postgres | RDS / Aurora | Flexible Server |
| Object store | S3 | Blob Storage |
| Public HTTPS | ALB + ACM | App Gateway / Front Door |
| Secrets | Secrets Manager | Key Vault |

Workflow is the same on both clouds: **GitHub → build images → push registry → roll cluster → Ingress serves users**.

---

## Related docs

- [`how-to-run-k8s.md`](how-to-run-k8s.md) — local Kind + Prometheus/Grafana  
- [`production-aws.md`](production-aws.md) — AWS EKS/ECS/EC2  
- [`kafka-sasl-prod.md`](kafka-sasl-prod.md) — Kafka auth  
- [`keda-regression-checklist.md`](keda-regression-checklist.md)  
