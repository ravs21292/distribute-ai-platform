# Production on AWS — EV fleet telematics (this project)

How to take `kafka-k8s-platform` from a laptop (Kind + Compose infra) to **AWS**, including **EKS vs ECS vs EC2**, public access, and the **GitHub → Docker image → Kubernetes** path when you ship a new feature.

Local runbook: [`how-to-run-k8s.md`](how-to-run-k8s.md). Azure twin: [`production-azure.md`](production-azure.md).

---

## 1. Map local components → AWS services

| Local (Kind / Compose) | AWS production target | Notes |
|------------------------|----------------------|--------|
| Kind cluster + Deployments | **Amazon EKS** | Best match for existing `deploy/k8s/*` + KEDA |
| Compose Kafka | **Amazon MSK** (or self-managed Kafka on EKS) | MSK = managed brokers; keep topic names |
| Compose Postgres + pgvector | **Amazon RDS PostgreSQL** (+ pgvector extension) or **Aurora PostgreSQL** | Point `DAP_POSTGRES_*` at RDS |
| Compose MinIO | **Amazon S3** | Object storage for blobs / future artifacts |
| `dap-api`, workers, `iot-edge` images | **Amazon ECR** | Push `dap/api`, `dap/ingest`, … |
| KEDA ScaledObjects | **KEDA on EKS** | Same lag-based scale as Kind |
| Prometheus / Grafana | **Amazon Managed Prometheus / Grafana**, or in-cluster Prom/Grafana | Keep scrape of `:9100` / `/metrics` |
| React console | **S3 + CloudFront**, or in-cluster `dap-console` + Ingress | Static SPA calling public API URL |
| Secrets / ConfigMaps | **AWS Secrets Manager** + External Secrets, or K8s Secrets | Never commit prod keys |
| TLS + public DNS | **ACM** + Route 53 + ALB Ingress | `api.yourdomain.com` |

---

## 2. Choose compute: EKS vs ECS vs EC2

### Recommended: **EKS** (Elastic Kubernetes Service)

Use when you want the same manifests as this repo (`deploy/k8s/apps`, KEDA, Ingress).

```
Users → Route 53 → ALB (Ingress) → dap-api / dap-console Services
                                      │
                                      ▼
                         EKS node groups / Fargate
                         + KEDA scaling workers from MSK lag
                                      │
                    ┌─────────────────┼─────────────────┐
                    ▼                 ▼                 ▼
                   MSK              RDS              S3
```

**Why EKS for this project:** Activate → Kafka → KEDA lag scaling is already proven on Kind; production should reuse that control plane model.

### Alternative: **ECS** (Elastic Container Service)

Use when you want managed containers **without** operating Kubernetes.

| Piece | ECS pattern |
|-------|-------------|
| API | ECS Service + ALB target group |
| Workers | Separate ECS Services; scale with **Application Auto Scaling** on custom metrics (e.g. Kafka lag via CloudWatch) — **not** the KEDA YAML in this repo |
| Images | Same ECR images |
| Kafka / DB | Still MSK + RDS |

ECS fits a simpler ops team; you **re-implement** autoscaling (CloudWatch + scaling policies) instead of applying `deploy/k8s/keda/scaledobjects.yaml`.

### Alternative: **EC2** (virtual machines)

Use for a small demo or single-tenant box—not ideal for multi-service lag scaling.

Typical pattern:

1. One (or a few) EC2 instances in a public/private VPC  
2. Install Docker (or Docker Compose) on the VM  
3. Run `docker compose up` (or systemd + containers) for API + workers + optional local Kafka  
4. Put an **Application Load Balancer** (or nginx on the instance) in front of port 8080 / console  
5. Security group: allow 443 from internet → ALB; ALB → EC2 private ports  

**How EC2 serves public users**

```
Browser (HTTPS)
    │
    ▼
Elastic Load Balancer (public subnet) + ACM certificate
    │
    ▼
EC2 instance (private or public subnet)
    │  host Docker / nginx
    ▼
dap-api :8080  (+ optional static console)
```

EC2 **does not** magically publish Kind ClusterIP Services. You either:

- bind containers to the instance and put a **load balancer** in front, or  
- run **k3s/kubeadm on EC2** and expose via NodePort/LB (you are then running your own Kubernetes, closer to EKS).

For portfolio production stories, prefer **EKS + ALB Ingress** over raw EC2 Compose.

---

## 3. End-to-end workflow: new feature → live on AWS (EKS)

```
Developer laptop
    │  git commit / push
    ▼
GitHub (main or release branch)
    │  GitHub Actions (extend dap-ci.yml)
    ├─► unit tests / smoke
    ├─► docker buildx build
    └─► docker push → Amazon ECR
            │
            ▼
     Update image tag in manifests / Helm / Kustomize
            │  kubectl / Argo CD / Flux
            ▼
     EKS rolls Deployments (api, ingest, …)
            │
            ▼
     KEDA continues to scale workers from MSK consumer lag
            │
            ▼
     Users hit https://api.example.com via ALB Ingress
```

### Step A — Develop and push

```powershell
cd kafka-k8s-platform
# ... change code, verify locally with Kind (how-to-run-k8s.md) ...
git checkout -b feature/my-change
git add .
git commit -m "Describe why the change exists"
git push -u origin HEAD
# Open PR → merge to main after CI green
```

Existing CI: `.github/workflows/dap-ci.yml` (unit + Compose smoke). **Extend** it for production with build/push jobs (example below).

### Step B — Build and push images to ECR

One-time AWS setup:

```bash
# AWS CLI configured (aws configure / SSO)
aws ecr create-repository --repository-name dap/api --region us-east-1
# repeat for: dap/iot-edge dap/ingest dap/etl dap/embed dap/index dap/console
```

Build/push (CI or laptop):

```bash
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
REGION=us-east-1
REGISTRY=$ACCOUNT.dkr.ecr.$REGION.amazonaws.com
aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $REGISTRY

TAG=git-$(git rev-parse --short HEAD)
for svc in api iot-edge ingest etl embed index; do
  docker build -f Dockerfile --build-arg SERVICE=$svc -t $REGISTRY/dap/$svc:$TAG .
  docker push $REGISTRY/dap/$svc:$TAG
done
```

(Adjust `Dockerfile` / build args to match this repo’s actual build commands in `scripts/kind-deploy.ps1`.)

### Step C — Point cluster config at AWS backends

Before first prod deploy, replace Kind/Compose endpoints:

| Env / ConfigMap | Local | AWS |
|-----------------|-------|-----|
| `DAP_KAFKA_BOOTSTRAP` | `host.docker.internal:19092` | MSK bootstrap brokers (TLS/SASL — see `kafka-sasl-prod.md`) |
| Postgres URL | Compose `:5433` | RDS endpoint |
| Object store | MinIO | S3 bucket + IAM role (IRSA) |
| Images | `dap/*:local` + `imagePullPolicy: Never` | `$REGISTRY/dap/*:$TAG` + pull from ECR |

Apply overlays: `kubectl apply -k deploy/k8s/overlays/prod` (auth on, secrets wired).

### Step D — Install EKS, KEDA, Ingress, apps

```bash
# Example: cluster already exists and kubeconfig is set
kubectl apply --server-side -f https://github.com/kedacore/keda/releases/download/v2.16.1/keda-2.16.1.yaml

# AWS Load Balancer Controller + Ingress (ALB)
# then:
kubectl apply -f deploy/k8s/base/
kubectl apply -f deploy/k8s/apps/
kubectl apply -f deploy/k8s/keda/
kubectl apply -f deploy/k8s/observability/prometheus-grafana.yaml   # or AMP/AMG
```

Public users reach the API through **Ingress → ALB**, not `kubectl port-forward`.

### Step E — Ship each new feature (steady state)

1. Merge PR to `main`  
2. CI builds images, tags with git SHA, pushes ECR  
3. CD updates Deployment image tags (Kustomize image transformer, Helm, or `kubectl set image`)  
4. `kubectl rollout status deploy/dap-api -n dap`  
5. Smoke: `GET https://api.example.com/health` and Activate path still works  
6. Confirm KEDA HPAs still move under load  

---

## 4. Example GitHub Actions fragment (build → ECR)

Add a job (after unit tests) using OIDC to AWS (preferred) or access keys in GitHub Secrets:

```yaml
# Sketch — wire real Dockerfile targets and IAM role ARN
deploy-images:
  needs: unit
  runs-on: ubuntu-latest
  permissions:
    id-token: write
    contents: read
  steps:
    - uses: actions/checkout@v4
    - uses: aws-actions/configure-aws-credentials@v4
      with:
        role-to-assume: arn:aws:iam::ACCOUNT:role/dap-github-ecr
        aws-region: us-east-1
    - uses: aws-actions/amazon-ecr-login@v2
    - name: Build and push
      working-directory: kafka-k8s-platform
      run: |
        # docker build + push each dap/* service with $GITHUB_SHA
```

Optional follow-on job: `kubectl` / Argo CD sync against EKS.

---

## 5. Public access checklist (EKS)

| Item | Action |
|------|--------|
| DNS | Route 53 A/ALIAS → ALB |
| TLS | ACM certificate on ALB |
| Ingress | Hosts `api.example.com`, `console.example.com` (see `deploy/k8s/apps/ingress.yaml` pattern) |
| Auth | Prod overlay: `DAP_AUTH_REQUIRED=true`, API keys / JWT |
| Workers | No public Service — only Kafka consumers |
| Observability | Grafana behind VPN / SSO, not open internet |

---

## 6. Security and ops notes

- Prefer **IRSA** (IAM Roles for Service Accounts) over long-lived AWS keys in pods.  
- MSK: enable SASL/SCRAM or IAM auth + TLS (`docs/kafka-sasl-prod.md`).  
- RDS: private subnets; Security Group only from EKS nodes.  
- DLQ replay and admin APIs: protect with auth; do not expose anonymously.  
- Cost: EKS control plane + nodes + MSK + RDS — right-size; use KEDA `minReplicaCount: 0` where safe to save worker cost.

---

## 7. Quick decision guide

| Goal | Use |
|------|-----|
| Same K8s + KEDA demo as Kind | **EKS** |
| Managed containers, less K8s | **ECS** + custom lag scaling |
| Cheapest single-box demo | **EC2** + Docker Compose + ALB |
| Static UI to the world | **S3 + CloudFront** → API on ALB |

---

## Related docs

- [`how-to-run-k8s.md`](how-to-run-k8s.md) — local Kind + Prometheus/Grafana  
- [`kafka-sasl-prod.md`](kafka-sasl-prod.md) — Kafka auth placeholders  
- [`keda-regression-checklist.md`](keda-regression-checklist.md) — Activate → scale checks  
- [`production-azure.md`](production-azure.md) — Azure equivalent  
