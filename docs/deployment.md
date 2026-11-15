# Local Compose is the Phase 1 deployment.

```bash
docker compose up --build
```

GPU overlay: `docker compose -f docker-compose.yml -f docker-compose.gpu.yml up --build`

Kubernetes and AWS are Phase 9–10. Do not add manifests until the local runtime is proven (worker death + idempotent dummy jobs).
