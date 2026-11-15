# Distributed AI Execution Platform

A **distributed task execution platform** for AI workloads (dataset ingest → ETL → embeddings → search, plus optional TinyGPT train/eval).

Local Compose and production use the **same real pipeline path**. Upload datasets, workers shard and process them, embeddings land in pgvector.

## What runs locally

```
POST /v1/datasets + upload files
  → immutable dataset version
  → ingest → etl → (optional pipeline) dedupe → embed → index
  → train → evaluate → promote (quality gate + registry alias)
```

```
PostgreSQL tasks  →  Scheduler XADD  →  Redis Streams
  → workers claim/execute  →  persist result  →  XACK
```
Killing a worker does not lose tasks. Serving always resolves **aliases** (`candidate` / `staging` / `production`), never a raw checkpoint path.

## Quick start (Docker)

Pipeline workers use a slim image. HuggingFace MiniLM (torch) is only in `embedding-worker` via `infra/docker/Dockerfile.embed`.

```bash
docker compose up --build
```

If Docker Desktop ran out of disk after a previous torch unpack, free space on `C:` first (Docker Desktop → Troubleshoot → Clean / delete unused images), then rebuild. Start the control plane without embeddings if needed:

```powershell
docker compose up -d --build postgres redis minio migrate api scheduler cpu-worker-1 cpu-worker-2 cpu-worker-3 cpu-ml-01
docker compose up -d --build embedding-worker
```

Services: PostgreSQL+pgvector, Redis, MinIO, API `:8000`, scheduler, `cpu-worker-1..3`, `embedding-worker`, `cpu-ml-01`, Prometheus `:9090`, Grafana `:3000` (admin/admin).

Open the **React operations console** at [http://localhost:5173/](http://localhost:5173/) (WebSocket live fleet, DAG handoffs, embedding progress, ETA). Legacy HTML remains at [http://localhost:8000/](http://localhost:8000/).

### Wikipedia corpus (ETL + HuggingFace embeddings in pgvector)

Embeddings use **`sentence-transformers/all-MiniLM-L6-v2`** (384-d cosine vectors in pgvector). The `embedding-worker` runs those tasks; CPU workers run ingest/ETL/dedupe/index.

Download ~1 GiB of English Wikipedia from Hugging Face, already split into ~64 MB jsonl shards:

```powershell
python -m pip install datasets
python scripts/prepare_corpus.py --out samples/wiki --bytes 1073741824
```

A 1 GB embed on CPU takes a long time. For a first end-to-end pass use 64 MB:

```powershell
python scripts/prepare_corpus.py --out samples/wiki --bytes 67108864
```

Then upload, wait for ingest/ETL, and start the DAG (`embedding-worker` will go **busy**):

```powershell
python scripts/upload_and_run.py --dir samples/wiki --api http://localhost:8000
```

Watch http://localhost:8000/ — `cpu-worker-*` go busy during ingest/ETL, then `embedding-worker` goes busy and pgvector fills. The search box encodes with the same MiniLM model (needs `sentence-transformers` on the API; rebuild API from `Dockerfile.embed` if search returns 503).

Add `--include-training` only if you also want TinyGPT after dedupe.

### 4 GB Wikipedia already on disk

If you prepared `samples/wiki4gb/` (65× ~64 MB jsonl shards):

```powershell
docker compose up -d
# recreate so migration 0005 + workers pick up governance/PII code
docker compose up -d --build migrate api scheduler cpu-worker-1 cpu-worker-2 cpu-worker-3 embedding-worker
python scripts/upload_and_run.py --dir samples/wiki4gb --api http://localhost:8000
```

CPU MiniLM for 4 GB takes many hours. Watch the dashboard: busy workers, then pgvector count rising.

### RAG, embedding security, RBAC, PII / governance

| Feature | Status |
|---|---|
| RAG retrieval | **Yes** — `POST /v1/search` returns `{ governance, query_pii_*, hits }` (query → MiniLM → pgvector). Not a full chat RAG app. |
| RAG Phase-8 gate | **Yes** — auth/`search:read` → classification → ready version → query PII → retrieval → hit PII sanitize → audit (`dml.security.rag`) |
| Ready + classification gate | **Yes** — search requires version `ready`; `public/internal`→reader, `confidential`→operator, `restricted`→admin |
| Same content skip | **Yes** — embeddings keyed by SHA-256 `content_hash` + model; re-run skips encode |
| Content change → new vector | **Yes** — new hash → new embedding only; unchanged hashes skip; old hash kept |
| Identical file re-upload | **Yes** — source fingerprint reuse; returns existing ready version |
| PII | **Yes** — ETL before hash/embed **and** RAG search-time on query + hit defense-in-depth; `redact`/`reject`/`allow` + `DML_PII_REDACTION_ENABLED` |
| RBAC | **Yes** — roles `admin` / `operator` / `reader` on jobs/tasks/models/workers/ops/datasets/pipelines/search/audit |
| Governance | **Yes** — labels on dataset; returned on every RAG response; `PATCH /v1/datasets/{id}/governance` (admin) |
| Audit | **Yes** — `audit_events` + `GET /v1/audit/events` (admin); RAG writes `search.query` without raw query text |
| Ops truthfulness | **Yes** — never claims “searchable” when latest version failed or embeddings incomplete |

Enable production auth:

```powershell
# in docker-compose dml-env (or .env):
DML_AUTH_REQUIRED=true
DML_API_KEYS={"dml-admin-key":{"subject":"alice","role":"admin"},"dml-reader-key":{"subject":"bob","role":"reader"}}
```

Then every `/v1/*` call needs `X-API-Key: dml-admin-key` (or reader for search-only).

### Dataset ingest + ETL (primary path)

```bash
curl -s -X POST http://localhost:8000/v1/datasets \
  -H "content-type: application/json" \
  -d "{\"name\":\"tiny-wiki\",\"description\":\"local corpus\"}"

# use the returned dataset id
curl -s -X POST "http://localhost:8000/v1/datasets/<DATASET_ID>/versions" \
  -F "files=@samples/tiny-corpus.txt" \
  -F "lines_per_shard=20" \
  -F "chunk_chars=200" \
  -F "chunk_overlap=20"
```

Wait until the version status is `ready` (`GET /v1/datasets/<id>/versions`).

### Phases 3–7 — pipeline, search, train, promote

```bash
curl -s -X POST http://localhost:8000/v1/pipelines/run \
  -H "content-type: application/json" \
  -d "{\"dataset_id\":\"<DATASET_ID>\",\"include_training\":true}"

curl -s http://localhost:8000/v1/pipelines
curl -s http://localhost:8000/v1/workers
curl -s http://localhost:8000/v1/models

curl -s -X POST http://localhost:8000/v1/search \
  -H "content-type: application/json" \
  -d "{\"query\":\"worker checkpoint\",\"dataset_version\":\"<VERSION_ID>\",\"limit\":5}"

# after promote, move candidate → production, then generate
curl -s -X POST http://localhost:8000/v1/models/tinygpt/promote \
  -H "content-type: application/json" \
  -d "{\"alias\":\"production\",\"from_alias\":\"candidate\"}"

curl -s -X POST http://localhost:8000/v1/models/tinygpt/generate \
  -H "content-type: application/json" \
  -d "{\"alias\":\"production\",\"prompt\":\"The \",\"max_tokens\":40}"
```

Worker-death demo (any phase):

```bash
docker compose kill cpu-worker-2
# remaining workers reclaim pending messages; jobs and pipelines still complete
```

Optional API key (Phase 8 local slice): set `DML_API_KEY` on the API service and send `X-API-Key`. Health, metrics, and the dashboard stay open.

GPU overlay (optional, NVIDIA runtime required):

```bash
docker compose -f docker-compose.yml -f docker-compose.gpu.yml up --build
```

Kubernetes and AWS (phases 9–10) are **not** required for this local run.

## Local tests

```bash
python -m pip install -e ".[dev]"
pytest tests/unit -q
```

Integration tests need Postgres + Redis (Compose is enough), then apply migrations:

```bash
docker compose up -d postgres redis
alembic upgrade head
pytest tests/integration -q
```

## Architecture (short)

| Piece | Role |
| --- | --- |
| PostgreSQL | Authoritative jobs, tasks, attempts, workers, datasets, pipelines, embeddings, registry |
| Redis Streams | At-least-once delivery, consumer groups, XAUTOCLAIM |
| Scheduler | Publish pending work, expire leases, advance ingest→ETL and the DAG |
| WorkerRuntime | Register, heartbeat, claim, fence, handler, ACK-after-commit |
| MinIO | Raw sources, processed shards, training checkpoints |
| pgvector | Cosine search over chunk embeddings |

Docs: `docs/architecture.md`, `docs/task-semantics.md`, `docs/failure-model.md`, `docs/worker-model.md`, `docs/data-model.md`, `docs/roadmap.md`, `docs/adr/`.

## API

| Method | Path |
| --- | --- |
| GET | `/`, `/ui` (dashboard) |
| GET | `/health/live`, `/health/ready`, `/metrics` |
| POST/GET | `/v1/jobs`, `/v1/jobs/{id}`, `/v1/jobs/{id}/tasks`, `/v1/jobs/{id}/cancel` |
| GET/POST | `/v1/tasks/{id}`, `/v1/tasks/{id}/attempts`, `/v1/tasks/{id}/retry` |
| GET | `/v1/workers`, `/v1/workers/{id}` |
| POST/GET | `/v1/datasets`, `/v1/datasets/{id}/versions` |
| POST/GET | `/v1/pipelines/run`, `/v1/pipelines`, `/v1/pipelines/{id}`, `/v1/pipelines/{id}/cancel` |
| POST | `/v1/search` |
| GET/POST | `/v1/models`, `/v1/models/{name}`, `/v1/models/{name}/promote`, `/rollback`, `/generate` |
