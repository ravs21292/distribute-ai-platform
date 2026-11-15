# How to run (local = same path as production)

This brings up **all workers**, then you upload real data. Workers ingest shards → ETL → dedupe → embed → **pgvector**. Unchanged text is skipped by `content_hash`; new/changed text gets new embeddings.

## 1. Prerequisites

- Docker Desktop running (enough free disk on `C:` — embedding image needs torch)
- PowerShell in the project root:

```powershell
cd D:\Ravinder_Data\Projects\distributed-ai-platform
```

## 2. Start the full stack

```powershell
docker compose up -d --build
```

This starts:

| Service | Role |
|---------|------|
| postgres | Source of truth + **pgvector** |
| redis | Task delivery (streams) |
| minio | Raw sources + shard objects |
| migrate | DB migrations |
| api | HTTP + WebSocket `:8000` |
| scheduler | Publish / reclaim / advance DAG |
| cpu-worker-1..3 | **ingest, etl, dedupe, index** |
| embedding-worker (+ _2) | **embedding** → pgvector |
| cpu-ml-01 | training / evaluation (optional) |
| frontend | Ops console `:5173` |
| prometheus / grafana | Metrics (`:9090` / `:3000` admin/admin) |

Check health:

```powershell
docker compose ps
Invoke-RestMethod http://localhost:8000/health/live
```

Open: [http://localhost:5173/](http://localhost:5173/)

## 3. Run a real end-to-end pipeline (small corpus first)

Use a small file to verify **all stages** before multi‑GB:

```powershell
# Create dataset
$ds = Invoke-RestMethod -Method POST http://localhost:8000/v1/datasets `
  -ContentType "application/json" `
  -Body '{"name":"portfolio-demo","description":"real ingest","classification":"internal","pii_policy":"redact"}'
$ds.id

# Upload + start ingest/ETL (creates version, shards, chunks)
$ver = curl.exe -s -X POST "http://localhost:8000/v1/datasets/$($ds.id)/versions" `
  -F "files=@samples/tiny-corpus.txt" `
  -F "lines_per_shard=20" `
  -F "chunk_chars=200" `
  -F "chunk_overlap=20" | ConvertFrom-Json
$ver.id
$ver.status

# Wait until status is ready
do {
  Start-Sleep -Seconds 2
  $v = Invoke-RestMethod "http://localhost:8000/v1/datasets/$($ds.id)/versions/$($ver.id)"
  Write-Host "status=$($v.status) chunks=$($v.chunk_count)"
} while ($v.status -notin @("ready","failed"))

# Full DAG: dedupe → embed → index (no TinyGPT training)
$pipe = Invoke-RestMethod -Method POST http://localhost:8000/v1/pipelines/run `
  -ContentType "application/json" `
  -Body (@{ dataset_version_id = $ver.id; include_training = $false } | ConvertTo-Json)
$pipe.id
```

Watch workers go **busy** on http://localhost:5173/ until embed finishes.

Search (RAG + Phase-8 auth / PII / governance on this path):

```powershell
# When DML_AUTH_REQUIRED=true, add: -Headers @{ "X-API-Key" = "dml-admin-key" }
$rag = Invoke-RestMethod -Method POST http://localhost:8000/v1/search `
  -ContentType "application/json" `
  -Body (@{ query = "worker checkpoint"; dataset_version = $ver.id; limit = 5 } | ConvertTo-Json)
$rag.governance   # classification + pii_policy + version ids
$rag.hits         # ranked chunks (PII re-checked on redact policy)
```

RAG gate order: `search:read` → classification role check → version `ready` → query PII
(`redact`/`reject`/`allow`) → MiniLM → pgvector → hit PII sanitize → `search.query` audit.

## 4. Large corpus (all shards → all embeddings)

If you already have `samples/wiki4gb/` (~4 GB jsonl shards):

```powershell
python scripts/upload_and_run.py --dir samples/wiki4gb --api http://localhost:8000
```

Or recover a previously ingested version, then run the pipeline:

```powershell
# POST /v1/datasets/{id}/versions/{version_id}/recover   # if shards left unfinished
# POST /v1/pipelines/run  with that dataset_version_id
```

**Expect hours on CPU** for millions of chunks. Index only succeeds when **every** non-duplicate chunk has an embedding (`coverage == 100%`).

## 5. Does this process everything?

| Question | Answer |
|----------|--------|
| All workers start? | Yes, with `docker compose up -d` |
| All shard files ingested? | Yes — one ingest task per source; line-split into shards; all `cpu-worker-*` claim in parallel |
| All shards ETL’d? | Yes — one ETL task per shard (PII → chunks in Postgres) |
| All chunks embedded? | Yes when the embed DAG completes — one embed task per shard; skip known `content_hash`; index **blocks** until full coverage |
| Stored in pgvector? | Yes — `chunk_embeddings` with model/version metadata |

Identical re-upload of the same files reuses the ready version (fingerprint). Unchanged chunk text does not re-encode.

## 6. Stop / restart

```powershell
docker compose stop
docker compose up -d
# after capability/code changes:
docker compose up -d --force-recreate cpu-worker-1 cpu-worker-2 cpu-worker-3 embedding-worker embedding-worker-2 api scheduler
```

## 7. Optional production auth

```powershell
# In docker-compose dml-env (or .env):
# DML_AUTH_REQUIRED=true
# DML_API_KEYS={"dml-admin-key":{"subject":"alice","role":"admin"},...}
```

Then send `X-API-Key` on API calls (and on the console reconnection field).
