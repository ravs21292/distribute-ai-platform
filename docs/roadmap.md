# Roadmap vs original spec

| Phase | Name | Status |
| --- | --- | --- |
| 0 | Foundation (Python, Compose, Postgres, Redis, MinIO, logging) | **Done** |
| 1 | Distributed task runtime (jobs/tasks/attempts, Redis Streams, 3 CPU workers, retry, DLQ) | **Done** |
| 2 | Datasets + streaming ingest + ETL + shards + hashes | **Done** |
| 3 | Embeddings + pgvector search + skip-if-hash-exists | **Done** |
| 4 | DAG scheduler (INGEST→ETL→DEDUPE→EMBED→INDEX, TRAIN→EVAL→PROMOTE) | **Done** |
| 5 | TinyGPT training + durable checkpoints + resume | **Done** |
| 6 | Evaluation + quality gate + model registry aliases | **Done** |
| 7 | Observability (Grafana dashboards + local UI) | **Done** (local). OpenTelemetry traces not required for laptop demo |
| 8 | Auth / hardening (local slice) | **Done (local)**: `DML_API_KEYS` RBAC (`admin`/`operator`/`reader`); RAG path owns Phase-8 via `dml.security.rag` (auth → classification → ready → query/hit PII → audit); ETL PII + governance PATCH; audit events. **Out of local scope**: tenants, rate limits, cluster backpressure |
| 9 | Kubernetes | Not started (not required to run on a local machine) |
| 10 | AWS / Terraform | Not started (not required to run on a local machine) |

Phase 0–2 behavior is unchanged: `POST /v1/jobs` dummy tasks and dataset ingest/ETL still work.
The later phases add handlers, tables, APIs, and workers on top of that runtime.
