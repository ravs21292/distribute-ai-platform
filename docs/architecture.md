# Architecture — Phase 0 / Phase 1 / Phase 2

This document describes the **Distributed AI Execution Platform** through Phase 2 (task runtime + dataset ingest/ETL). Later phases add embeddings, a generic DAG, TinyGPT, and the model registry on top of this runtime. They do not replace it.

## What exists after Phase 2

A modular monolith that can:

1. Accept a job over HTTP.
2. Persist jobs/tasks in PostgreSQL (source of truth).
3. Have a scheduler publish tasks onto Redis Streams.
4. Have disposable CPU workers claim, execute, persist, and ACK work.
5. Recover work when a worker dies.
6. Retry retryable failures with backoff, then dead-letter.
7. Register immutable dataset versions, stream-split sources into shards, and ETL-chunk them with SHA-256 hashes.

TinyGPT, embeddings, generic DAG, and Kubernetes are **out of scope** until later phases.

## Control plane vs data plane

```
CLIENT
  |
  v
FastAPI control plane          PostgreSQL (authoritative state)
  |                              ^
  +--> Scheduler ----------------+
         |
         v
    Redis Streams (delivery only)
         |
         v
    WorkerRuntime  (data plane)
         |
         v
    Task handlers (dummy hash/sleep in Phase 1)
```

| Plane | Processes | Responsibilities |
| --- | --- | --- |
| Control | `api`, `scheduler` | HTTP, job/task CRUD, publish, retry/DLQ, worker health, cancellation |
| Data | `worker` processes | Register, heartbeat, claim, execute, persist results, ACK |

Workers never coordinate through shared process memory. The only shared state is PostgreSQL, Redis Streams, and (later) object storage.

## Modular monolith

Phase 1 is **one Python package** (`dml`) with hard module boundaries, not a fleet of microservices.

| Module | Role | Maps to requested layout |
| --- | --- | --- |
| `dml.api` | Control-plane HTTP | `apps/api` |
| `dml.scheduler` | Control-plane loop | `apps/scheduler` |
| `dml.worker` | Data-plane runtime | `workers/runtime` |
| `dml.worker.handlers` | Pluggable handlers | `workers/handlers` |
| `dml.contracts` | States, errors, matching | `libs/contracts` |
| `dml.db` | SQLAlchemy models/repos | `libs/db` |
| `dml.queue` | TaskQueue + Redis Streams | `libs/queue` |
| `dml.storage` | Object storage client (unused by dummy workload) | `libs/storage` |
| `dml.telemetry` | Logs + Prometheus | `libs/telemetry` |
| `dml.config` | Environment settings | `libs/config` |

Why one package: independent services would duplicate migrations, settings, and contracts before we have independent scale or release needs. Split later if deploy cadences diverge.

Thin entrypoints live under `apps/` and `workers/` so the repo shape matches the long-term layout.

## Shared state

| System | Authority | Used for |
| --- | --- | --- |
| PostgreSQL | **Yes** | Jobs, tasks, attempts, workers, dummy results |
| Redis Streams | No | Delivery, consumer groups, pending reclaim |
| MinIO | No (not in the dummy path) | Provisioned now; used from Phase 2 |
| Worker RAM / disk | Never | May cache; never the only copy of progress |

## Task lifecycle (happy path)

```
POST /v1/jobs
  -> INSERT job + N tasks (status=pending)     # PostgreSQL commit
Scheduler
  -> XADD dml.tasks.{type}.{cpu|gpu}           # Redis
  -> UPDATE task status=queued, store stream_id
Worker
  -> XREADGROUP (claim)
  -> SELECT FOR UPDATE + insert TaskAttempt     # fence + lease
  -> handler.execute() + persist result
  -> UPDATE task status=succeeded               # PostgreSQL commit
  -> XACK                                       # only after durable success/failure
```

## CPU / GPU model (Phase 1 implements the rules, dummy work is CPU)

Tasks declare requirements (`required_cpu`, `required_memory_mb`, `required_gpu`, `required_gpu_memory_mb`, `gpu_preferred`).

Workers advertise capabilities and hardware.

Eligibility:

```
task_type in worker.capabilities
AND worker.cpu_count      >= required_cpu
AND worker.memory_mb      >= required_memory_mb
AND worker.gpu_count      >= required_gpu
AND (required_gpu == 0 OR worker.gpu_memory_mb >= required_gpu_memory_mb)
```

Routing:

- `required_gpu > 0` → stream class `gpu` (CPU workers never subscribe)
- `required_gpu == 0` and `gpu_preferred` and a healthy GPU worker exists → prefer `gpu` stream, else `cpu`
- otherwise → `cpu` stream (CPU-only work). GPU workers may subscribe to `cpu` streams as overflow but read `gpu` streams first.

Phase 1 ships only the `dummy` capability on CPU workers. The matching functions are unit-tested so later GPU training/embedding can reuse them.

## Scheduler responsibilities (Phase 1)

The scheduler **does not execute work**. It:

1. Publishes `pending` tasks whose `next_run_at` is due.
2. Re-publishes `queued` tasks that have no `stream_id` (crash between intent and XADD recovery).
3. Marks workers `unhealthy` when heartbeats are stale.
4. Releases leases on running tasks whose worker is unhealthy / lease expired, counting the attempt as retryable `worker_lost`.
5. Rolls job status up from task statuses.
6. Respects `cancelled` jobs (does not publish their pending tasks).

Workers, not the scheduler, perform `XAUTOCLAIM` for pending Redis messages.

## Worker runtime

`WorkerRuntime` is reusable. Handlers are a dict:

```python
handlers = {
    "dummy": DummyHandler(),
}
```

A worker process:

1. Detects resources.
2. Registers in PostgreSQL.
3. Heartbeats.
4. Subscribes to Redis streams for each advertised capability × eligible resource class.
5. Claims at most one task at a time (Phase 1).
6. Executes the matching handler.
7. ACKs only after the task row and side effects are committed.
8. On SIGTERM: stop claiming, finish or abandon the in-flight task, mark `stopped`.

## Failure recovery (summary)

See `docs/failure-model.md`. The two recovery paths are:

1. **Redis pending + XAUTOCLAIM** — worker died after claim, message still in the consumer group.
2. **Scheduler lease reclaim** — worker unhealthy or Redis lost the message; task returns to `pending` and is XADD'd again.

Both are at-least-once. Correctness is fencing + idempotent handlers + unique result keys.

## Observability (Phase 1 slice)

- Structured JSON logs with `request_id`, `job_id`, `task_id`, `attempt_id`, `worker_id` when known.
- Prometheus counters/histograms on API, scheduler, and workers (`/metrics` or `:9100/metrics`).
- Grafana/Prometheus are in Compose for scrape targets; full dashboards are Phase 7.

## What this phase deliberately does not do

- Dataset versioning, ingest, ETL, embeddings, pgvector search.
- DAG pipeline engine.
- TinyGPT, checkpoints, evaluation, model registry.
- Auth, tenants, rate limits, backpressure.
- Kubernetes / AWS.
