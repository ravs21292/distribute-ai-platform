# Data model — Phase 1

PostgreSQL is the authoritative store. Redis may cache delivery metadata (`stream_id`) on the task row; if Redis and PostgreSQL disagree, PostgreSQL wins.

## Entity relationship

```
jobs 1---N tasks 1---N task_attempts
                N---1 workers     (leased_by / attempts.worker_id, nullable)
tasks 1---0..1 dummy_results
```

No pipeline/dataset/model tables in this phase.

## jobs

| Column | Type | Notes |
| --- | --- | --- |
| id | UUID PK | |
| idempotency_key | TEXT UNIQUE NULL | Client-supplied; repeat POST returns the same job |
| job_type | TEXT | Phase 1: `dummy` |
| name | TEXT NULL | |
| status | TEXT | See task-semantics.md |
| payload | JSONB | Template applied to each task |
| priority | INT | Higher is scheduled first |
| task_count | INT | Denormalized create-time count |
| created_at, started_at, finished_at | TIMESTAMPTZ | |

## tasks

| Column | Type | Notes |
| --- | --- | --- |
| id | UUID PK | |
| job_id | UUID FK | ON DELETE RESTRICT |
| task_type | TEXT | Handler key, e.g. `dummy` |
| payload | JSONB | |
| status | TEXT | State machine in task-semantics.md |
| priority | INT | |
| max_attempts | INT | Default 5 |
| current_attempt | INT | 0 until first claim |
| required_cpu | INT | |
| required_memory_mb | INT | |
| required_gpu | INT | `> 0` means GPU required |
| required_gpu_memory_mb | INT | |
| gpu_preferred | BOOL | Prefer GPU when required_gpu = 0 |
| next_run_at | TIMESTAMPTZ NULL | Backoff gate |
| lease_until | TIMESTAMPTZ NULL | Fencing / steal gate |
| leased_by_worker_id | TEXT NULL | FK-like to workers.worker_id |
| stream_name | TEXT NULL | Redis stream last published to |
| stream_id | TEXT NULL | Redis message id |
| result | JSONB NULL | Handler output |
| last_error | TEXT NULL | |
| last_error_type | TEXT NULL | `retryable` / `permanent` / `worker_lost` |
| first_failed_at, last_failed_at | TIMESTAMPTZ NULL | |
| created_at, scheduled_at, started_at, finished_at | TIMESTAMPTZ | |

Indexes: `(status, next_run_at)`, `(job_id)`, `(leased_by_worker_id)`, `(status, lease_until)`.

## task_attempts

Every claim that begins execution inserts a row. Retries are new attempts, never in-place mutation of a previous attempt.

| Column | Type | Notes |
| --- | --- | --- |
| id | UUID PK | |
| task_id | UUID FK | |
| attempt_number | INT | UNIQUE with task_id |
| worker_id | TEXT NULL | |
| started_at, finished_at | TIMESTAMPTZ | |
| status | TEXT | `running`, `succeeded`, `failed`, `lost` |
| error_type | TEXT NULL | |
| error_message | TEXT NULL | |
| retryable | BOOL NULL | |
| duration_ms | INT NULL | |

## workers

| Column | Type | Notes |
| --- | --- | --- |
| worker_id | TEXT PK | Stable process identity, e.g. `cpu-worker-1` |
| hostname | TEXT | |
| process_id | INT | |
| capabilities | TEXT[] | |
| status | TEXT | Worker state machine |
| cpu_count | INT | |
| memory_mb | INT | |
| gpu_count | INT | |
| gpu_model | TEXT NULL | |
| gpu_memory_mb | INT | |
| device | TEXT | `cpu` / `cuda` / `auto` |
| runtime | TEXT | e.g. `dml-worker/0.1.0` |
| started_at | TIMESTAMPTZ | |
| last_heartbeat | TIMESTAMPTZ | |
| created_at, updated_at | TIMESTAMPTZ | |

High-frequency CPU/GPU utilization is **not** stored here. Heartbeat is liveness only. Utilization belongs in Prometheus.

## dummy_results

Idempotent output for the Phase 1 hash workload.

| Column | Type | Notes |
| --- | --- | --- |
| task_id | UUID PK FK tasks | |
| seed | TEXT | |
| digest | TEXT | SHA-256 of seed; stable across retries |
| executions | INT | Incremented on duplicate execution (audit, not business state) |

`UNIQUE(task_id)` plus deterministic `digest` is the idempotency strategy.

## Redis (not a table)

Messages are JSON:

```json
{
  "task_id": "...",
  "job_id": "...",
  "task_type": "dummy",
  "resource_class": "cpu",
  "priority": 0
}
```

Payload is **not** required in Redis. Workers load the authoritative payload from PostgreSQL after claim so a poison/mutated stream body cannot diverge from the database.
