# Worker model

Workers are disposable processes. They hold no authoritative state.

## Identity

`worker_id` is a stable string for the process lifetime (`cpu-worker-1`, or `{hostname}-{pid}-{shortuuid}` if unset). Restarting a container with the same Compose service name reuses the id and upserts the workers row.

## Capabilities

A worker starts with an explicit capability list:

```
python -m dml.worker --capabilities ingest,etl --device cpu
```

The worker process loads only the handlers listed in `--capabilities`. Production Compose registers real pipeline handlers (`ingest`, `etl`, `dedupe`, `embedding`, `index`, `training`, `evaluation`, `artifact`). Example:

```
dml worker --capabilities ingest,etl,dedupe
dml worker --capabilities embedding,training,evaluation --device cuda
```

A worker **refuses** a task whose `task_type` is not in its capabilities (should not be delivered; still checked after loading the PG row).

## Resources

On boot the worker records:

- `cpu_count`, `memory_mb`
- `gpu_count`, `gpu_model`, `gpu_memory_mb` (0 / null on CPU-only machines)
- `device` (`cpu`, `cuda`, `auto`)

`auto` means: use CUDA if `nvidia-smi` reports a GPU, otherwise CPU. Phase 1 Compose workers force `cpu`.

Resource matching algorithm: `dml.contracts.matching`.

CPU-only work prefers CPU workers (sort key) so a GPU worker is not stolen by light CPU tasks. GPU-preferred work prefers workers with `gpu_count > 0` but remains eligible on CPU when `required_gpu = 0`.

## State machine

```
starting ──► idle ◄──► busy
               │
               ├──► draining ──► stopped     (SIGTERM)
               └──► unhealthy                (scheduler: stale heartbeat)
```

| Status | Who writes it | Meaning |
| --- | --- | --- |
| starting | worker | Registering |
| idle | worker | Heartbeating, no lease |
| busy | worker | Holds at least one task lease |
| draining | worker | SIGTERM received; no new claims |
| unhealthy | scheduler | `last_heartbeat` older than `worker_stale_after_seconds` |
| stopped | worker (best effort) | Process exiting |

A worker that becomes unhealthy is still allowed to resume: a later heartbeat from the same `worker_id` sets status back to `idle`/`busy`. Tasks it held are only stolen after `lease_until` expires.

## Heartbeats

Interval: `DML_HEARTBEAT_INTERVAL_SECONDS` (default 5).

Stale after: `DML_WORKER_STALE_AFTER_SECONDS` (default 15).

Heartbeat updates `workers.last_heartbeat` and, if busy, extends `tasks.lease_until`.

Heartbeat is **liveness**, not utilization. CPU %, RAM, GPU util, VRAM go to Prometheus gauges on the worker metrics port.

## Health

| Signal | Meaning |
| --- | --- |
| Process alive | OS process exists (Compose/K8s liveness later) |
| Ready to work | Worker row exists, Redis + Postgres reachable, not draining |
| Scheduler view | `status != unhealthy` and recent heartbeat |

A worker that stopped heartbeating must not look healthy in `GET /v1/workers`. The API derives `healthy` from `last_heartbeat` and status.

## Scheduling interaction

Workers do **not** pick globally optimal tasks. They:

1. Subscribe to streams they can serve.
2. Claim.
3. Fence in PostgreSQL.
4. Execute or skip.

The scheduler chooses the stream class (`cpu` vs `gpu`) at publish time. That is the Phase 1 “resource-aware scheduler”: deterministic and explainable, not a Kubernetes-grade bin packer.

## Graceful shutdown

On SIGTERM / SIGINT:

1. Set `draining`; stop `XREADGROUP` for new `>` messages.
2. If a task is in-flight: wait up to `DML_SHUTDOWN_GRACE_SECONDS` for the handler to finish.
3. If the grace period expires: do not ACK; do not forge success; exit. Redis pending + lease expiry recover it.
4. Flush metrics best-effort.
5. Set `stopped`.

## Compose topology

CPU profile (default) — **real pipeline workers only**:

| Service | Capabilities | Device |
| --- | --- | --- |
| cpu-worker-1 | ingest, etl, dedupe, index, artifact | cpu |
| cpu-worker-2 | ingest, etl, dedupe, index | cpu |
| cpu-worker-3 | ingest, etl | cpu |
| embedding-worker | embedding | cpu |
| embedding-worker-2 | embedding | cpu |
| cpu-ml-01 | training, evaluation | cpu |

Multiple CPU workers exist so ingest/ETL shards are claimed in parallel and a killed worker can be reclaimed by the others.

The mixed GPU profile is a Compose overlay that does not run unless NVIDIA runtime is present.
