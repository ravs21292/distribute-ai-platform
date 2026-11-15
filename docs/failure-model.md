# Failure model — Phase 1

## Guarantees

1. PostgreSQL is authoritative for job/task/attempt/worker state.
2. A task is not ACKed until its outcome is committed (or it is a no-op skip of an already-terminal task).
3. Worker death does not delete a task.
4. Duplicate execution of the dummy handler cannot yield two different digests for one `task_id`.
5. Retryable failures stop at `max_attempts` and become `dead_letter`.
6. Permanent failures become `dead_letter` without burning the remaining retry budget as “maybe it works next time.”

## Non-guarantees

1. Exactly-once execution.
2. No lost **in-flight handler side effects** that the handler itself failed to make idempotent (Phase 1 dummy is idempotent).
3. Progress if PostgreSQL is down for longer than the operator is willing to wait.
4. Progress of GPU-required tasks without a GPU worker (they remain pending).
5. Scheduler uniqueness — two scheduler replicas can double-publish (SKIP LOCKED reduces but does not eliminate duplicate XADD). That is still at-least-once.
6. Clock sync — leases use database `now()` for fence checks; worker-side sleep is not the source of truth.

## Scenario matrix

| # | Failure | What we do | Residual risk |
| --- | --- | --- | --- |
| 1 | Worker crashes before processing (after claim) | Message stays pending; `XAUTOCLAIM` after `min_idle_ms`; new attempt | Duplicate if original process was actually alive (split brain) — lease fence should reject the late completer |
| 2 | Worker crashes during processing | Same as (1); partial dummy upsert is idempotent | Handler with non-idempotent external calls could double-apply (none in Phase 1) |
| 3 | PostgreSQL commit of success, crash before ACK | Reclaim → begin_attempt sees `succeeded` → Skip+ACK | Extra attempt row is not created |
| 4 | Redis temporarily down | Workers stop claiming; scheduler publish fails and leaves `pending`; PG state intact | Backlog grows; no corruption |
| 5 | PostgreSQL temporarily down | Worker classifies as retryable if the error looks transient; does not ACK until it can record failure. If it cannot record either, message stays pending | Operator sees retries / pending |
| 6 | Redis accepts XADD, scheduler crashes before `queued` | Task still `pending` plus a stream message. Worker may claim a pending row (allowed). Scheduler may XADD a second copy | Duplicate delivery; fencing + idempotency |
| 7 | Task output stored, worker dies before status=succeeded | If both writes are one transaction, they commit together. If we split them, status could lag — **we write result + status in one commit** | Dummy upsert and task status are the same session/commit |
| 8 | Same dummy seed processed twice (two tasks) | Two task_ids → two result rows. Same seed → same digest values, independent rows | Jobs with duplicate seeds are valid; dedupe is a later pipeline stage |
| 9 | Same task_id executed twice | `ON CONFLICT (task_id) DO UPDATE` digest unchanged, `executions += 1` | Audit counter grows |
| 10 | Training worker disappears | **Not in Phase 1.** Will resume from MinIO checkpoints in Phase 5 | — |
| 11 | Replacement worker is CPU, task requires GPU | CPU worker does not subscribe to `*.gpu` streams; task stays in GPU stream | Pending until a GPU worker exists |
| 12 | No compatible worker | Task remains `pending`/`queued`. Metrics: queue depth, oldest pending age | No silent drop |
| 13 | Task continuously fails retryably | Attempts increment; backoff 1..16s; then `dead_letter` | Needs manual retry |
| 14 | Embedding API 429 | **Not in Phase 1.** Handler will raise RetryableTaskError later | — |
| 15 | Graceful shutdown mid-task | Wait grace period; else abandon without ACK | Task pauses up to lease + min_idle |
| 16 | Invalid payload | PermanentTaskError → dead_letter | Operator inspects attempts |
| 17 | Poison task (handler always throws retryable) | DLQ after max_attempts | Manual skip/retry |
| 18 | Worker unhealthy but still running (GC pause) | Scheduler may expire lease; second worker starts; late commit loses fence | Duplicate work; dummy still correct |
| 19 | Duplicate job POST | `jobs.idempotency_key` unique; returns original job | Client must send the key |

## Questions we ask after every distributed change

1. Crash after this line — is the task still recoverable?  
2. Can the task disappear from both PG and Redis with no reconciler path?  
3. If it runs twice, is the business output identical?  
4. PG commit + failed ACK?  
5. XADD + scheduler crash?  
6. Result written + status not updated? (must be one transaction)  
7. No compatible worker — do we block or drop? (block)  
8. How do we see it in logs/metrics? (`task_id`, attempt metrics, queue depth)

## Known Phase 1 weaknesses

- **No Redis stream trim policy.** Streams grow until an operator trims. Fine for local demos.
- **Single-task concurrency per worker.** Simplifies fencing; limits throughput.
- **Lease + XAUTOCLAIM both exist.** They can both fire, causing extra attempts. Accepted for at-least-once.
- **Scheduler poll interval** (default 500ms) adds latency vs push-on-commit.
- **Unhealthy worker that is not actually dead** can have work stolen.
- **No backpressure** from queue depth onto job submission.
- **MinIO is not on the dummy path**, so object-store failure is untested here.
- **No OpenTelemetry traces yet.**
