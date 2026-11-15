# Task semantics

**Tasks use at-least-once execution.**

This platform does not provide exactly-once execution. A task may run more than once. Final business output must still be correct.

## Why at-least-once

Redis Streams + consumer groups deliver a message to a consumer. The worker must persist results to PostgreSQL, then XACK. Between those steps a crash is always possible:

- Crash before persist → another worker runs the task (necessary).
- Crash after persist, before ACK → another worker runs the task again (duplicate).

Eliminating the second case would require a distributed transaction across Redis and PostgreSQL, which we do not have. We choose duplicate execution + idempotency.

## Job / Task / TaskAttempt

| Concept | Meaning |
| --- | --- |
| Job | Unit of user submission. Owns many tasks. |
| Task | Unit of schedulable work. Has resource requirements and a handler type. |
| TaskAttempt | One execution try on one worker. Retries create new attempts. |

## Task state machine

```
pending ──► queued ──► running ──► succeeded
                ▲          │
                │          ├──► failed ──► queued/pending (retry, backoff)
                │          │         └──► dead_letter (max attempts or permanent)
                │          └──► cancelled
pending/queued ──────────────────► cancelled

dead_letter ──► pending   (manual retry API only)
```

| Status | Terminal? | Meaning |
| --- | --- | --- |
| pending | no | In PostgreSQL, not yet (or no longer) in Redis |
| queued | no | Published to Redis; not leased |
| running | no | A worker holds a lease |
| failed | no | Last attempt failed retryably; waiting for `next_run_at` |
| succeeded | yes | Handler result committed |
| cancelled | yes | Job or task cancelled; no further publishes |
| dead_letter | yes* | Exhausted attempts or permanent error. Manual retry can reopen |

\* `dead_letter` is terminal for the scheduler. `POST /v1/tasks/{id}/retry` may move it back to `pending`.

Allowed transitions are enforced in `dml.contracts.states`. Invalid transitions raise and are not written.

## Attempt fencing

When a worker begins work:

1. `SELECT task FOR UPDATE`.
2. If status is terminal (`succeeded`, `cancelled`, `dead_letter`) → skip and ACK (duplicate delivery).
3. If status is `running` and `lease_until` is in the future and `leased_by_worker_id` is another worker → skip and ACK (duplicate message; owner still holds the lease).
4. If status is `failed` and `next_run_at` is in the future → skip and ACK (backoff; scheduler will republish).
5. Otherwise insert `task_attempts` with `attempt_number = current_attempt + 1`. If that exceeds `max_attempts` → `dead_letter`.
6. Set `status=running`, `leased_by`, `lease_until=now()+lease_ttl`.

Completion:

```
UPDATE tasks SET status='succeeded', ...
WHERE id=:id AND current_attempt=:my_attempt AND status='running'
```

If this updates 0 rows, this attempt lost the fence. The handler's writes must still have been idempotent.

## Idempotency

Every handler must define one. Phase 1 dummy handler:

- Input: `payload.seed`
- Output row: `dummy_results.task_id` primary key
- Digest: `SHA256(seed)` — overwriting it with the same value is a no-op
- `executions` increments so duplicates are visible without changing the digest

Later handlers (documented now, not implemented):

| Handler | Strategy |
| --- | --- |
| ETL | Deterministic object key `processed/{dataset_version}/{shard_id}.parquet` |
| Embedding | `UNIQUE(content_hash, embedding_model, embedding_model_version)` |
| Model artifact | Immutable versioned path; never overwrite a production version |

## ACK rules

ACK only after PostgreSQL commit of the attempt outcome (`succeeded`, `failed` waiting backoff, `dead_letter`, or skip-because-already-terminal).

Do **not** leave a failed attempt un-ACKed to implement backoff. Redis would redeliver immediately and bypass `next_run_at`. Failed retryable tasks return to PostgreSQL; the scheduler re-XADDs after backoff.

## Retry policy

Errors are classified at the worker boundary:

| Class | Examples | Action |
| --- | --- | --- |
| RetryableTaskError | timeout, PG/Redis transient, 429, 5xx, object-store blip | attempt failed, `next_run_at = now + exp backoff`, ACK |
| PermanentTaskError | malformed payload, missing required field | `dead_letter` immediately |
| Unclassified Exception | bugs, unexpected faults | treated as retryable (do not ACK-and-drop) |

Backoff (attempt 1 after first failure is the second try):

```
1s, 2s, 4s, 8s, 16s  (capped)
```

Jitter is applied (±20%) so recovering workers do not thundering-herd.

There is no infinite retry. `max_attempts` is the hard stop.

## Dead letter

When `current_attempt >= max_attempts` after a retryable failure, or on permanent failure:

- `task.status = dead_letter`
- last error, first/last failure timestamps, and all attempts remain queryable
- APIs: inspect (`GET /v1/tasks/{id}/attempts`), `POST /v1/tasks/{id}/retry`, cancel via job cancel

## Job state rollup

| Job status | Rule |
| --- | --- |
| pending | No task has started |
| running | Any task is pending/queued/running/failed (in-flight retry) |
| completed | All tasks succeeded |
| failed | All tasks terminal and at least one `dead_letter` |
| cancelled | Job cancel requested; tasks moved to cancelled when not succeeded |

## Delivery guarantees / non-guarantees

Guaranteed:

- A non-cancelled task will be retried until success or dead-letter (assuming control plane and Redis/Postgres eventually return).
- Duplicate execution cannot create two dummy digest values for one task_id.

Not guaranteed:

- Exactly-once handler invocation.
- Bounded duplicate count (a publish/ACK incident can add extras).
- Order between tasks of the same job.
- That a GPU-required task ever runs if no GPU worker exists (it stays pending/queued).
