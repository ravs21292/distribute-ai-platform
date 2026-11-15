# Phase 1 test plan

## Unit (no Docker)

| Test | Proves |
| --- | --- |
| `test_states` | Legal / illegal task transitions |
| `test_matching` | CPU cannot run GPU-required; GPU-preferred can fall back to CPU |
| `test_backoff` | 1s, 2s, 4s, 8s, 16s cap |
| `test_queue_memory` | publish, claim, ACK, reclaim without `sleep` (injected clock) |
| `test_dummy_handler` | missing seed is permanent |

## Integration (Postgres + Redis)

| Test | Proves |
| --- | --- |
| `test_publish_claim_ack` | Redis Streams happy path |
| `test_autoclaim_after_abandoned_claim` | Worker death → XAUTOCLAIM |
| `test_vertical_slice_and_worker_death` | Job → 3 workers → kill one → all tasks succeed, digests stable |
| `test_duplicate_execution_keeps_digest` | Second upsert does not change SHA-256 |
| `test_retry_then_dead_letter` | 3 retryable failures → `dead_letter`, 3 attempts |
| `test_cancel_stops_new_work` | Cancelled job is not executed |

## Not yet automated (Phase 1 residual)

- PostgreSQL commit + Redis ACK failure at the socket level (covered by skip-if-succeeded path, not a injected ACK fault).
- Scheduler crash between XADD and `queued` (at-least-once allows duplicate messages).
- Compose `docker compose kill cpu-worker-2` demo (manual; see README).
