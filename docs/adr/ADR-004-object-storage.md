# ADR-004: Object storage for large artifacts

## Status

Accepted (Phase 0); **not used by the Phase 1 dummy workload**

## Decision

MinIO locally, S3 later. Large blobs (datasets, shards, checkpoints, weights) never live in PostgreSQL.

## Why

Checkpoints and corpora will exceed reasonable BYTEA/JSONB sizes. Durable training recovery requires an object key that any worker can read.

## Consequences

- Compose includes MinIO from Phase 0 so later phases do not change topology.
- Phase 1 dummy results stay in PostgreSQL (tiny, need transactional fencing with task status).
- Production credentials come from environment / secret manager, never from git.
