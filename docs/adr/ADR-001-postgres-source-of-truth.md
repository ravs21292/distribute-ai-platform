# ADR-001: PostgreSQL is the source of truth

## Status

Accepted (Phase 0)

## Decision

Job, task, attempt, and worker state live in PostgreSQL. Redis, Prometheus, and process memory are not authoritative.

## Why

- Tasks must survive worker and scheduler restarts.
- We need transactional fencing (`SELECT FOR UPDATE`, conditional `UPDATE`).
- Operators need SQL to inspect dead letters and attempts.
- Later phases add relational entities (datasets, models) that belong next to tasks.

## Consequences

- Every claim/complete path hits PostgreSQL.
- Redis outage pauses delivery; it must not corrupt task status.
- A reconciler (scheduler) must re-publish `pending` rows if Redis lost messages.
