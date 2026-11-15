# ADR-003: At-least-once execution

## Status

Accepted (Phase 0)

## Decision

We will not pretend to offer exactly-once task execution.

Correctness = at-least-once delivery + idempotent handlers + database constraints + fencing + deterministic outputs.

## Why

Exactly-once across Redis and PostgreSQL requires a dual-commit protocol we will not build. Crash-between-persist-and-ACK is unavoidable.

## Consequences

- Handlers must be safe to re-run.
- Tests must include duplicate execution.
- Metrics count attempts, not “unique business events,” unless a table with a uniqueness constraint says otherwise.
