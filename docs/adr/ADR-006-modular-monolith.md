# ADR-006: Modular monolith for Phase 0/1

## Status

Accepted

## Decision

Ship one installable Python package `dml` with module boundaries (`api`, `scheduler`, `worker`, `db`, `queue`, `contracts`) instead of separately versioned microservices.

Compose still runs API, scheduler, and workers as **separate processes**.

## Why

- Shared migrations and contracts change together in Phase 1.
- Process isolation is what the distributed design needs, not separate wheels.
- The requested `apps/` `workers/` `libs/` layout is preserved via modules + thin `__main__` entrypoints.

## Consequences

- One Docker image, different `CMD`.
- We can split packages when (if) teams or scale demand it.
