# ADR-005: PostgreSQL + pgvector for embeddings

## Status

Accepted (Phase 0); **extension enabled, unused until Phase 3**

## Decision

Use the `pgvector` image and `CREATE EXTENSION vector` in migrations so chunk embeddings and task metadata share one database.

## Why

Operational simplicity for the local/demo system. One backup, one transaction boundary for “chunk exists + embedding exists.”

## Consequences

- We are not introducing a separate vector DB in Phase 1–3.
- ANN index tuning is a later concern; Phase 3 can start with exact / small IVFFlat.
- If vector workload fights OLTP later, we can split — not now.
