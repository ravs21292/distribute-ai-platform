# ADR-002: Redis Streams for delivery

## Status

Accepted (Phase 0)

## Decision

Use Redis Streams with consumer groups (`XADD`, `XREADGROUP`, `XACK`, `XPENDING`, `XAUTOCLAIM`).

Do not use `RPOP` + `SETEX` (or LIST pop + SETNX lease). That pattern races on claim.

## Why

- Consumer groups track pending messages per consumer.
- `XAUTOCLAIM` is the native “this worker disappeared” primitive.
- At-least-once is explicit: un-ACKed messages remain pending.

## Stream layout

```
dml.tasks.{task_type}.{resource_class}
```

`resource_class` is `cpu` or `gpu`. Consumer group name: `dml-workers`. Consumer name: `worker_id`.

## Consequences

- Workers must create the group (`MKSTREAM`) on startup.
- Payload in Redis is a pointer (`task_id`), not the full job document.
- Duplicate XADD is possible and allowed.
