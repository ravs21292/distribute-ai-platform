"""Deterministic hash/sleep workload used to prove the runtime before AI jobs."""

from __future__ import annotations

import asyncio
import hashlib

from dml.contracts.errors import PermanentTaskError
from dml.db.repositories import TaskRepository
from dml.worker.handlers.base import HandlerResult, TaskContext


class DummyHandler:
    task_type = "dummy"

    async def execute(self, ctx: TaskContext) -> HandlerResult:
        seed = ctx.payload.get("seed")
        if not seed or not isinstance(seed, str):
            raise PermanentTaskError("dummy task payload.seed is required", error_type="invalid_payload")
        sleep_ms = int(ctx.payload.get("sleep_ms") or 0)
        if sleep_ms < 0:
            raise PermanentTaskError("sleep_ms must be >= 0", error_type="invalid_payload")
        if sleep_ms:
            await asyncio.sleep(sleep_ms / 1000.0)
        digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()
        await TaskRepository(ctx.session).upsert_dummy_result(
            task_id=ctx.task_id, seed=seed, digest=digest
        )
        return HandlerResult(data={"digest": digest, "seed": seed})
