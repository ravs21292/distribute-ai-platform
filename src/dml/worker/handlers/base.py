from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession


@dataclass(slots=True)
class TaskContext:
    task_id: UUID
    job_id: UUID
    attempt_id: UUID
    attempt_number: int
    task_type: str
    payload: dict[str, Any]
    worker_id: str
    session: AsyncSession


@dataclass(frozen=True, slots=True)
class HandlerResult:
    data: dict[str, Any]


class TaskHandler(Protocol):
    task_type: str

    async def execute(self, ctx: TaskContext) -> HandlerResult: ...
