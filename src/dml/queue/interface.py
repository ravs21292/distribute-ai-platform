"""TaskQueue protocol. Redis delivers; PostgreSQL remains authoritative."""

from __future__ import annotations

from typing import Protocol

from dml.contracts.messages import ClaimedTask, TaskMessage


class TaskQueue(Protocol):
    async def publish(self, message: TaskMessage) -> tuple[str, str]:
        """XADD. Returns (stream_name, message_id)."""

    async def claim(
        self, worker_id: str, streams: list[str], *, count: int = 1, block_ms: int = 1000
    ) -> list[ClaimedTask]:
        """XREADGROUP for never-delivered messages (`>`)."""

    async def ack(self, claimed: ClaimedTask) -> None:
        """XACK after durable PostgreSQL commit."""

    async def reclaim_stale(
        self, worker_id: str, streams: list[str], *, min_idle_ms: int, count: int = 10
    ) -> list[ClaimedTask]:
        """XAUTOCLAIM pending messages abandoned by a dead consumer."""

    async def depth(self, stream: str) -> int:
        """XLEN — approximate backlog including pending."""

    async def pending_count(self, stream: str) -> int:
        """XPENDING group size."""

    async def ping(self) -> None: ...

    async def close(self) -> None: ...
