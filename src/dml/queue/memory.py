"""In-memory TaskQueue for unit tests. Time is injected so reclaim needs no sleep."""

from __future__ import annotations

import asyncio
import itertools
from dataclasses import dataclass, field

from dml.contracts.matching import stream_name
from dml.contracts.messages import ClaimedTask, TaskMessage


@dataclass
class _Entry:
    message_id: str
    message: TaskMessage
    pending_consumer: str | None = None
    last_delivered_at: float = 0.0
    acked: bool = False


@dataclass
class InMemoryTaskQueue:
    prefix: str = "dml.tasks"
    time_fn: callable = field(default=lambda: 0.0)  # type: ignore[type-arg]
    _streams: dict[str, list[_Entry]] = field(default_factory=dict)
    _seq: itertools.count = field(default_factory=lambda: itertools.count(1))
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def now(self) -> float:
        return float(self.time_fn())

    async def publish(self, message: TaskMessage) -> tuple[str, str]:
        stream = stream_name(message.task_type, message.resource_class, prefix=self.prefix)
        async with self._lock:
            message_id = f"{int(self.now() * 1000)}-{next(self._seq)}"
            self._streams.setdefault(stream, []).append(
                _Entry(message_id=message_id, message=message)
            )
            return stream, message_id

    async def claim(
        self, worker_id: str, streams: list[str], *, count: int = 1, block_ms: int = 1000
    ) -> list[ClaimedTask]:
        del block_ms
        async with self._lock:
            claimed: list[ClaimedTask] = []
            for stream in streams:
                for entry in self._streams.get(stream, []):
                    if entry.acked or entry.pending_consumer is not None:
                        continue
                    entry.pending_consumer = worker_id
                    entry.last_delivered_at = self.now()
                    claimed.append(
                        ClaimedTask(stream=stream, message_id=entry.message_id, message=entry.message)
                    )
                    if len(claimed) >= count:
                        return claimed
            return claimed

    async def ack(self, claimed: ClaimedTask) -> None:
        async with self._lock:
            for entry in self._streams.get(claimed.stream, []):
                if entry.message_id == claimed.message_id:
                    entry.acked = True
                    entry.pending_consumer = None
                    return

    async def reclaim_stale(
        self, worker_id: str, streams: list[str], *, min_idle_ms: int, count: int = 10
    ) -> list[ClaimedTask]:
        min_idle = min_idle_ms / 1000.0
        now = self.now()
        async with self._lock:
            claimed: list[ClaimedTask] = []
            for stream in streams:
                for entry in self._streams.get(stream, []):
                    if entry.acked or entry.pending_consumer is None:
                        continue
                    if entry.pending_consumer == worker_id:
                        continue
                    if now - entry.last_delivered_at < min_idle:
                        continue
                    entry.pending_consumer = worker_id
                    entry.last_delivered_at = now
                    claimed.append(
                        ClaimedTask(stream=stream, message_id=entry.message_id, message=entry.message)
                    )
                    if len(claimed) >= count:
                        return claimed
            return claimed

    async def depth(self, stream: str) -> int:
        return sum(1 for e in self._streams.get(stream, []) if not e.acked)

    async def pending_count(self, stream: str) -> int:
        return sum(
            1
            for e in self._streams.get(stream, [])
            if not e.acked and e.pending_consumer is not None
        )

    async def ping(self) -> None:
        return None

    async def close(self) -> None:
        return None
