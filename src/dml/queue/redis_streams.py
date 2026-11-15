"""Redis Streams implementation of TaskQueue."""

from __future__ import annotations

import redis.asyncio as redis
from redis.exceptions import ResponseError

from dml.config import Settings
from dml.contracts.matching import stream_name
from dml.contracts.messages import ClaimedTask, TaskMessage


class RedisTaskQueue:
    def __init__(self, client: redis.Redis, settings: Settings) -> None:
        self._redis = client
        self._settings = settings
        self._group = settings.consumer_group
        self._prefix = settings.stream_prefix

    @classmethod
    def from_settings(cls, settings: Settings) -> RedisTaskQueue:
        client = redis.from_url(settings.redis_url, decode_responses=True)
        return cls(client, settings)

    def stream_for(self, task_type: str, resource_class: str) -> str:
        return stream_name(task_type, resource_class, prefix=self._prefix)

    async def ensure_group(self, stream: str) -> None:
        try:
            await self._redis.xgroup_create(stream, self._group, id="0-0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def publish(self, message: TaskMessage) -> tuple[str, str]:
        stream = self.stream_for(message.task_type, message.resource_class)
        await self.ensure_group(stream)
        message_id = await self._redis.xadd(stream, message.to_fields())  # type: ignore[arg-type]
        return stream, str(message_id)

    async def claim(
        self, worker_id: str, streams: list[str], *, count: int = 1, block_ms: int = 1000
    ) -> list[ClaimedTask]:
        if not streams:
            return []
        for stream in streams:
            await self.ensure_group(stream)
        mapping = {stream: ">" for stream in streams}
        result = await self._redis.xreadgroup(
            self._group,
            worker_id,
            streams=mapping,
            count=count,
            block=block_ms,
        )
        return _parse_xread(result)

    async def ack(self, claimed: ClaimedTask) -> None:
        await self._redis.xack(claimed.stream, self._group, claimed.message_id)

    async def reclaim_stale(
        self, worker_id: str, streams: list[str], *, min_idle_ms: int, count: int = 10
    ) -> list[ClaimedTask]:
        claimed: list[ClaimedTask] = []
        for stream in streams:
            await self.ensure_group(stream)
            try:
                response = await self._redis.xautoclaim(
                    stream,
                    self._group,
                    worker_id,
                    min_idle_time=min_idle_ms,
                    start_id="0-0",
                    count=count,
                )
            except ResponseError:
                continue
            messages = _messages_from_autoclaim(response)
            for message_id, fields in messages:
                claimed.append(
                    ClaimedTask(
                        stream=stream,
                        message_id=str(message_id),
                        message=TaskMessage.from_fields(_as_str_dict(fields)),
                    )
                )
        return claimed

    async def depth(self, stream: str) -> int:
        return int(await self._redis.xlen(stream))

    async def pending_count(self, stream: str) -> int:
        try:
            info = await self._redis.xpending(stream, self._group)
        except ResponseError:
            return 0
        if not info:
            return 0
        if isinstance(info, dict):
            return int(info.get("pending", 0) or 0)
        return int(info[0] or 0)

    async def ping(self) -> None:
        await self._redis.ping()

    async def close(self) -> None:
        await self._redis.aclose()


def _as_str_dict(fields: dict) -> dict[str, str]:
    return {str(k): str(v) for k, v in fields.items()}


def _parse_xread(result: object) -> list[ClaimedTask]:
    claimed: list[ClaimedTask] = []
    if not result:
        return claimed
    for stream, messages in result:  # type: ignore[misc]
        for message_id, fields in messages:
            claimed.append(
                ClaimedTask(
                    stream=str(stream),
                    message_id=str(message_id),
                    message=TaskMessage.from_fields(_as_str_dict(fields)),
                )
            )
    return claimed


def _messages_from_autoclaim(response: object) -> list[tuple[str, dict]]:
    """redis-py returns (next_id, messages) or (next_id, messages, deleted)."""
    if not response:
        return []
    if isinstance(response, (list, tuple)):
        if len(response) >= 2:
            messages = response[1]
            if messages:
                return [(str(mid), fields) for mid, fields in messages]
    return []
