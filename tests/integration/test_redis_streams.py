from __future__ import annotations

from uuid import uuid4

import pytest

from dml.config import Settings
from dml.contracts.messages import TaskMessage
from dml.queue.redis_streams import RedisTaskQueue

pytestmark = pytest.mark.integration


@pytest.fixture
async def queue(settings: Settings):
    q = RedisTaskQueue.from_settings(settings)
    try:
        await q.ping()
    except Exception as exc:
        await q.close()
        pytest.skip(f"redis unavailable: {exc}")
    yield q
    await q.close()


async def test_publish_claim_ack(queue: RedisTaskQueue) -> None:
    message = TaskMessage(
        task_id=uuid4(),
        job_id=uuid4(),
        task_type="dummy",
        resource_class="cpu",
    )
    stream, message_id = await queue.publish(message)
    claimed = await queue.claim("itest-worker-a", [stream], count=1, block_ms=500)
    assert len(claimed) == 1
    assert claimed[0].message_id == message_id
    assert claimed[0].message.task_id == message.task_id
    assert await queue.pending_count(stream) == 1
    await queue.ack(claimed[0])
    assert await queue.pending_count(stream) == 0


async def test_autoclaim_after_abandoned_claim(queue: RedisTaskQueue, settings: Settings) -> None:
    message = TaskMessage(
        task_id=uuid4(),
        job_id=uuid4(),
        task_type="dummy",
        resource_class="cpu",
    )
    stream, _ = await queue.publish(message)
    claimed = await queue.claim("itest-dead", [stream], count=1, block_ms=500)
    assert claimed
    # No ACK. Another consumer waits until min idle, then XAUTOCLAIM.
    import asyncio

    await asyncio.sleep(settings.claim_min_idle_ms / 1000.0 + 0.2)
    reclaimed = await queue.reclaim_stale(
        "itest-alive",
        [stream],
        min_idle_ms=settings.claim_min_idle_ms,
        count=10,
    )
    assert any(item.message.task_id == message.task_id for item in reclaimed)
    for item in reclaimed:
        await queue.ack(item)
