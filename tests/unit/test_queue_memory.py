from uuid import uuid4

from dml.contracts.errors import PermanentTaskError, RetryableTaskError, classify_exception
from dml.contracts.messages import TaskMessage
from dml.queue.memory import InMemoryTaskQueue


def test_classify_explicit_errors() -> None:
    retryable, name = classify_exception(RetryableTaskError("timeout", error_type="Timeout"))
    assert retryable is True
    assert name == "Timeout"
    retryable, name = classify_exception(PermanentTaskError("bad json"))
    assert retryable is False


def test_unknown_exceptions_are_retryable() -> None:
    retryable, _ = classify_exception(RuntimeError("boom"))
    assert retryable is True


async def test_memory_queue_publish_claim_ack() -> None:
    queue = InMemoryTaskQueue(time_fn=lambda: 1.0)
    job_id = uuid4()
    task_id = uuid4()
    stream, message_id = await queue.publish(
        TaskMessage(task_id=task_id, job_id=job_id, task_type="dummy", resource_class="cpu")
    )
    assert stream.endswith(".dummy.cpu")
    claimed = await queue.claim("w1", [stream])
    assert len(claimed) == 1
    assert claimed[0].message_id == message_id
    assert await queue.pending_count(stream) == 1
    await queue.ack(claimed[0])
    assert await queue.pending_count(stream) == 0
    assert await queue.depth(stream) == 0


async def test_reclaim_after_worker_disappears_no_sleep() -> None:
    clock = {"t": 0.0}

    def now() -> float:
        return clock["t"]

    queue = InMemoryTaskQueue(time_fn=now)
    msg = TaskMessage(task_id=uuid4(), job_id=uuid4(), task_type="dummy", resource_class="cpu")
    stream, _ = await queue.publish(msg)
    claimed_a = await queue.claim("worker-a", [stream])
    assert claimed_a
    # worker-a dies without ACK. Time advances past min idle.
    clock["t"] = 20.0
    claimed_b = await queue.reclaim_stale("worker-b", [stream], min_idle_ms=15_000)
    assert len(claimed_b) == 1
    assert claimed_b[0].message.task_id == msg.task_id
    await queue.ack(claimed_b[0])
    assert await queue.pending_count(stream) == 0
