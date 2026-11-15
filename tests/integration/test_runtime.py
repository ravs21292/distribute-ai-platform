from __future__ import annotations

import asyncio
import hashlib
from uuid import UUID

import pytest
from sqlalchemy import select

from dml.config import Settings
from dml.contracts.errors import RetryableTaskError
from dml.contracts.states import JobStatus, TaskStatus
from dml.db.models import Attempt, DummyResult, Job, Task
from dml.db.repositories import JobRepository, TaskRepository
from dml.queue.redis_streams import RedisTaskQueue
from dml.scheduler.service import Scheduler
from dml.telemetry.metrics import Metrics
from dml.worker.handlers.base import HandlerResult, TaskContext
from dml.worker.handlers.dummy import DummyHandler
from dml.worker.resources import DetectedResources
from dml.worker.runtime import WorkerRuntime

pytestmark = pytest.mark.integration


def _resources(worker_id: str) -> DetectedResources:
    return DetectedResources(
        hostname="test-host",
        process_id=1,
        cpu_count=4,
        memory_mb=8192,
        gpu_count=0,
        gpu_model=None,
        gpu_memory_mb=0,
        device="cpu",
    )


def _runtime(
    settings: Settings,
    session_factory,
    queue: RedisTaskQueue,
    worker_id: str,
    handlers=None,
) -> WorkerRuntime:
    return WorkerRuntime(
        settings=settings,
        session_factory=session_factory,
        queue=queue,
        handlers=handlers or {"dummy": DummyHandler()},
        resources=_resources(worker_id),
        metrics=Metrics(),
        worker_id=worker_id,
        capabilities=["dummy"],
    )


async def _wait_job(session_factory, job_id: UUID, timeout: float = 30.0) -> Job:
    deadline = asyncio.get_event_loop().time() + timeout
    last = None
    while asyncio.get_event_loop().time() < deadline:
        async with session_factory() as session:
            job = await session.get(Job, job_id)
            last = job
            if job and job.status in {
                JobStatus.COMPLETED.value,
                JobStatus.FAILED.value,
                JobStatus.CANCELLED.value,
            }:
                return job
        await asyncio.sleep(0.1)
    raise AssertionError(f"job {job_id} did not finish, last={last.status if last else None}")


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


async def test_vertical_slice_and_worker_death(settings: Settings, session_factory, queue) -> None:
    async with session_factory() as session:
        job = await JobRepository(session).create_dummy_job(
            task_count=12,
            payload={"seed_prefix": f"slice-{job_seed()}"},
            name="vertical-slice",
            idempotency_key=None,
            priority=0,
            max_attempts=5,
            required_cpu=1,
            required_memory_mb=64,
            required_gpu=0,
            required_gpu_memory_mb=0,
            gpu_preferred=False,
            sleep_ms=400,
        )
        job_id = job.id
        await session.commit()

    scheduler = Scheduler(
        settings=settings, session_factory=session_factory, queue=queue, metrics=Metrics()
    )
    workers = [
        _runtime(settings, session_factory, queue, f"slice-w{i}") for i in range(3)
    ]
    sched_task = asyncio.create_task(scheduler.run_forever(), name="sched")
    worker_tasks = [asyncio.create_task(w.run(), name=w.worker_id) for w in workers]
    try:
        await asyncio.sleep(0.8)
        workers[0].request_stop()
        job = await _wait_job(session_factory, job_id, timeout=40)
        assert job.status == JobStatus.COMPLETED.value

        async with session_factory() as session:
            tasks = (await session.execute(select(Task).where(Task.job_id == job_id))).scalars().all()
            assert len(tasks) == 12
            assert all(t.status == TaskStatus.SUCCEEDED.value for t in tasks)
            results = (
                await session.execute(select(DummyResult).where(DummyResult.task_id.in_([t.id for t in tasks])))
            ).scalars().all()
            assert len(results) == 12
            for row in results:
                assert row.digest == hashlib.sha256(row.seed.encode()).hexdigest()
            attempts = (
                await session.execute(select(Attempt).where(Attempt.task_id.in_([t.id for t in tasks])))
            ).scalars().all()
            assert len(attempts) >= 12
    finally:
        scheduler.request_stop()
        for w in workers:
            w.request_stop()
        await asyncio.gather(sched_task, *worker_tasks, return_exceptions=True)


def job_seed() -> str:
    from uuid import uuid4

    return uuid4().hex[:8]


async def test_duplicate_execution_keeps_digest(settings: Settings, session_factory, queue) -> None:
    async with session_factory() as session:
        job = await JobRepository(session).create_dummy_job(
            task_count=1,
            payload={"seed_prefix": f"dup-{job_seed()}"},
            name="dup",
            idempotency_key=None,
            priority=0,
            max_attempts=5,
            required_cpu=1,
            required_memory_mb=64,
            required_gpu=0,
            required_gpu_memory_mb=0,
            gpu_preferred=False,
            sleep_ms=0,
        )
        job_id = job.id
        await session.commit()

    scheduler = Scheduler(
        settings=settings, session_factory=session_factory, queue=queue, metrics=Metrics()
    )
    worker = _runtime(settings, session_factory, queue, "dup-w1")
    sched_task = asyncio.create_task(scheduler.run_forever())
    worker_task = asyncio.create_task(worker.run())
    try:
        job = await _wait_job(session_factory, job_id)
        assert job.status == JobStatus.COMPLETED.value
        async with session_factory() as session:
            task = (await session.execute(select(Task).where(Task.job_id == job_id))).scalar_one()
            first = await session.get(DummyResult, task.id)
            assert first is not None
            digest = first.digest
            await TaskRepository(session).upsert_dummy_result(
                task_id=task.id, seed=first.seed, digest=digest
            )
            await session.commit()
            await session.refresh(first)
            assert first.digest == digest
            assert first.executions == 2
    finally:
        scheduler.request_stop()
        worker.request_stop()
        await asyncio.gather(sched_task, worker_task, return_exceptions=True)


class AlwaysFailHandler:
    task_type = "dummy"

    async def execute(self, ctx: TaskContext) -> HandlerResult:
        raise RetryableTaskError("synthetic failure", error_type="synthetic")


async def test_retry_then_dead_letter(settings: Settings, session_factory, queue) -> None:
    async with session_factory() as session:
        job = await JobRepository(session).create_dummy_job(
            task_count=1,
            payload={"seed_prefix": f"dlq-{job_seed()}"},
            name="dlq",
            idempotency_key=None,
            priority=0,
            max_attempts=3,
            required_cpu=1,
            required_memory_mb=64,
            required_gpu=0,
            required_gpu_memory_mb=0,
            gpu_preferred=False,
            sleep_ms=0,
        )
        job_id = job.id
        await session.commit()

    scheduler = Scheduler(
        settings=settings, session_factory=session_factory, queue=queue, metrics=Metrics()
    )
    worker = _runtime(
        settings,
        session_factory,
        queue,
        "dlq-w1",
        handlers={"dummy": AlwaysFailHandler()},
    )
    sched_task = asyncio.create_task(scheduler.run_forever())
    worker_task = asyncio.create_task(worker.run())
    try:
        job = await _wait_job(session_factory, job_id, timeout=25)
        assert job.status == JobStatus.FAILED.value
        async with session_factory() as session:
            task = (await session.execute(select(Task).where(Task.job_id == job_id))).scalar_one()
            assert task.status == TaskStatus.DEAD_LETTER.value
            attempts = (
                await session.execute(select(Attempt).where(Attempt.task_id == task.id))
            ).scalars().all()
            assert len(attempts) == 3
            assert all(a.status == "failed" for a in attempts)
    finally:
        scheduler.request_stop()
        worker.request_stop()
        await asyncio.gather(sched_task, worker_task, return_exceptions=True)


async def test_cancel_stops_new_work(settings: Settings, session_factory, queue) -> None:
    async with session_factory() as session:
        jobs = JobRepository(session)
        job = await jobs.create_dummy_job(
            task_count=5,
            payload={"seed_prefix": f"cancel-{job_seed()}"},
            name="cancel",
            idempotency_key=None,
            priority=0,
            max_attempts=3,
            required_cpu=1,
            required_memory_mb=64,
            required_gpu=0,
            required_gpu_memory_mb=0,
            gpu_preferred=False,
            sleep_ms=5_000,
        )
        job_id = job.id
        await jobs.cancel_job(job_id)
        await session.commit()

    scheduler = Scheduler(
        settings=settings, session_factory=session_factory, queue=queue, metrics=Metrics()
    )
    worker = _runtime(settings, session_factory, queue, "cancel-w1")
    sched_task = asyncio.create_task(scheduler.run_forever())
    worker_task = asyncio.create_task(worker.run())
    try:
        await asyncio.sleep(1.0)
        async with session_factory() as session:
            job = await session.get(Job, job_id)
            assert job is not None
            assert job.status == JobStatus.CANCELLED.value
            tasks = (await session.execute(select(Task).where(Task.job_id == job_id))).scalars().all()
            assert all(t.status == TaskStatus.CANCELLED.value for t in tasks)
    finally:
        scheduler.request_stop()
        worker.request_stop()
        await asyncio.gather(sched_task, worker_task, return_exceptions=True)
