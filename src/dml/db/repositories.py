"""PostgreSQL repositories. Domain logic stays out of Redis."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import Select, func, select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from dml.contracts.matching import ResourceRequirements, WorkerSnapshot, is_eligible
from dml.contracts.states import (
    AttemptStatus,
    IN_FLIGHT_TASK_STATUSES,
    JobStatus,
    TERMINAL_TASK_STATUSES,
    TaskStatus,
    WorkerStatus,
    assert_task_transition,
    backoff_seconds,
)
from dml.db.models import Attempt, DummyResult, Job, Task, Worker


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class BeginAttemptResult:
    kind: str  # execute | skip_ack | dead_letter
    task: Task | None = None
    attempt: Attempt | None = None


@dataclass(frozen=True, slots=True)
class NewTaskSpec:
    task_type: str
    payload: dict[str, Any]
    required_cpu: int = 1
    required_memory_mb: int = 256
    required_gpu: int = 0
    required_gpu_memory_mb: int = 0
    gpu_preferred: bool = False


class JobRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get(self, job_id: UUID) -> Job | None:
        return await self.session.get(Job, job_id)

    async def get_by_idempotency(self, key: str) -> Job | None:
        stmt: Select[tuple[Job]] = select(Job).where(Job.idempotency_key == key)
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def list_jobs(self, *, limit: int = 50, offset: int = 0) -> list[Job]:
        stmt = select(Job).order_by(Job.created_at.desc()).limit(limit).offset(offset)
        return list((await self.session.execute(stmt)).scalars())

    async def create_dummy_job(
        self,
        *,
        task_count: int,
        payload: dict[str, Any],
        name: str | None,
        idempotency_key: str | None,
        priority: int,
        max_attempts: int,
        required_cpu: int,
        required_memory_mb: int,
        required_gpu: int,
        required_gpu_memory_mb: int,
        gpu_preferred: bool,
        sleep_ms: int,
    ) -> Job:
        if idempotency_key:
            existing = await self.get_by_idempotency(idempotency_key)
            if existing is not None:
                return existing

        job = Job(
            id=uuid4(),
            idempotency_key=idempotency_key,
            job_type="dummy",
            name=name,
            status=JobStatus.PENDING.value,
            payload={**payload, "sleep_ms": sleep_ms, "task_count": task_count},
            priority=priority,
            task_count=task_count,
        )
        self.session.add(job)
        try:
            await self.session.flush()
        except IntegrityError:
            await self.session.rollback()
            if idempotency_key:
                existing = await self.get_by_idempotency(idempotency_key)
                if existing is not None:
                    return existing
            raise

        tasks = []
        for index in range(task_count):
            seed = payload.get("seed_prefix", "seed") + f":{index}"
            tasks.append(
                Task(
                    id=uuid4(),
                    job_id=job.id,
                    task_type="dummy",
                    payload={"seed": seed, "index": index, "sleep_ms": sleep_ms},
                    status=TaskStatus.PENDING.value,
                    priority=priority,
                    max_attempts=max_attempts,
                    required_cpu=required_cpu,
                    required_memory_mb=required_memory_mb,
                    required_gpu=required_gpu,
                    required_gpu_memory_mb=required_gpu_memory_mb,
                    gpu_preferred=gpu_preferred,
                )
            )
        self.session.add_all(tasks)
        await self.session.flush()
        return job

    async def create_job_with_tasks(
        self,
        *,
        job_type: str,
        name: str | None,
        tasks: list[NewTaskSpec],
        payload: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
        priority: int = 0,
        max_attempts: int = 5,
        dataset_version_id: UUID | None = None,
        pipeline_run_id: UUID | None = None,
    ) -> Job:
        if not tasks:
            raise ValueError("at least one task is required")
        if idempotency_key:
            existing = await self.get_by_idempotency(idempotency_key)
            if existing is not None:
                return existing
        job = Job(
            id=uuid4(),
            idempotency_key=idempotency_key,
            job_type=job_type,
            name=name,
            status=JobStatus.PENDING.value,
            payload=payload or {},
            priority=priority,
            task_count=len(tasks),
            dataset_version_id=dataset_version_id,
            pipeline_run_id=pipeline_run_id,
        )
        self.session.add(job)
        await self.session.flush()
        rows = [
            Task(
                id=uuid4(),
                job_id=job.id,
                task_type=spec.task_type,
                payload=spec.payload,
                status=TaskStatus.PENDING.value,
                priority=priority,
                max_attempts=max_attempts,
                required_cpu=spec.required_cpu,
                required_memory_mb=spec.required_memory_mb,
                required_gpu=spec.required_gpu,
                required_gpu_memory_mb=spec.required_gpu_memory_mb,
                gpu_preferred=spec.gpu_preferred,
            )
            for spec in tasks
        ]
        self.session.add_all(rows)
        await self.session.flush()
        return job

    async def cancel_job(self, job_id: UUID) -> Job | None:
        job = await self.get(job_id)
        if job is None:
            return None
        if job.status in {JobStatus.COMPLETED.value, JobStatus.CANCELLED.value}:
            return job
        job.status = JobStatus.CANCELLED.value
        job.finished_at = _utcnow()
        await self.session.execute(
            update(Task)
            .where(Task.job_id == job_id)
            .where(Task.status.in_([s.value for s in IN_FLIGHT_TASK_STATUSES]))
            .values(status=TaskStatus.CANCELLED.value, finished_at=_utcnow(), lease_until=None)
        )
        return job

    async def refresh_status(self, job_id: UUID) -> None:
        counts_stmt = (
            select(Task.status, func.count())
            .where(Task.job_id == job_id)
            .group_by(Task.status)
        )
        rows = (await self.session.execute(counts_stmt)).all()
        counts = {status: n for status, n in rows}
        job = await self.get(job_id)
        if job is None or job.status == JobStatus.CANCELLED.value:
            return

        total = sum(counts.values())
        succeeded = counts.get(TaskStatus.SUCCEEDED.value, 0)
        dead = counts.get(TaskStatus.DEAD_LETTER.value, 0)
        cancelled = counts.get(TaskStatus.CANCELLED.value, 0)
        in_flight = sum(counts.get(s.value, 0) for s in IN_FLIGHT_TASK_STATUSES)

        if total == 0:
            return
        if succeeded == total:
            job.status = JobStatus.COMPLETED.value
            job.finished_at = _utcnow()
            if job.started_at is None:
                job.started_at = job.created_at
        elif in_flight == 0 and (dead + cancelled + succeeded) == total and dead > 0:
            job.status = JobStatus.FAILED.value
            job.finished_at = _utcnow()
        elif in_flight > 0 or succeeded > 0:
            if job.status == JobStatus.PENDING.value:
                job.status = JobStatus.RUNNING.value
                job.started_at = _utcnow()


class TaskRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get(self, task_id: UUID) -> Task | None:
        return await self.session.get(Task, task_id)

    async def list_for_job(self, job_id: UUID) -> list[Task]:
        stmt = select(Task).where(Task.job_id == job_id).order_by(Task.created_at)
        return list((await self.session.execute(stmt)).scalars())

    async def list_attempts(self, task_id: UUID) -> list[Attempt]:
        stmt = (
            select(Attempt)
            .where(Attempt.task_id == task_id)
            .order_by(Attempt.attempt_number)
        )
        return list((await self.session.execute(stmt)).scalars())

    async def inflight_by_worker(self) -> dict[str, Task]:
        stmt = (
            select(Task)
            .where(Task.status == TaskStatus.RUNNING.value)
            .where(Task.leased_by_worker_id.is_not(None))
        )
        rows = list((await self.session.execute(stmt)).scalars())
        return {t.leased_by_worker_id: t for t in rows if t.leased_by_worker_id}

    async def list_by_statuses(self, statuses: list[str], *, limit: int = 25) -> list[Task]:
        stmt = (
            select(Task)
            .where(Task.status.in_(statuses))
            .order_by(Task.last_failed_at.desc().nullslast(), Task.created_at.desc())
            .limit(limit)
        )
        return list((await self.session.execute(stmt)).scalars())

    async def status_counts(self) -> dict[str, int]:
        rows = (await self.session.execute(select(Task.status, func.count()).group_by(Task.status))).all()
        return {status: int(n) for status, n in rows}

    async def claim_pending_for_publish(self, limit: int) -> list[Task]:
        now = _utcnow()
        stmt = (
            select(Task)
            .where(Task.status == TaskStatus.PENDING.value)
            .where((Task.next_run_at.is_(None)) | (Task.next_run_at <= now))
            .order_by(Task.priority.desc(), Task.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return list((await self.session.execute(stmt)).scalars())

    async def claim_orphaned_queued(self, limit: int) -> list[Task]:
        stmt = (
            select(Task)
            .where(Task.status == TaskStatus.QUEUED.value)
            .where(Task.stream_id.is_(None))
            .order_by(Task.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return list((await self.session.execute(stmt)).scalars())

    async def mark_queued(self, task: Task, *, stream_name: str, stream_id: str) -> None:
        assert_task_transition(TaskStatus(task.status), TaskStatus.QUEUED)
        task.status = TaskStatus.QUEUED.value
        task.stream_name = stream_name
        task.stream_id = stream_id
        task.scheduled_at = _utcnow()

    async def begin_attempt(
        self,
        task_id: UUID,
        *,
        worker_id: str,
        lease_seconds: float,
        capabilities: list[str],
        worker_cpu: int,
        worker_memory_mb: int,
        worker_gpu: int,
        worker_gpu_memory_mb: int,
    ) -> BeginAttemptResult:
        stmt = select(Task).where(Task.id == task_id).with_for_update()
        task = (await self.session.execute(stmt)).scalar_one_or_none()
        if task is None:
            return BeginAttemptResult(kind="skip_ack")

        status = TaskStatus(task.status)
        now = _utcnow()

        if status in TERMINAL_TASK_STATUSES:
            return BeginAttemptResult(kind="skip_ack", task=task)

        if status == TaskStatus.FAILED and task.next_run_at is not None and task.next_run_at > now:
            return BeginAttemptResult(kind="skip_ack", task=task)

        if (
            status == TaskStatus.RUNNING
            and task.lease_until is not None
            and task.lease_until > now
            and task.leased_by_worker_id
            and task.leased_by_worker_id != worker_id
        ):
            return BeginAttemptResult(kind="skip_ack", task=task)

        if task.task_type not in capabilities:
            return BeginAttemptResult(kind="skip_ack", task=task)

        req = ResourceRequirements(
            cpu=task.required_cpu,
            memory_mb=task.required_memory_mb,
            gpu=task.required_gpu,
            gpu_memory_mb=task.required_gpu_memory_mb,
            gpu_preferred=task.gpu_preferred,
        )
        snapshot = WorkerSnapshot(
            worker_id=worker_id,
            capabilities=frozenset(capabilities),
            cpu_count=worker_cpu,
            memory_mb=worker_memory_mb,
            gpu_count=worker_gpu,
            gpu_memory_mb=worker_gpu_memory_mb,
        )
        if not is_eligible(snapshot, task.task_type, req):
            # Wrong worker claimed this pointer. ACK the bad delivery and let the
            # scheduler republish onto the correct stream class.
            if status not in TERMINAL_TASK_STATUSES:
                task.status = TaskStatus.PENDING.value
                task.stream_id = None
                task.stream_name = None
                task.lease_until = None
                task.leased_by_worker_id = None
            return BeginAttemptResult(kind="skip_ack", task=task)

        if status == TaskStatus.RUNNING and task.leased_by_worker_id != worker_id:
            await self._close_open_attempts(task.id, AttemptStatus.LOST, "worker_lost", retryable=True)

        next_attempt = task.current_attempt + 1
        if next_attempt > task.max_attempts:
            assert_task_transition(status, TaskStatus.DEAD_LETTER)
            task.status = TaskStatus.DEAD_LETTER.value
            task.finished_at = now
            task.lease_until = None
            task.leased_by_worker_id = None
            task.last_error_type = "max_attempts"
            task.last_error = "exceeded max_attempts while beginning attempt"
            return BeginAttemptResult(kind="dead_letter", task=task)

        if status != TaskStatus.RUNNING:
            assert_task_transition(status, TaskStatus.RUNNING)

        attempt = Attempt(
            id=uuid4(),
            task_id=task.id,
            attempt_number=next_attempt,
            worker_id=worker_id,
            status=AttemptStatus.RUNNING.value,
        )
        self.session.add(attempt)
        task.current_attempt = next_attempt
        task.status = TaskStatus.RUNNING.value
        task.started_at = now
        task.leased_by_worker_id = worker_id
        task.lease_until = now + timedelta(seconds=lease_seconds)
        await self.session.flush()
        return BeginAttemptResult(kind="execute", task=task, attempt=attempt)

    async def complete_success(
        self, *, task_id: UUID, attempt_id: UUID, attempt_number: int, result: dict[str, Any]
    ) -> bool:
        now = _utcnow()
        stmt = (
            update(Task)
            .where(Task.id == task_id)
            .where(Task.current_attempt == attempt_number)
            .where(Task.status == TaskStatus.RUNNING.value)
            .values(
                status=TaskStatus.SUCCEEDED.value,
                result=result,
                finished_at=now,
                lease_until=None,
                leased_by_worker_id=None,
                last_error=None,
            )
        )
        updated = (await self.session.execute(stmt)).rowcount
        duration = await self._finish_attempt(attempt_id, AttemptStatus.SUCCEEDED)
        _ = duration
        return updated > 0

    async def complete_failure(
        self,
        *,
        task_id: UUID,
        attempt_id: UUID,
        attempt_number: int,
        error_type: str,
        error_message: str,
        retryable: bool,
        max_attempts: int,
        backoff_base: float,
        backoff_cap: float,
        backoff_jitter: float,
    ) -> TaskStatus:
        now = _utcnow()
        task = await self.get(task_id)
        if task is None:
            return TaskStatus.DEAD_LETTER

        await self._finish_attempt(
            attempt_id,
            AttemptStatus.FAILED,
            error_type=error_type,
            error_message=error_message,
            retryable=retryable,
        )

        if task.current_attempt != attempt_number or task.status != TaskStatus.RUNNING.value:
            return TaskStatus(task.status)

        task.last_error = error_message[:4000]
        task.last_error_type = error_type
        task.last_failed_at = now
        if task.first_failed_at is None:
            task.first_failed_at = now
        task.lease_until = None
        task.leased_by_worker_id = None
        task.stream_id = None
        task.stream_name = None

        if not retryable:
            assert_task_transition(TaskStatus.RUNNING, TaskStatus.DEAD_LETTER)
            task.status = TaskStatus.DEAD_LETTER.value
            task.finished_at = now
            return TaskStatus.DEAD_LETTER

        if task.current_attempt >= max_attempts:
            assert_task_transition(TaskStatus.RUNNING, TaskStatus.DEAD_LETTER)
            task.status = TaskStatus.DEAD_LETTER.value
            task.finished_at = now
            return TaskStatus.DEAD_LETTER

        delay = backoff_seconds(
            task.current_attempt, base=backoff_base, cap=backoff_cap, jitter=backoff_jitter
        )
        assert_task_transition(TaskStatus.RUNNING, TaskStatus.FAILED)
        task.status = TaskStatus.FAILED.value
        task.next_run_at = now + timedelta(seconds=delay)
        return TaskStatus.FAILED

    async def release_expired_leases(
        self,
        *,
        stale_worker_ids: list[str],
        backoff_base: float,
        backoff_cap: float,
        backoff_jitter: float,
    ) -> list[UUID]:
        del stale_worker_ids  # liveness is displayed separately; steal is lease-based
        now = _utcnow()
        stmt = (
            select(Task)
            .where(Task.status == TaskStatus.RUNNING.value)
            .where(Task.lease_until.is_not(None))
            .where(Task.lease_until <= now)
            .with_for_update(skip_locked=True)
        )

        tasks = list((await self.session.execute(stmt)).scalars())
        released: list[UUID] = []
        for task in tasks:
            await self._close_open_attempts(task.id, AttemptStatus.LOST, "worker_lost", retryable=True)
            task.last_error = "worker lost or lease expired"
            task.last_error_type = "worker_lost"
            task.last_failed_at = now
            if task.first_failed_at is None:
                task.first_failed_at = now
            task.lease_until = None
            task.leased_by_worker_id = None
            task.stream_id = None
            if task.current_attempt >= task.max_attempts:
                assert_task_transition(TaskStatus.RUNNING, TaskStatus.DEAD_LETTER)
                task.status = TaskStatus.DEAD_LETTER.value
                task.finished_at = now
            else:
                delay = backoff_seconds(
                    task.current_attempt, base=backoff_base, cap=backoff_cap, jitter=backoff_jitter
                )
                assert_task_transition(TaskStatus.RUNNING, TaskStatus.FAILED)
                task.status = TaskStatus.FAILED.value
                task.next_run_at = now + timedelta(seconds=delay)
            released.append(task.id)
        return released

    async def extend_lease(self, task_id: UUID, worker_id: str, lease_seconds: float) -> None:
        now = _utcnow()
        await self.session.execute(
            update(Task)
            .where(Task.id == task_id)
            .where(Task.leased_by_worker_id == worker_id)
            .where(Task.status == TaskStatus.RUNNING.value)
            .values(lease_until=now + timedelta(seconds=lease_seconds))
        )

    async def retry_dead_letter(self, task_id: UUID) -> Task | None:
        task = await self.get(task_id)
        if task is None:
            return None
        if task.status != TaskStatus.DEAD_LETTER.value:
            return task
        assert_task_transition(TaskStatus.DEAD_LETTER, TaskStatus.PENDING)
        task.status = TaskStatus.PENDING.value
        task.next_run_at = _utcnow()
        task.finished_at = None
        task.stream_id = None
        task.current_attempt = 0
        return task

    async def upsert_dummy_result(self, *, task_id: UUID, seed: str, digest: str) -> None:
        stmt = insert(DummyResult).values(task_id=task_id, seed=seed, digest=digest, executions=1)
        stmt = stmt.on_conflict_do_update(
            index_elements=["task_id"],
            set_={
                "digest": stmt.excluded.digest,
                "seed": stmt.excluded.seed,
                "executions": DummyResult.executions + 1,
                "updated_at": func.now(),
            },
        )
        await self.session.execute(stmt)

    async def get_dummy_result(self, task_id: UUID) -> DummyResult | None:
        return await self.session.get(DummyResult, task_id)

    async def _finish_attempt(
        self,
        attempt_id: UUID,
        status: AttemptStatus,
        *,
        error_type: str | None = None,
        error_message: str | None = None,
        retryable: bool | None = None,
    ) -> int | None:
        attempt = await self.session.get(Attempt, attempt_id)
        if attempt is None:
            return None
        now = _utcnow()
        attempt.status = status.value
        attempt.finished_at = now
        attempt.error_type = error_type
        attempt.error_message = error_message
        attempt.retryable = retryable
        duration = int((now - attempt.started_at).total_seconds() * 1000)
        attempt.duration_ms = duration
        return duration

    async def _close_open_attempts(
        self, task_id: UUID, status: AttemptStatus, error_type: str, *, retryable: bool
    ) -> None:
        now = _utcnow()
        open_stmt = (
            select(Attempt)
            .where(Attempt.task_id == task_id)
            .where(Attempt.status == AttemptStatus.RUNNING.value)
        )
        for attempt in (await self.session.execute(open_stmt)).scalars():
            attempt.status = status.value
            attempt.finished_at = now
            attempt.error_type = error_type
            attempt.error_message = "attempt superseded by reclaim or worker loss"
            attempt.retryable = retryable
            attempt.duration_ms = int((now - attempt.started_at).total_seconds() * 1000)

    async def failed_due_for_retry(self, limit: int) -> list[Task]:
        now = _utcnow()
        stmt = (
            select(Task)
            .where(Task.status == TaskStatus.FAILED.value)
            .where(Task.next_run_at.is_not(None))
            .where(Task.next_run_at <= now)
            .order_by(Task.priority.desc(), Task.next_run_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        tasks = list((await self.session.execute(stmt)).scalars())
        for task in tasks:
            assert_task_transition(TaskStatus.FAILED, TaskStatus.PENDING)
            task.status = TaskStatus.PENDING.value
            task.stream_id = None
        return tasks


class WorkerRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def upsert_registration(
        self,
        *,
        worker_id: str,
        hostname: str,
        process_id: int,
        capabilities: list[str],
        cpu_count: int,
        memory_mb: int,
        gpu_count: int,
        gpu_model: str | None,
        gpu_memory_mb: int,
        device: str,
        runtime: str,
    ) -> Worker:
        now = _utcnow()
        stmt = insert(Worker).values(
            worker_id=worker_id,
            hostname=hostname,
            process_id=process_id,
            capabilities=capabilities,
            status=WorkerStatus.IDLE.value,
            cpu_count=cpu_count,
            memory_mb=memory_mb,
            gpu_count=gpu_count,
            gpu_model=gpu_model,
            gpu_memory_mb=gpu_memory_mb,
            device=device,
            runtime=runtime,
            started_at=now,
            last_heartbeat=now,
            created_at=now,
            updated_at=now,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=[Worker.worker_id],
            set_={
                "hostname": hostname,
                "process_id": process_id,
                "capabilities": capabilities,
                "status": WorkerStatus.IDLE.value,
                "cpu_count": cpu_count,
                "memory_mb": memory_mb,
                "gpu_count": gpu_count,
                "gpu_model": gpu_model,
                "gpu_memory_mb": gpu_memory_mb,
                "device": device,
                "runtime": runtime,
                "started_at": now,
                "last_heartbeat": now,
                "updated_at": now,
            },
        )
        await self.session.execute(stmt)
        worker = await self.session.get(Worker, worker_id)
        assert worker is not None
        return worker

    async def heartbeat(self, worker_id: str, status: WorkerStatus) -> None:
        await self.session.execute(
            update(Worker)
            .where(Worker.worker_id == worker_id)
            .values(last_heartbeat=_utcnow(), status=status.value, updated_at=_utcnow())
        )

    async def mark_stale(self, stale_after_seconds: float) -> list[str]:
        cutoff = _utcnow() - timedelta(seconds=stale_after_seconds)
        stmt = (
            select(Worker)
            .where(Worker.last_heartbeat < cutoff)
            .where(Worker.status.notin_([WorkerStatus.STOPPED.value, WorkerStatus.UNHEALTHY.value]))
        )
        workers = list((await self.session.execute(stmt)).scalars())
        ids = []
        for worker in workers:
            worker.status = WorkerStatus.UNHEALTHY.value
            ids.append(worker.worker_id)
        return ids

    async def list_workers(self) -> list[Worker]:
        stmt = select(Worker).order_by(Worker.worker_id)
        return list((await self.session.execute(stmt)).scalars())

    async def get(self, worker_id: str) -> Worker | None:
        return await self.session.get(Worker, worker_id)

    async def healthy_gpu_exists(self, *, stale_after_seconds: float, capability: str) -> bool:
        cutoff = _utcnow() - timedelta(seconds=stale_after_seconds)
        count = (
            await self.session.execute(
                text(
                    """
                    SELECT count(*) FROM workers
                    WHERE last_heartbeat >= :cutoff
                      AND status NOT IN ('stopped', 'unhealthy')
                      AND gpu_count > 0
                      AND :cap = ANY(capabilities)
                    """
                ),
                {"cutoff": cutoff, "cap": capability},
            )
        ).scalar_one()
        return bool(count)

    async def ping(self) -> None:
        await self.session.execute(text("SELECT 1"))
