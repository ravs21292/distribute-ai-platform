"""Disposable worker runtime: register, heartbeat, claim, fence, execute, ACK."""

from __future__ import annotations

import asyncio
import signal
import time
from collections.abc import Mapping

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from dml import __version__
from dml.config import Settings
from dml.contracts.errors import classify_exception
from dml.contracts.matching import worker_streams
from dml.contracts.messages import ClaimedTask
from dml.contracts.states import WorkerStatus
from dml.db.repositories import JobRepository, TaskRepository, WorkerRepository
from dml.queue.interface import TaskQueue
from dml.telemetry.logging import bind_context, clear_context, get_logger
from dml.telemetry.metrics import Metrics
from dml.worker.handlers.base import TaskContext, TaskHandler
from dml.worker.resources import DetectedResources

log = get_logger("dml.worker")


class WorkerRuntime:
    def __init__(
        self,
        *,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        queue: TaskQueue,
        handlers: Mapping[str, TaskHandler],
        resources: DetectedResources,
        metrics: Metrics,
        worker_id: str,
        capabilities: list[str],
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.queue = queue
        self.handlers = dict(handlers)
        self.resources = resources
        self.metrics = metrics
        self.worker_id = worker_id
        self.capabilities = capabilities
        self._stop = asyncio.Event()
        self._busy = False
        self._current_task_id = None
        self._status = WorkerStatus.STARTING

    @property
    def streams(self) -> list[str]:
        return worker_streams(
            self.capabilities,
            gpu_count=self.resources.gpu_count,
            prefix=self.settings.stream_prefix,
        )

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.request_stop)
            except NotImplementedError:
                # Windows: signals in asyncio are limited; main() also traps KeyboardInterrupt.
                pass

        await self._register()
        self._status = WorkerStatus.IDLE
        heartbeat = asyncio.create_task(self._heartbeat_loop(), name="heartbeat")
        reclaim = asyncio.create_task(self._reclaim_loop(), name="reclaim")
        try:
            while not self._stop.is_set():
                claimed = await self.queue.claim(
                    self.worker_id,
                    self.streams,
                    count=1,
                    block_ms=self.settings.claim_block_ms,
                )
                if self._stop.is_set():
                    break
                if not claimed:
                    continue
                await self._process(claimed[0])
        finally:
            self._status = WorkerStatus.DRAINING
            heartbeat.cancel()
            reclaim.cancel()
            await asyncio.gather(heartbeat, reclaim, return_exceptions=True)
            await self._set_status(WorkerStatus.STOPPED)
            log.info("worker_stopped", worker_id=self.worker_id)

    def request_stop(self) -> None:
        self._status = WorkerStatus.DRAINING
        self._stop.set()

    async def _register(self) -> None:
        async with self.session_factory() as session:
            repo = WorkerRepository(session)
            await repo.upsert_registration(
                worker_id=self.worker_id,
                hostname=self.resources.hostname,
                process_id=self.resources.process_id,
                capabilities=self.capabilities,
                cpu_count=self.resources.cpu_count,
                memory_mb=self.resources.memory_mb,
                gpu_count=self.resources.gpu_count,
                gpu_model=self.resources.gpu_model,
                gpu_memory_mb=self.resources.gpu_memory_mb,
                device=self.resources.device,
                runtime=f"dml-worker/{__version__}",
            )
            await session.commit()
        log.info(
            "worker_registered",
            worker_id=self.worker_id,
            capabilities=self.capabilities,
            device=self.resources.device,
            streams=self.streams,
        )

    async def _set_status(self, status: WorkerStatus) -> None:
        self._status = status
        try:
            async with self.session_factory() as session:
                await WorkerRepository(session).heartbeat(self.worker_id, status)
                await session.commit()
        except Exception as exc:
            log.warning("worker_status_update_failed", error=str(exc), status=status.value)

    async def _heartbeat_loop(self) -> None:
        while not self._stop.is_set():
            status = WorkerStatus.BUSY if self._busy else self._status
            if status == WorkerStatus.STARTING:
                status = WorkerStatus.IDLE
            try:
                async with self.session_factory() as session:
                    await WorkerRepository(session).heartbeat(self.worker_id, status)
                    if self._current_task_id is not None:
                        await TaskRepository(session).extend_lease(
                            self._current_task_id,
                            self.worker_id,
                            self.settings.task_lease_seconds,
                        )
                    await session.commit()
            except Exception as exc:
                log.warning("heartbeat_failed", error=str(exc), worker_id=self.worker_id)
            try:
                await asyncio.wait_for(
                    self._stop.wait(), timeout=self.settings.heartbeat_interval_seconds
                )
            except TimeoutError:
                continue

    async def _reclaim_loop(self) -> None:
        while not self._stop.is_set():
            try:
                stale = await self.queue.reclaim_stale(
                    self.worker_id,
                    self.streams,
                    min_idle_ms=self.settings.claim_min_idle_ms,
                    count=5,
                )
                for item in stale:
                    if self._stop.is_set():
                        break
                    log.info(
                        "reclaimed_pending_task",
                        task_id=str(item.message.task_id),
                        stream=item.stream,
                        worker_id=self.worker_id,
                    )
                    await self._process(item)
            except Exception as exc:
                log.warning("reclaim_failed", error=str(exc), worker_id=self.worker_id)
            try:
                await asyncio.wait_for(
                    self._stop.wait(), timeout=self.settings.heartbeat_interval_seconds
                )
            except TimeoutError:
                continue

    async def _process(self, claimed: ClaimedTask) -> None:
        clear_context()
        bind_context(
            worker_id=self.worker_id,
            task_id=str(claimed.message.task_id),
            job_id=str(claimed.message.job_id),
        )
        self._busy = True
        self.metrics.worker_task_count.set(1)
        started = time.perf_counter()
        try:
            await self._process_inner(claimed, started)
        finally:
            self._busy = False
            self._current_task_id = None
            self.metrics.worker_task_count.set(0)
            clear_context()

    async def _process_inner(self, claimed: ClaimedTask, started: float) -> None:
        async with self.session_factory() as session:
            tasks = TaskRepository(session)
            begin = await tasks.begin_attempt(
                claimed.message.task_id,
                worker_id=self.worker_id,
                lease_seconds=self.settings.task_lease_seconds,
                capabilities=self.capabilities,
                worker_cpu=self.resources.cpu_count,
                worker_memory_mb=self.resources.memory_mb,
                worker_gpu=self.resources.gpu_count,
                worker_gpu_memory_mb=self.resources.gpu_memory_mb,
            )
            await session.commit()

            if begin.kind != "execute":
                if begin.kind == "dead_letter":
                    self.metrics.task_dead_letter_total.inc()
                log.info("skip_task", kind=begin.kind, task_id=str(claimed.message.task_id))
                await self._ack(claimed)
                return

            assert begin.task is not None and begin.attempt is not None
            self._current_task_id = begin.task.id
            handler = self.handlers.get(begin.task.task_type)
            if handler is None:
                from dml.contracts.errors import PermanentTaskError

                await self._fail(
                    session,
                    claimed,
                    begin,
                    PermanentTaskError(
                        f"no handler registered for task_type={begin.task.task_type}",
                        error_type="missing_handler",
                    ),
                    started,
                )
                return

            ctx = TaskContext(
                task_id=begin.task.id,
                job_id=begin.task.job_id,
                attempt_id=begin.attempt.id,
                attempt_number=begin.attempt.attempt_number,
                task_type=begin.task.task_type,
                payload=dict(begin.task.payload),
                worker_id=self.worker_id,
                session=session,
            )
            bind_context(attempt_id=str(begin.attempt.id), attempt_number=begin.attempt.attempt_number)
            try:
                result = await handler.execute(ctx)
                won = await tasks.complete_success(
                    task_id=begin.task.id,
                    attempt_id=begin.attempt.id,
                    attempt_number=begin.attempt.attempt_number,
                    result=result.data,
                )
                await JobRepository(session).refresh_status(begin.task.job_id)
                await session.commit()
                duration = time.perf_counter() - started
                self.metrics.task_duration_seconds.labels(task_type=begin.task.task_type).observe(
                    duration
                )
                if won:
                    self.metrics.tasks_completed_total.labels(task_type=begin.task.task_type).inc()
                    log.info("task_succeeded", duration_s=round(duration, 4))
                else:
                    log.info("task_success_lost_fence", duration_s=round(duration, 4))
                await self._ack(claimed)
            except Exception as exc:
                await self._fail(session, claimed, begin, exc, started)

    async def _fail(self, session, claimed, begin, exc: BaseException, started: float) -> None:
        retryable, error_type = classify_exception(exc)
        from dml.contracts.errors import PermanentTaskError

        if isinstance(exc, PermanentTaskError):
            retryable = False
        tasks = TaskRepository(session)
        assert begin.task is not None and begin.attempt is not None
        try:
            outcome = await tasks.complete_failure(
                task_id=begin.task.id,
                attempt_id=begin.attempt.id,
                attempt_number=begin.attempt.attempt_number,
                error_type=error_type,
                error_message=str(exc),
                retryable=retryable,
                max_attempts=begin.task.max_attempts,
                backoff_base=self.settings.retry_base_seconds,
                backoff_cap=self.settings.retry_cap_seconds,
                backoff_jitter=self.settings.retry_jitter,
            )
            await JobRepository(session).refresh_status(begin.task.job_id)
            await session.commit()
        except Exception as persist_exc:
            await session.rollback()
            log.error(
                "failure_persist_failed",
                error=str(persist_exc),
                original=str(exc),
            )
            return

        self.metrics.tasks_failed_total.labels(
            task_type=begin.task.task_type, error_type=error_type
        ).inc()
        if retryable and outcome.value == "failed":
            self.metrics.task_retries_total.inc()
        if outcome.value == "dead_letter":
            self.metrics.task_dead_letter_total.inc()
        log.warning(
            "task_failed",
            retryable=retryable,
            error_type=error_type,
            outcome=outcome.value,
            duration_s=round(time.perf_counter() - started, 4),
        )
        await self._ack(claimed)

    async def _ack(self, claimed: ClaimedTask) -> None:
        try:
            await self.queue.ack(claimed)
        except Exception as exc:
            log.error("ack_failed", error=str(exc), message_id=claimed.message_id)


