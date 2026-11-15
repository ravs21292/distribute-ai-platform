"""Control-plane scheduler: publish, reclaim leases, roll up job status."""

from __future__ import annotations

import asyncio
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from sqlalchemy import select

from dml.config import Settings
from dml.contracts.matching import ResourceRequirements, publish_resource_class, stream_name
from dml.contracts.messages import TaskMessage
from dml.contracts.states import JobStatus, TaskStatus
from dml.db.models import Job, Task
from dml.db.repositories import JobRepository, TaskRepository, WorkerRepository
from dml.queue.interface import TaskQueue
from dml.telemetry.logging import get_logger
from dml.telemetry.metrics import Metrics

log = get_logger("dml.scheduler")


class Scheduler:
    def __init__(
        self,
        *,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        queue: TaskQueue,
        metrics: Metrics,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.queue = queue
        self.metrics = metrics
        self._stop = asyncio.Event()

    def request_stop(self) -> None:
        self._stop.set()

    async def run_forever(self) -> None:
        log.info("scheduler_started", poll=self.settings.scheduler_poll_seconds)
        while not self._stop.is_set():
            try:
                await self.tick()
            except Exception as exc:
                log.error("scheduler_tick_failed", error=str(exc), exc_info=True)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.settings.scheduler_poll_seconds)
            except TimeoutError:
                continue
        log.info("scheduler_stopped")

    async def tick(self) -> None:
        async with self.session_factory() as session:
            workers = WorkerRepository(session)
            tasks = TaskRepository(session)
            jobs = JobRepository(session)

            stale_ids = await workers.mark_stale(self.settings.worker_stale_after_seconds)
            if stale_ids:
                log.warning("workers_marked_unhealthy", worker_ids=stale_ids)

            released = await tasks.release_expired_leases(
                stale_worker_ids=stale_ids,
                backoff_base=self.settings.retry_base_seconds,
                backoff_cap=self.settings.retry_cap_seconds,
                backoff_jitter=self.settings.retry_jitter,
            )
            if released:
                log.warning("leases_released", task_ids=[str(t) for t in released])

            await tasks.failed_due_for_retry(self.settings.publish_batch_size)
            pending = await tasks.claim_pending_for_publish(self.settings.publish_batch_size)
            orphaned = await tasks.claim_orphaned_queued(self.settings.publish_batch_size)
            seen: set[UUID] = set()
            to_publish: list[Task] = []
            for task in (*pending, *orphaned):
                if task.id in seen:
                    continue
                seen.add(task.id)
                to_publish.append(task)

            gpu_cache: dict[str, bool] = {}
            touched_jobs: set[UUID] = set()

            for task in to_publish:
                job = await session.get(Job, task.job_id)
                if job is not None and job.status == JobStatus.CANCELLED.value:
                    task.status = TaskStatus.CANCELLED.value
                    touched_jobs.add(task.job_id)
                    continue
                resource_class = await self._resource_class(session, task, gpu_cache)
                message = TaskMessage(
                    task_id=task.id,
                    job_id=task.job_id,
                    task_type=task.task_type,
                    resource_class=resource_class,
                    priority=task.priority,
                )
                try:
                    stream, message_id = await self.queue.publish(message)
                except Exception as exc:
                    log.error("publish_failed", task_id=str(task.id), error=str(exc))
                    continue
                await tasks.mark_queued(task, stream_name=stream, stream_id=message_id)
                touched_jobs.add(task.job_id)
                log.info(
                    "task_published",
                    task_id=str(task.id),
                    stream=stream,
                    message_id=message_id,
                    resource_class=resource_class,
                )

            open_jobs = (
                await session.execute(
                    select(Job.id).where(
                        Job.status.in_([JobStatus.PENDING.value, JobStatus.RUNNING.value])
                    )
                )
            ).scalars()
            for job_id in set(touched_jobs) | set(open_jobs):
                await jobs.refresh_status(job_id)

            await self._advance_dataset_versions(session)
            await self._advance_pipelines(session)

            all_workers = await workers.list_workers()
            self.metrics.workers_registered.set(len(all_workers))
            healthy = 0
            from datetime import datetime, timedelta, timezone

            cutoff = datetime.now(timezone.utc) - timedelta(
                seconds=self.settings.worker_stale_after_seconds
            )
            for worker in all_workers:
                if worker.last_heartbeat >= cutoff and worker.status not in {"stopped", "unhealthy"}:
                    healthy += 1
            self.metrics.workers_healthy.set(healthy)
            await session.commit()

        await self._scrape_queue_depths()

    async def _advance_dataset_versions(self, session: AsyncSession) -> None:
        from dml.db.datasets import DatasetRepository, job_terminal_status

        repo = DatasetRepository(session)
        for version in await repo.versions_needing_etl():
            status = await job_terminal_status(session, version.ingest_job_id)
            if status == JobStatus.FAILED.value or status == JobStatus.CANCELLED.value:
                await repo.mark_failed(version)
                log.warning("dataset_version_failed", version_id=str(version.id), ingest_status=status)
            elif status == JobStatus.COMPLETED.value:
                job = await repo.create_etl_job(version, max_attempts=self.settings.max_attempts)
                if job is None:
                    await repo.mark_failed(version)
                    log.warning("dataset_version_no_shards", version_id=str(version.id))
                else:
                    log.info("etl_job_created", version_id=str(version.id), job_id=str(job.id))
        for version in await repo.versions_needing_ready():
            status = await job_terminal_status(session, version.etl_job_id)
            if status == JobStatus.COMPLETED.value:
                await repo.mark_ready(version)
                log.info("dataset_version_ready", version_id=str(version.id), chunks=version.chunk_count)
            elif status in {JobStatus.FAILED.value, JobStatus.CANCELLED.value}:
                shards = await repo.list_shards(version.id)
                processed = sum(1 for s in shards if s.status == "processed")
                ratio = (processed / len(shards)) if shards else 0.0
                if ratio >= self.settings.dataset_ready_min_shard_ratio and processed > 0:
                    await repo.mark_ready(version)
                    log.warning(
                        "dataset_version_ready_partial",
                        version_id=str(version.id),
                        processed_shards=processed,
                        total_shards=len(shards),
                        ratio=round(ratio, 6),
                        chunks=version.chunk_count,
                    )
                else:
                    await repo.mark_failed(version)

    async def _advance_pipelines(self, session: AsyncSession) -> None:
        from dml.db.pipelines import PipelineRepository

        repo = PipelineRepository(session)
        for run in await repo.active_runs():
            await repo.advance(run, settings_max_attempts=self.settings.max_attempts)

    async def _resource_class(
        self, session: AsyncSession, task: Task, cache: dict[str, bool]
    ) -> str:
        req = ResourceRequirements(
            cpu=task.required_cpu,
            memory_mb=task.required_memory_mb,
            gpu=task.required_gpu,
            gpu_memory_mb=task.required_gpu_memory_mb,
            gpu_preferred=task.gpu_preferred,
        )
        if req.gpu_required:
            return "gpu"
        if not req.gpu_preferred:
            return "cpu"
        if task.task_type not in cache:
            try:
                cache[task.task_type] = await WorkerRepository(session).healthy_gpu_exists(
                    stale_after_seconds=self.settings.worker_stale_after_seconds,
                    capability=task.task_type,
                )
            except Exception as exc:
                log.warning("gpu_probe_failed", task_type=task.task_type, error=str(exc))
                cache[task.task_type] = False
        return publish_resource_class(req, healthy_gpu_worker_exists=cache[task.task_type])

    async def _scrape_queue_depths(self) -> None:
        kinds = (
            "dummy",
            "ingest",
            "etl",
            "dedupe",
            "embedding",
            "index",
            "training",
            "evaluation",
            "artifact",
        )
        streams = [stream_name(kind, "cpu", prefix=self.settings.stream_prefix) for kind in kinds]
        streams.append(stream_name("dummy", "gpu", prefix=self.settings.stream_prefix))
        streams.append(stream_name("embedding", "gpu", prefix=self.settings.stream_prefix))
        streams.append(stream_name("training", "gpu", prefix=self.settings.stream_prefix))
        for stream in streams:
            try:
                depth = await self.queue.depth(stream)
            except Exception:
                depth = 0
            self.metrics.queue_depth.labels(stream=stream).set(depth)
