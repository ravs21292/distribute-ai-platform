"""Build the live operations overview used by REST and WebSocket clients."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from pydantic import BaseModel
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from dml.api.ops_progress import resolve_searchable_progress
from dml.api.routes.pipelines import PipelineOut, _out
from dml.api.routes.workers import decorate
from dml.config import Settings
from dml.contracts.states import TaskStatus
from dml.db.models import Attempt, Chunk, Dataset, DatasetVersion, Task
from dml.db.pipelines import DATA_GRAPH, TRAIN_GRAPH, PipelineRepository
from dml.db.repositories import JobRepository, TaskRepository, WorkerRepository


class FailureOut(BaseModel):
    task_id: UUID
    job_id: UUID
    task_type: str
    status: str
    attempts: int
    max_attempts: int
    error_type: str | None
    error: str | None
    worker_id: str | None


class DataPlaneOut(BaseModel):
    datasets: int
    versions_ready: int
    documents: int
    chunks: int
    embeddings: int
    embedding_model: str
    embedding_backend: str
    embedding_dim: int
    latest_dataset: str | None = None
    latest_version_id: UUID | None = None
    latest_version_label: str | None = None
    latest_status: str | None = None
    source_bytes: int = 0


class ClusterOut(BaseModel):
    mode: str
    workers_live: int
    workers_busy: int
    workers_idle: int
    pipelines_active: int
    jobs_running: int
    tasks_queued: int
    tasks_running: int
    tasks_failed: int
    tasks_dead_letter: int
    tasks_succeeded: int


class ProgressOut(BaseModel):
    phase: str
    percent: float
    embeddings_done: int
    embeddings_total: int
    tasks_done: int
    tasks_total: int
    nodes_done: int
    nodes_total: int
    eta_seconds: float | None
    eta_human: str | None
    rate_embeddings_per_min: float | None
    rate_tasks_per_min: float | None
    message: str


class PipelineFocusOut(BaseModel):
    """Which pipeline the DAG panel represents — never confuse with global totals."""

    pipeline_id: UUID
    status: str
    dataset_name: str | None
    version_label: str | None
    dataset_version_id: UUID
    version_status: str | None
    source_bytes: int
    chunks: int
    embeddings: int
    note: str


class ActivityOut(BaseModel):
    at: str
    worker_id: str | None
    task_type: str
    status: str
    task_id: str
    job_id: str | None
    duration_ms: int | None
    detail: str | None = None


class OverviewOut(BaseModel):
    cluster: ClusterOut
    data: DataPlaneOut
    progress: ProgressOut
    pipeline_focus: PipelineFocusOut | None = None
    workers: list[dict[str, Any]]
    pipelines: list[PipelineOut]
    jobs: list[dict[str, Any]]
    activity: list[ActivityOut]
    failures: list[FailureOut]
    failure_policy: dict[str, Any]
    dag: dict[str, Any]
    server_time: str


def _human_eta(seconds: float | None) -> str | None:
    if seconds is None or seconds < 0 or seconds != seconds:  # NaN
        return None
    s = int(seconds)
    if s < 60:
        return f"~{s}s remaining"
    if s < 3600:
        return f"~{s // 60}m {s % 60}s remaining"
    h, rem = divmod(s, 3600)
    return f"~{h}h {rem // 60}m remaining"


async def build_overview(session: AsyncSession, settings: Settings) -> OverviewOut:
    workers_repo = WorkerRepository(session)
    tasks_repo = TaskRepository(session)
    jobs_repo = JobRepository(session)
    pipes = PipelineRepository(session)

    inflight = await tasks_repo.inflight_by_worker()
    job_workers = {str(task.job_id): wid for wid, task in inflight.items() if task.job_id}
    live_workers = []
    for worker in await workers_repo.list_workers():
        payload = decorate(worker, inflight, settings)
        if payload.healthy:
            live_workers.append(payload)

    counts = await tasks_repo.status_counts()
    jobs = await jobs_repo.list_jobs(limit=20)
    runs = await pipes.list_runs(limit=8)
    pipeline_out = [_out(run, await pipes.list_nodes(run.id)) for run in runs]
    for pipeline in pipeline_out:
        for node in pipeline.nodes:
            if node.job_id is not None:
                node.worker_id = job_workers.get(str(node.job_id))
    active_pipes = sum(1 for p in pipeline_out if p.status in {"pending", "running"})

    busy = sum(1 for w in live_workers if w.status == "busy")
    failures = []
    for task in await tasks_repo.list_by_statuses(
        [TaskStatus.FAILED.value, TaskStatus.DEAD_LETTER.value], limit=20
    ):
        failures.append(
            FailureOut(
                task_id=task.id,
                job_id=task.job_id,
                task_type=task.task_type,
                status=task.status,
                attempts=task.current_attempt,
                max_attempts=task.max_attempts,
                error_type=task.last_error_type,
                error=(task.last_error or "")[:400] or None,
                worker_id=task.leased_by_worker_id,
            )
        )

    datasets = int((await session.execute(select(func.count()).select_from(Dataset))).scalar_one() or 0)
    versions_ready = int(
        (
            await session.execute(
                select(func.count()).select_from(DatasetVersion).where(DatasetVersion.status == "ready")
            )
        ).scalar_one()
        or 0
    )
    latest = (
        await session.execute(select(DatasetVersion).order_by(DatasetVersion.created_at.desc()).limit(1))
    ).scalar_one_or_none()
    # Prefer an in-flight pipeline; otherwise the run for the latest dataset version.
    active = next((p for p in pipeline_out if p.status == "running"), None)
    if active is None and latest is not None:
        active = next(
            (p for p in pipeline_out if p.dataset_version_id == latest.id),
            None,
        )
    if active is None and pipeline_out:
        active = pipeline_out[0]
    latest_ds_name = None
    if latest is not None:
        ds = await session.get(Dataset, latest.dataset_id)
        latest_ds_name = ds.name if ds else None
    embeddings = int(
        (await session.execute(text("SELECT count(*) FROM chunk_embeddings"))).scalar_one() or 0
    )
    version_chunks = 0
    version_embeddings = 0
    if latest is not None:
        version_chunks = int(
            (
                await session.execute(
                    select(func.count())
                    .select_from(Chunk)
                    .where(Chunk.dataset_version_id == latest.id)
                    .where(Chunk.is_duplicate.is_(False))
                )
            ).scalar_one()
            or 0
        )
        version_embeddings = int(
            (
                await session.execute(
                    text(
                        "SELECT count(*) FROM chunk_embeddings WHERE dataset_version_id = :vid"
                    ),
                    {"vid": str(latest.id)},
                )
            ).scalar_one()
            or 0
        )
    chunks = int((await session.execute(select(func.count()).select_from(Chunk))).scalar_one() or 0)
    documents = int(
        (await session.execute(select(func.coalesce(func.sum(DatasetVersion.document_count), 0)))).scalar_one()
        or 0
    )

    activity = await _recent_activity(session, limit=40)
    progress = await _progress(
        session,
        active_pipeline=active,
        latest=latest,
        version_chunks=version_chunks,
        version_embeddings=version_embeddings,
        counts=counts,
        busy=busy,
    )
    pipeline_focus = await _pipeline_focus(session, active)

    data = DataPlaneOut(
        datasets=datasets,
        versions_ready=versions_ready,
        documents=documents,
        chunks=chunks,
        embeddings=embeddings,
        embedding_model=settings.embedding_model,
        embedding_backend=settings.embedding_backend,
        embedding_dim=settings.embedding_dim,
        latest_dataset=latest_ds_name,
        latest_version_id=latest.id if latest else None,
        latest_version_label=latest.version_label if latest else None,
        latest_status=latest.status if latest else None,
        source_bytes=int(latest.source_bytes) if latest else 0,
    )
    cluster = ClusterOut(
        mode="processing" if busy or active_pipes else "idle",
        workers_live=len(live_workers),
        workers_busy=busy,
        workers_idle=len(live_workers) - busy,
        pipelines_active=active_pipes,
        jobs_running=sum(1 for j in jobs if j.status == "running"),
        tasks_queued=counts.get(TaskStatus.QUEUED.value, 0) + counts.get(TaskStatus.PENDING.value, 0),
        tasks_running=counts.get(TaskStatus.RUNNING.value, 0),
        tasks_failed=counts.get(TaskStatus.FAILED.value, 0),
        tasks_dead_letter=counts.get(TaskStatus.DEAD_LETTER.value, 0),
        tasks_succeeded=counts.get(TaskStatus.SUCCEEDED.value, 0),
    )
    return OverviewOut(
        cluster=cluster,
        data=data,
        progress=progress,
        pipeline_focus=pipeline_focus,
        workers=[w.model_dump(mode="json") for w in live_workers],
        pipelines=pipeline_out,
        jobs=[
            {
                "id": str(j.id),
                "name": j.name,
                "job_type": j.job_type,
                "status": j.status,
                "task_count": j.task_count,
                "created_at": j.created_at.isoformat() if j.created_at else None,
                "started_at": j.started_at.isoformat() if j.started_at else None,
                "finished_at": j.finished_at.isoformat() if j.finished_at else None,
            }
            for j in jobs
        ],
        activity=activity,
        failures=failures,
        failure_policy={
            "max_attempts": settings.max_attempts,
            "lease_seconds": settings.task_lease_seconds,
            "stale_worker_seconds": settings.worker_stale_after_seconds,
            "ack_after": "postgres_commit",
            "delivery": "at_least_once_redis_streams",
            "reclaim": "XAUTOCLAIM + lease expiry",
            "dead_letter": "after max_attempts retryable failures",
            "quality_gate": "promote only if eval perplexity passes",
        },
        dag={
            "data": [{"name": n, "depends_on": deps} for n, deps in DATA_GRAPH],
            "train": [{"name": n, "depends_on": deps} for n, deps in TRAIN_GRAPH],
        },
        server_time=datetime.now(timezone.utc).isoformat(),
    )


async def _pipeline_focus(
    session: AsyncSession, active_pipeline: PipelineOut | None
) -> PipelineFocusOut | None:
    if active_pipeline is None:
        return None
    version = await session.get(DatasetVersion, active_pipeline.dataset_version_id)
    dataset_name = None
    version_label = None
    version_status = None
    source_bytes = 0
    chunks = 0
    embeddings = 0
    if version is not None:
        version_label = version.version_label
        version_status = version.status
        source_bytes = int(version.source_bytes or 0)
        ds = await session.get(Dataset, version.dataset_id)
        dataset_name = ds.name if ds else None
        chunks = int(
            (
                await session.execute(
                    select(func.count())
                    .select_from(Chunk)
                    .where(Chunk.dataset_version_id == version.id)
                    .where(Chunk.is_duplicate.is_(False))
                )
            ).scalar_one()
            or 0
        )
        embeddings = int(
            (
                await session.execute(
                    text(
                        "SELECT count(*) FROM chunk_embeddings WHERE dataset_version_id = :vid"
                    ),
                    {"vid": str(version.id)},
                )
            ).scalar_one()
            or 0
        )
    mb = source_bytes / (1024 * 1024) if source_bytes else 0.0
    return PipelineFocusOut(
        pipeline_id=active_pipeline.id,
        status=active_pipeline.status,
        dataset_name=dataset_name,
        version_label=version_label,
        dataset_version_id=active_pipeline.dataset_version_id,
        version_status=version_status,
        source_bytes=source_bytes,
        chunks=chunks,
        embeddings=embeddings,
        note=(
            f"DAG is for {dataset_name or 'unknown'} {version_label or ''} "
            f"({mb:.2f} MB source · {chunks} chunks · {embeddings} embeddings). "
            "Global Postgres totals in Data plane include other versions and failed partial ETL."
        ),
    )


async def _recent_activity(session: AsyncSession, *, limit: int) -> list[ActivityOut]:
    stmt = (
        select(Attempt, Task)
        .join(Task, Task.id == Attempt.task_id)
        .order_by(Attempt.started_at.desc())
        .limit(limit)
    )
    rows = (await session.execute(stmt)).all()
    out: list[ActivityOut] = []
    for attempt, task in rows:
        at = attempt.finished_at or attempt.started_at
        detail = None
        if attempt.error_message:
            detail = attempt.error_message[:160]
        elif task.leased_by_worker_id and attempt.status == "running":
            detail = f"leased by {task.leased_by_worker_id}"
        out.append(
            ActivityOut(
                at=at.isoformat() if at else datetime.now(timezone.utc).isoformat(),
                worker_id=attempt.worker_id or task.leased_by_worker_id,
                task_type=task.task_type,
                status=attempt.status,
                task_id=str(task.id),
                job_id=str(task.job_id) if task.job_id else None,
                duration_ms=attempt.duration_ms,
                detail=detail,
            )
        )
    return out


async def _progress(
    session: AsyncSession,
    *,
    active_pipeline: PipelineOut | None,
    latest: DatasetVersion | None,
    version_chunks: int,
    version_embeddings: int,
    counts: dict[str, int],
    busy: int,
) -> ProgressOut:
    nodes_done = 0
    nodes_total = 0
    phase = "idle"
    pipeline_for_latest = (
        active_pipeline is not None
        and latest is not None
        and active_pipeline.dataset_version_id == latest.id
    )
    if active_pipeline and (
        active_pipeline.status in {"pending", "running"} or pipeline_for_latest
    ):
        nodes_total = len(active_pipeline.nodes) or 1
        nodes_done = sum(1 for n in active_pipeline.nodes if n.status == "succeeded")
        running = next((n for n in active_pipeline.nodes if n.status == "running"), None)
        if active_pipeline.status == "failed":
            phase = "failed"
        elif active_pipeline.status == "completed":
            phase = "completed"
        elif running:
            phase = running.name
        elif active_pipeline.status == "running":
            phase = "pipeline"
        else:
            phase = active_pipeline.status
    elif latest and latest.status in {"ingesting", "processing", "etl", "failed"}:
        phase = latest.status

    # Latest dataset version always overrides a stale "completed" pipeline banner.
    if latest is not None and latest.status == "failed":
        phase = "failed"
    elif (
        latest is not None
        and latest.status in {"ingesting", "processing", "etl"}
        and phase == "completed"
        and not pipeline_for_latest
    ):
        phase = latest.status

    tasks_done = counts.get(TaskStatus.SUCCEEDED.value, 0)
    tasks_total = max(
        tasks_done
        + counts.get(TaskStatus.RUNNING.value, 0)
        + counts.get(TaskStatus.QUEUED.value, 0)
        + counts.get(TaskStatus.PENDING.value, 0),
        1,
    )

    # Embedding ETA from recent embedding attempt durations + remaining chunks.
    embed_total = max(version_chunks, version_embeddings, 1)
    embed_done = min(version_embeddings, embed_total)
    rate_emb = await _embedding_rate(session)
    rate_tasks = await _task_rate(session)

    eta_seconds = None
    message = "Cluster idle — waiting for a dataset or pipeline."
    decided = resolve_searchable_progress(
        phase=phase,
        latest_status=latest.status if latest else None,
        version_chunks=version_chunks,
        version_embeddings=version_embeddings,
    )
    if decided[1]:
        phase, message, percent = decided
    elif phase in {"embed", "embedding"} or (
        active_pipeline
        and any(n.name == "embed" and n.status == "running" for n in active_pipeline.nodes)
    ):
        remaining = max(embed_total - embed_done, 0)
        if rate_emb and rate_emb > 0:
            eta_seconds = (remaining / rate_emb) * 60.0
        message = f"Embedding {embed_done}/{embed_total} canonical chunks into pgvector."
        percent = min(99.0, (nodes_done / max(nodes_total, 1)) * 70 + (embed_done / embed_total) * 30)
    elif phase in {"ingest", "etl", "ingesting", "processing"}:
        message = f"Data plane phase: {phase}. Workers are sharding / chunking into Postgres."
        percent = min(40.0, (tasks_done / tasks_total) * 40)
        if rate_tasks and rate_tasks > 0:
            left = max(tasks_total - tasks_done, 0)
            eta_seconds = (left / rate_tasks) * 60.0
    elif active_pipeline and active_pipeline.status == "running":
        message = f"Pipeline node '{phase}' running across the worker fleet."
        percent = min(95.0, (nodes_done / max(nodes_total, 1)) * 100)
        if rate_tasks and rate_tasks > 0:
            left = max(tasks_total - tasks_done, 0)
            eta_seconds = (left / rate_tasks) * 60.0
    elif busy:
        message = "Workers are busy on jobs outside an active pipeline view."
        percent = min(90.0, (tasks_done / tasks_total) * 100)
    else:
        percent = 0.0 if not tasks_done else min(100.0, (tasks_done / tasks_total) * 100)

    return ProgressOut(
        phase=phase,
        percent=round(percent, 1),
        embeddings_done=embed_done if version_chunks else version_embeddings,
        embeddings_total=embed_total if version_chunks else max(version_embeddings, 1),
        tasks_done=tasks_done,
        tasks_total=tasks_total,
        nodes_done=nodes_done,
        nodes_total=nodes_total,
        eta_seconds=round(eta_seconds, 1) if eta_seconds is not None else None,
        eta_human=_human_eta(eta_seconds),
        rate_embeddings_per_min=round(rate_emb, 2) if rate_emb else None,
        rate_tasks_per_min=round(rate_tasks, 2) if rate_tasks else None,
        message=message,
    )


async def _embedding_rate(session: AsyncSession) -> float | None:
    """Approx embeddings/min from recent succeeded embedding attempts."""
    stmt = (
        select(Attempt.duration_ms, Attempt.finished_at)
        .join(Task, Task.id == Attempt.task_id)
        .where(Task.task_type == "embedding")
        .where(Attempt.status == "succeeded")
        .where(Attempt.finished_at.is_not(None))
        .order_by(Attempt.finished_at.desc())
        .limit(30)
    )
    rows = (await session.execute(stmt)).all()
    if len(rows) < 2:
        return None
    durations = [r[0] for r in rows if r[0] and r[0] > 0]
    if not durations:
        return None
    avg_ms = sum(durations) / len(durations)
    # Each embedding task covers one shard; treat as 1 unit/min from avg duration.
    if avg_ms <= 0:
        return None
    return 60_000.0 / avg_ms


async def _task_rate(session: AsyncSession) -> float | None:
    stmt = (
        select(Attempt.finished_at)
        .where(Attempt.status == "succeeded")
        .where(Attempt.finished_at.is_not(None))
        .order_by(Attempt.finished_at.desc())
        .limit(40)
    )
    times = [r[0] for r in (await session.execute(stmt)).all() if r[0] is not None]
    if len(times) < 3:
        return None
    newest, oldest = times[0], times[-1]
    span = (newest - oldest).total_seconds()
    if span <= 1:
        return None
    return (len(times) - 1) / span * 60.0
