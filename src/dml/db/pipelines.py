"""Small internal DAG. Not Airflow — fixed node graph for this platform."""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dml.contracts.states import JobStatus
from dml.db.datasets import DatasetRepository, job_terminal_status
from dml.db.models import DatasetVersion, Job, PipelineNode, PipelineRun
from dml.db.repositories import JobRepository, NewTaskSpec

DATA_GRAPH: list[tuple[str, list[str]]] = [
    ("ingest", []),
    ("etl", ["ingest"]),
    ("dedupe", ["etl"]),
    ("embed", ["dedupe"]),
    ("index", ["embed"]),
]
TRAIN_GRAPH: list[tuple[str, list[str]]] = [
    ("train", ["dedupe"]),
    ("evaluate", ["train"]),
    ("promote", ["evaluate"]),
]


class PipelineRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create_run(
        self,
        *,
        version: DatasetVersion,
        include_training: bool,
        version_already_ready: bool,
    ) -> PipelineRun:
        run = PipelineRun(
            id=uuid4(),
            dataset_version_id=version.id,
            status="running",
            include_training=include_training,
        )
        self.session.add(run)
        await self.session.flush()
        graph = list(DATA_GRAPH)
        if include_training:
            graph.extend(TRAIN_GRAPH)
        for name, deps in graph:
            status = "pending"
            if version_already_ready and name in {"ingest", "etl"}:
                status = "succeeded"
            self.session.add(
                PipelineNode(
                    id=uuid4(),
                    pipeline_run_id=run.id,
                    name=name,
                    task_type=name,
                    depends_on=deps,
                    status=status,
                )
            )
        await self.session.flush()
        return run

    async def get(self, run_id: UUID) -> PipelineRun | None:
        return await self.session.get(PipelineRun, run_id)

    async def list_runs(self, limit: int = 50) -> list[PipelineRun]:
        stmt = select(PipelineRun).order_by(PipelineRun.created_at.desc()).limit(limit)
        return list((await self.session.execute(stmt)).scalars())

    async def list_nodes(self, run_id: UUID) -> list[PipelineNode]:
        stmt = select(PipelineNode).where(PipelineNode.pipeline_run_id == run_id)
        return list((await self.session.execute(stmt)).scalars())

    async def active_runs(self) -> list[PipelineRun]:
        stmt = (
            select(PipelineRun)
            .where(PipelineRun.status.in_(["pending", "running"]))
            .with_for_update(skip_locked=True)
        )
        return list((await self.session.execute(stmt)).scalars())

    async def cancel(self, run: PipelineRun) -> PipelineRun:
        run.status = "cancelled"
        nodes = await self.list_nodes(run.id)
        jobs = JobRepository(self.session)
        for node in nodes:
            if node.status not in {"succeeded", "failed"}:
                node.status = "cancelled"
            if node.job_id:
                await jobs.cancel_job(node.job_id)
        return run

    async def advance(self, run: PipelineRun, *, settings_max_attempts: int) -> None:
        if run.status == "cancelled":
            return
        nodes = {n.name: n for n in await self.list_nodes(run.id)}
        for node in nodes.values():
            if node.job_id and node.status == "running":
                job_status = await job_terminal_status(self.session, node.job_id)
                if job_status == JobStatus.COMPLETED.value:
                    node.status = "succeeded"
                elif job_status in {JobStatus.FAILED.value, JobStatus.CANCELLED.value}:
                    node.status = "failed"
                    run.status = "failed"

        if run.status in {"failed", "cancelled"}:
            if run.finished_at is None:
                run.finished_at = datetime.now(timezone.utc)
            return

        version = await self.session.get(DatasetVersion, run.dataset_version_id)
        if version is None:
            run.status = "failed"
            return

        for node in nodes.values():
            if node.status != "pending":
                continue
            if any(nodes[dep].status != "succeeded" for dep in node.depends_on):
                if any(nodes[dep].status == "failed" for dep in node.depends_on):
                    node.status = "failed"
                    run.status = "failed"
                continue
            job = await self._create_node_job(node, version, settings_max_attempts)
            if job == "attached":
                continue
            if job is None:
                node.status = "succeeded"
            else:
                node.job_id = job.id
                node.status = "running"

        if all(n.status in {"succeeded", "failed", "cancelled"} for n in nodes.values()):
            run.status = "failed" if any(n.status == "failed" for n in nodes.values()) else "completed"
            if run.finished_at is None:
                run.finished_at = datetime.now(timezone.utc)

    async def _create_node_job(
        self, node: PipelineNode, version: DatasetVersion, max_attempts: int
    ) -> Job | str | None:
        jobs = JobRepository(self.session)
        datasets = DatasetRepository(self.session)
        shards = await datasets.list_shards(version.id)
        base = {
            "dataset_id": str(version.dataset_id),
            "dataset_version_id": str(version.id),
            "version_label": version.version_label,
            "pipeline_run_id": str(node.pipeline_run_id),
        }
        if node.name == "ingest":
            ingest_status = await job_terminal_status(self.session, version.ingest_job_id)
            if version.status == "ready" or ingest_status == JobStatus.COMPLETED.value:
                return None
            if version.ingest_job_id:
                node.job_id = version.ingest_job_id
                node.status = "running"
                return "attached"
            return None
        if node.name == "etl":
            etl_status = await job_terminal_status(self.session, version.etl_job_id)
            if version.status == "ready" or etl_status == JobStatus.COMPLETED.value:
                return None
            if version.etl_job_id:
                node.job_id = version.etl_job_id
                node.status = "running"
                return "attached"
            return await datasets.create_etl_job(version, max_attempts=max_attempts)
        if node.name == "dedupe":
            return await jobs.create_job_with_tasks(
                job_type="dedupe",
                name=f"dedupe:{version.version_label}",
                tasks=[NewTaskSpec(task_type="dedupe", payload=base)],
                payload=base,
                max_attempts=max_attempts,
                dataset_version_id=version.id,
                pipeline_run_id=node.pipeline_run_id,
            )
        if node.name == "embed":
            specs = [
                NewTaskSpec(
                    task_type="embedding",
                    payload={**base, "shard_id": str(shard.id), "shard_key": shard.shard_key},
                    gpu_preferred=True,
                )
                for shard in shards
            ] or [NewTaskSpec(task_type="embedding", payload=base, gpu_preferred=True)]
            return await jobs.create_job_with_tasks(
                job_type="embedding",
                name=f"embed:{version.version_label}",
                tasks=specs,
                payload=base,
                max_attempts=max_attempts,
                dataset_version_id=version.id,
                pipeline_run_id=node.pipeline_run_id,
            )
        if node.name == "index":
            return await jobs.create_job_with_tasks(
                job_type="index",
                name=f"index:{version.version_label}",
                tasks=[NewTaskSpec(task_type="index", payload=base)],
                payload=base,
                max_attempts=max_attempts,
                dataset_version_id=version.id,
                pipeline_run_id=node.pipeline_run_id,
            )
        if node.name == "train":
            return await jobs.create_job_with_tasks(
                job_type="training",
                name=f"train:{version.version_label}",
                tasks=[
                    NewTaskSpec(
                        task_type="training",
                        payload=base,
                        required_cpu=2,
                        required_memory_mb=1024,
                        gpu_preferred=True,
                    )
                ],
                payload=base,
                max_attempts=max_attempts,
                dataset_version_id=version.id,
                pipeline_run_id=node.pipeline_run_id,
            )
        if node.name == "evaluate":
            return await jobs.create_job_with_tasks(
                job_type="evaluation",
                name=f"eval:{version.version_label}",
                tasks=[NewTaskSpec(task_type="evaluation", payload=base, gpu_preferred=True)],
                payload=base,
                max_attempts=max_attempts,
                dataset_version_id=version.id,
                pipeline_run_id=node.pipeline_run_id,
            )
        if node.name == "promote":
            return await jobs.create_job_with_tasks(
                job_type="artifact",
                name=f"promote:{version.version_label}",
                tasks=[NewTaskSpec(task_type="artifact", payload=base)],
                payload=base,
                max_attempts=max_attempts,
                dataset_version_id=version.id,
                pipeline_run_id=node.pipeline_run_id,
            )
        return None
