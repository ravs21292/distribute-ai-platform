"""Dataset / version / shard / chunk persistence."""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dml.db.models import Chunk, Dataset, DatasetSource, DatasetVersion, Job, Shard
from dml.db.repositories import JobRepository, NewTaskSpec
from dml.security.governance import source_fingerprint as fingerprint_of


class DatasetRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create(
        self,
        *,
        name: str,
        description: str | None,
        classification: str = "internal",
        pii_policy: str = "redact",
    ) -> Dataset:
        dataset = Dataset(
            id=uuid4(),
            name=name,
            description=description,
            classification=classification,
            pii_policy=pii_policy,
        )
        self.session.add(dataset)
        await self.session.flush()
        return dataset

    async def update_governance(
        self,
        dataset: Dataset,
        *,
        classification: str | None = None,
        pii_policy: str | None = None,
    ) -> Dataset:
        if classification is not None:
            dataset.classification = classification
        if pii_policy is not None:
            dataset.pii_policy = pii_policy
        await self.session.flush()
        return dataset

    async def get(self, dataset_id: UUID) -> Dataset | None:
        return await self.session.get(Dataset, dataset_id)

    async def get_by_name(self, name: str) -> Dataset | None:
        return (await self.session.execute(select(Dataset).where(Dataset.name == name))).scalar_one_or_none()

    async def list_datasets(self) -> list[Dataset]:
        return list((await self.session.execute(select(Dataset).order_by(Dataset.created_at.desc()))).scalars())

    async def list_versions(self, dataset_id: UUID) -> list[DatasetVersion]:
        stmt = (
            select(DatasetVersion)
            .where(DatasetVersion.dataset_id == dataset_id)
            .order_by(DatasetVersion.version_number.desc())
        )
        return list((await self.session.execute(stmt)).scalars())

    async def get_version(self, version_id: UUID) -> DatasetVersion | None:
        return await self.session.get(DatasetVersion, version_id)

    async def find_ready_by_fingerprint(
        self, dataset_id: UUID, fingerprint: str
    ) -> DatasetVersion | None:
        stmt = (
            select(DatasetVersion)
            .where(DatasetVersion.dataset_id == dataset_id)
            .where(DatasetVersion.source_fingerprint == fingerprint)
            .where(DatasetVersion.status == "ready")
            .order_by(DatasetVersion.version_number.desc())
            .limit(1)
        )
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def next_version_number(self, dataset_id: UUID) -> int:
        current = (
            await self.session.execute(
                select(func.max(DatasetVersion.version_number)).where(
                    DatasetVersion.dataset_id == dataset_id
                )
            )
        ).scalar_one()
        return int(current or 0) + 1

    async def create_version(
        self,
        *,
        dataset: Dataset,
        sources: list[DatasetSource],
        chunking_config: dict[str, Any],
        transform_version: str,
        max_attempts: int,
        lines_per_shard: int,
        version_number: int | None = None,
        commit: bool = True,
    ) -> DatasetVersion:
        number = version_number or await self.next_version_number(dataset.id)
        version = DatasetVersion(
            id=uuid4(),
            dataset_id=dataset.id,
            version_number=number,
            version_label=f"v{number}",
            status="draft" if not commit else "ingesting",
            transform_version=transform_version,
            chunking_config=chunking_config,
            source_bytes=sum(s.byte_count for s in sources),
            source_fingerprint=fingerprint_of([s.checksum_sha256 for s in sources]) if sources else None,
        )
        self.session.add(version)
        await self.session.flush()
        for source in sources:
            source.dataset_version_id = version.id
            self.session.add(source)
        await self.session.flush()
        if commit:
            await self.start_ingest(
                dataset=dataset,
                version=version,
                sources=sources,
                max_attempts=max_attempts,
                lines_per_shard=lines_per_shard,
            )
        return version

    async def add_sources(
        self, version: DatasetVersion, sources: list[DatasetSource]
    ) -> DatasetVersion:
        if version.status != "draft":
            raise ValueError("can only append sources to a draft version")
        for source in sources:
            source.dataset_version_id = version.id
            self.session.add(source)
            version.source_bytes = int(version.source_bytes or 0) + int(source.byte_count or 0)
        await self.session.flush()
        existing = (
            await self.session.execute(
                select(DatasetSource).where(DatasetSource.dataset_version_id == version.id)
            )
        ).scalars()
        checksums = [s.checksum_sha256 for s in existing]
        version.source_fingerprint = fingerprint_of(checksums) if checksums else None
        return version

    async def start_ingest(
        self,
        *,
        dataset: Dataset,
        version: DatasetVersion,
        sources: list[DatasetSource] | None = None,
        max_attempts: int,
        lines_per_shard: int,
    ) -> DatasetVersion:
        if sources is None:
            sources = list(
                (
                    await self.session.execute(
                        select(DatasetSource).where(DatasetSource.dataset_version_id == version.id)
                    )
                ).scalars()
            )
        if not sources:
            raise ValueError("version has no sources to ingest")
        if version.status not in {"draft", "ingesting"}:
            raise ValueError(f"cannot start ingest from status {version.status}")
        # Identical ready fingerprint → mark reused and skip work.
        if version.source_fingerprint:
            existing = await self.find_ready_by_fingerprint(dataset.id, version.source_fingerprint)
            if existing is not None and existing.id != version.id:
                version.status = "ready"
                version.reused_from_version_id = existing.id
                version.document_count = existing.document_count
                version.chunk_count = existing.chunk_count
                version.shard_count = existing.shard_count
                return version
        tasks = [
            NewTaskSpec(
                task_type="ingest",
                payload={
                    "dataset_id": str(dataset.id),
                    "dataset_version_id": str(version.id),
                    "version_label": version.version_label,
                    "source_id": str(source.id),
                    "source_key": source.object_key,
                    "filename": source.filename,
                    "content_type": source.content_type,
                    "lines_per_shard": lines_per_shard,
                },
            )
            for source in sources
        ]
        job = await JobRepository(self.session).create_job_with_tasks(
            job_type="ingest",
            name=f"{dataset.name}:{version.version_label}:ingest",
            tasks=tasks,
            payload={"dataset_version_id": str(version.id)},
            max_attempts=max_attempts,
            dataset_version_id=version.id,
        )
        version.status = "ingesting"
        version.ingest_job_id = job.id
        return version

    async def list_sources(self, version_id: UUID) -> list[DatasetSource]:
        stmt = (
            select(DatasetSource)
            .where(DatasetSource.dataset_version_id == version_id)
            .order_by(DatasetSource.filename)
        )
        return list((await self.session.execute(stmt)).scalars())

    async def list_shards(self, version_id: UUID) -> list[Shard]:
        stmt = select(Shard).where(Shard.dataset_version_id == version_id).order_by(Shard.shard_key)
        return list((await self.session.execute(stmt)).scalars())

    async def upsert_shard(
        self,
        *,
        dataset_version_id: UUID,
        source_id: UUID | None,
        shard_key: str,
        status: str,
        raw_object_key: str | None,
        processed_object_key: str | None = None,
        line_count: int | None = None,
        document_count: int | None = None,
        chunk_count: int | None = None,
        byte_count: int | None = None,
    ) -> Shard:
        stmt = select(Shard).where(
            Shard.dataset_version_id == dataset_version_id, Shard.shard_key == shard_key
        )
        shard = (await self.session.execute(stmt)).scalar_one_or_none()
        if shard is None:
            shard = Shard(
                id=uuid4(),
                dataset_version_id=dataset_version_id,
                source_id=source_id,
                shard_key=shard_key,
            )
            self.session.add(shard)
        elif source_id is not None:
            shard.source_id = source_id
        shard.status = status
        if raw_object_key is not None:
            shard.raw_object_key = raw_object_key
        if processed_object_key is not None:
            shard.processed_object_key = processed_object_key
        if line_count is not None:
            shard.line_count = line_count
        if document_count is not None:
            shard.document_count = document_count
        if chunk_count is not None:
            shard.chunk_count = chunk_count
        if byte_count is not None:
            shard.byte_count = byte_count
        await self.session.flush()
        return shard

    async def replace_shard_chunks(
        self,
        *,
        shard: Shard,
        chunks: list[Chunk],
    ) -> None:
        await self.session.execute(delete(Chunk).where(Chunk.shard_id == shard.id))
        self.session.add_all(chunks)
        await self.session.flush()

    async def versions_needing_etl(self, limit: int = 20) -> list[DatasetVersion]:
        stmt = (
            select(DatasetVersion)
            .where(DatasetVersion.status == "ingesting")
            .where(DatasetVersion.etl_job_id.is_(None))
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return list((await self.session.execute(stmt)).scalars())

    async def versions_needing_ready(self, limit: int = 20) -> list[DatasetVersion]:
        stmt = (
            select(DatasetVersion)
            .where(DatasetVersion.status == "transforming")
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return list((await self.session.execute(stmt)).scalars())

    async def create_etl_job(self, version: DatasetVersion, *, max_attempts: int) -> Job | None:
        shards = await self.list_shards(version.id)
        if not shards:
            return None
        dataset = await self.session.get(Dataset, version.dataset_id)
        assert dataset is not None
        ready_shards = [shard for shard in shards if shard.raw_object_key]
        if not ready_shards:
            return None
        sources = {
            s.id: s
            for s in (
                await self.session.execute(
                    select(DatasetSource).where(DatasetSource.dataset_version_id == version.id)
                )
            ).scalars()
        }
        tasks = []
        for shard in ready_shards:
            source = sources.get(shard.source_id) if shard.source_id else None
            content_type = source.content_type if source is not None else "txt"
            tasks.append(
                NewTaskSpec(
                    task_type="etl",
                    payload={
                        "dataset_id": str(version.dataset_id),
                        "dataset_version_id": str(version.id),
                        "version_label": version.version_label,
                        "shard_id": str(shard.id),
                        "shard_key": shard.shard_key,
                        "raw_object_key": shard.raw_object_key,
                        "content_type": content_type,
                        "chunk_chars": version.chunking_config.get("chunk_chars", 1000),
                        "chunk_overlap": version.chunking_config.get("chunk_overlap", 100),
                        "transform_version": version.transform_version,
                        "pii_policy": dataset.pii_policy,
                    },
                )
            )
        job = await JobRepository(self.session).create_job_with_tasks(
            job_type="etl",
            name=f"{dataset.name}:{version.version_label}:etl",
            tasks=tasks,
            payload={"dataset_version_id": str(version.id)},
            max_attempts=max_attempts,
            dataset_version_id=version.id,
        )
        version.etl_job_id = job.id
        version.status = "transforming"
        version.shard_count = len(shards)
        return job

    async def mark_failed(self, version: DatasetVersion) -> None:
        version.status = "failed"

    async def mark_ready(self, version: DatasetVersion) -> None:
        shards = await self.list_shards(version.id)
        chunk_total = (
            await self.session.execute(
                select(func.count()).select_from(Chunk).where(Chunk.dataset_version_id == version.id)
            )
        ).scalar_one()
        version.document_count = sum(s.document_count for s in shards)
        version.chunk_count = int(chunk_total or 0)
        version.shard_count = len(shards)
        version.status = "ready"

    async def create_recovery_etl_job(
        self, version: DatasetVersion, *, max_attempts: int
    ) -> Job | None:
        """Re-ETL shards that never reached processed (enterprise recovery)."""
        shards = await self.list_shards(version.id)
        pending = [s for s in shards if s.status != "processed" and s.raw_object_key]
        if not pending:
            return None
        dataset = await self.session.get(Dataset, version.dataset_id)
        assert dataset is not None
        sources = {
            s.id: s
            for s in (
                await self.session.execute(
                    select(DatasetSource).where(DatasetSource.dataset_version_id == version.id)
                )
            ).scalars()
        }
        tasks = []
        for shard in pending:
            source = sources.get(shard.source_id) if shard.source_id else None
            content_type = source.content_type if source is not None else "txt"
            tasks.append(
                NewTaskSpec(
                    task_type="etl",
                    payload={
                        "dataset_id": str(version.dataset_id),
                        "dataset_version_id": str(version.id),
                        "version_label": version.version_label,
                        "shard_id": str(shard.id),
                        "shard_key": shard.shard_key,
                        "raw_object_key": shard.raw_object_key,
                        "content_type": content_type,
                        "chunk_chars": (version.chunking_config or {}).get("chunk_chars", 1000),
                        "chunk_overlap": (version.chunking_config or {}).get("chunk_overlap", 100),
                        "transform_version": version.transform_version,
                        "pii_policy": dataset.pii_policy,
                    },
                )
            )
        job = await JobRepository(self.session).create_job_with_tasks(
            job_type="etl",
            name=f"{dataset.name}:{version.version_label}:etl-recover",
            tasks=tasks,
            payload={"dataset_version_id": str(version.id), "recovery": True},
            max_attempts=max_attempts,
            dataset_version_id=version.id,
        )
        version.etl_job_id = job.id
        version.status = "transforming"
        return job


async def job_terminal_status(session: AsyncSession, job_id: UUID | None) -> str | None:
    if job_id is None:
        return None
    job = await session.get(Job, job_id)
    return job.status if job else None
