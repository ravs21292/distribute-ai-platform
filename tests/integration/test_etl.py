from __future__ import annotations

import asyncio
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select

from dml.config import Settings
from dml.db.datasets import DatasetRepository
from dml.db.models import Chunk, Dataset, DatasetSource, DatasetVersion
from dml.queue.redis_streams import RedisTaskQueue
from dml.scheduler.service import Scheduler
from dml.storage import LocalObjectStore
from dml.telemetry.metrics import Metrics
from dml.worker.handlers.dummy import DummyHandler
from dml.worker.handlers.etl import ETLHandler
from dml.worker.handlers.ingest import IngestHandler
from dml.worker.resources import DetectedResources
from dml.worker.runtime import WorkerRuntime

pytestmark = pytest.mark.integration


def _resources() -> DetectedResources:
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


async def _wait_version(session_factory, version_id: UUID, timeout: float = 40.0) -> DatasetVersion:
    deadline = asyncio.get_event_loop().time() + timeout
    last = None
    while asyncio.get_event_loop().time() < deadline:
        async with session_factory() as session:
            version = await session.get(DatasetVersion, version_id)
            last = version
            if version and version.status in {"ready", "failed"}:
                return version
        await asyncio.sleep(0.1)
    raise AssertionError(f"version {version_id} did not finish, last={last.status if last else None}")


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


async def test_ingest_etl_across_workers_and_reclaim(
    settings: Settings, session_factory, queue, tmp_path: Path
) -> None:
    store = LocalObjectStore(tmp_path)
    corpus = "\n\n".join(f"Document {i}. " + ("lorem ipsum " * 20) for i in range(40))
    source_name = "wiki.txt"
    # object key filled after version number is known; write after we create dataset
    async with session_factory() as session:
        repo = DatasetRepository(session)
        dataset = await repo.create(name=f"wiki-{uuid4().hex[:8]}", description="phase2")
        number = await repo.next_version_number(dataset.id)
        version_label = f"v{number}"
        source_key = f"raw/{dataset.id}/{version_label}/sources/{source_name}"
        store.put_bytes(source_key, corpus.encode("utf-8"))
        source = DatasetSource(
            id=uuid4(),
            dataset_version_id=uuid4(),
            filename=source_name,
            content_type="txt",
            checksum_sha256="abc",
            byte_count=len(corpus.encode()),
            object_key=source_key,
        )
        version = await repo.create_version(
            dataset=dataset,
            sources=[source],
            chunking_config={"lines_per_shard": 15, "chunk_chars": 120, "chunk_overlap": 20},
            transform_version="etl-v1",
            max_attempts=5,
            lines_per_shard=15,
            version_number=number,
        )
        version_id = version.id
        await session.commit()

    handlers = {
        "dummy": DummyHandler(),
        "ingest": IngestHandler(store),
        "etl": ETLHandler(store),
    }
    scheduler = Scheduler(
        settings=settings, session_factory=session_factory, queue=queue, metrics=Metrics()
    )
    workers = [
        WorkerRuntime(
            settings=settings,
            session_factory=session_factory,
            queue=queue,
            handlers=handlers,
            resources=_resources(),
            metrics=Metrics(),
            worker_id=f"etl-w{i}",
            capabilities=["ingest", "etl"],
        )
        for i in range(3)
    ]
    sched_task = asyncio.create_task(scheduler.run_forever())
    worker_tasks = [asyncio.create_task(w.run()) for w in workers]
    try:
        await asyncio.sleep(0.6)
        workers[0].request_stop()
        version = await _wait_version(session_factory, version_id, timeout=45)
        assert version.status == "ready"
        assert version.chunk_count > 0
        assert version.shard_count >= 1
        async with session_factory() as session:
            hashes = list(
                (
                    await session.execute(
                        select(Chunk.content_hash).where(Chunk.dataset_version_id == version_id)
                    )
                ).scalars()
            )
            assert hashes
            assert all(len(h) == 64 for h in hashes)
            processed = (
                await session.execute(
                    select(func.count()).select_from(Chunk).where(Chunk.dataset_version_id == version_id)
                )
            ).scalar_one()
            assert processed == version.chunk_count
    finally:
        scheduler.request_stop()
        for w in workers:
            w.request_stop()
        await asyncio.gather(sched_task, *worker_tasks, return_exceptions=True)
