from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select, text

from dml.config import Settings
from dml.db.datasets import DatasetRepository
from dml.db.models import Chunk, DatasetVersion, Shard, TrainingRun
from dml.db.pipelines import PipelineRepository
from dml.db.registry import ModelRegistry
from dml.ml.embeddings import LocalEmbeddingProvider
from dml.queue.redis_streams import RedisTaskQueue
from dml.scheduler.service import Scheduler
from dml.storage import LocalObjectStore
from dml.telemetry.metrics import Metrics
from dml.worker.handlers.artifact import ArtifactHandler
from dml.worker.handlers.dedupe import DedupeHandler
from dml.worker.handlers.dummy import DummyHandler
from dml.worker.handlers.embedding import EmbeddingHandler
from dml.worker.handlers.evaluation import EvaluationHandler
from dml.worker.handlers.indexing import IndexHandler
from dml.worker.handlers.training import TrainingHandler
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


def _hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


async def _seed_ready_version(session, texts: list[str]) -> DatasetVersion:
    repo = DatasetRepository(session)
    dataset = await repo.create(name=f"phase37-{uuid4().hex[:8]}", description="ready")
    version = DatasetVersion(
        id=uuid4(),
        dataset_id=dataset.id,
        version_number=1,
        version_label="v1",
        status="ready",
        transform_version="etl-v1",
        chunking_config={"chunk_chars": 80},
        document_count=len(texts),
        chunk_count=len(texts),
        shard_count=1,
    )
    session.add(version)
    await session.flush()
    shard = Shard(
        id=uuid4(),
        dataset_version_id=version.id,
        shard_key="shard-0000",
        status="completed",
        line_count=len(texts),
        document_count=len(texts),
        chunk_count=len(texts),
    )
    session.add(shard)
    await session.flush()
    for i, text in enumerate(texts):
        session.add(
            Chunk(
                id=uuid4(),
                dataset_version_id=version.id,
                shard_id=shard.id,
                document_id=f"doc-{i}",
                ordinal=i,
                text=text,
                content_hash=_hash(text),
                byte_count=len(text.encode()),
                token_count=len(text.split()),
                source_metadata={"filename": "tiny-corpus.txt"},
            )
        )
    await session.flush()
    return version


async def _wait_pipeline(session_factory, run_id: UUID, timeout: float = 90.0):
    from dml.db.models import PipelineRun

    deadline = asyncio.get_event_loop().time() + timeout
    last = None
    while asyncio.get_event_loop().time() < deadline:
        async with session_factory() as session:
            run = await session.get(PipelineRun, run_id)
            last = run
            if run and run.status in {"completed", "failed", "cancelled"}:
                return run
        await asyncio.sleep(0.15)
    raise AssertionError(f"pipeline {run_id} did not finish, last={last.status if last else None}")


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


async def test_registry_promote_and_rollback(session_factory) -> None:
    async with session_factory() as session:
        try:
            await session.execute(text("SELECT 1 FROM model_aliases LIMIT 1"))
        except Exception as exc:
            await session.rollback()
            pytest.skip(f"phase 3-7 schema missing: {exc}")
        registry = ModelRegistry(session)
        first = await registry.publish(
            name=f"tinygpt-test-{uuid4().hex[:6]}",
            artifact_key="training/demo/checkpoint-1.npz",
            evaluation_id=None,
            alias="candidate",
        )
        second = await registry.publish(
            name=first.model_name,
            artifact_key="training/demo/checkpoint-2.npz",
            evaluation_id=None,
            alias="candidate",
        )
        await registry.set_alias(first.model_name, "production", first.version)
        prod = await registry.resolve(first.model_name, "production")
        assert prod is not None and prod.version == first.version
        await registry.set_alias(first.model_name, "production", second.version)
        await registry.set_alias(first.model_name, "production", first.version)
        rolled = await registry.resolve(first.model_name, "production")
        assert rolled is not None and rolled.version == first.version
        cand = await registry.resolve(first.model_name, "candidate")
        assert cand is not None and cand.version == second.version
        await session.commit()


async def test_pipeline_embed_search_train_and_skip(
    settings: Settings, session_factory, queue, tmp_path: Path
) -> None:
    store = LocalObjectStore(tmp_path)
    texts = [
        "Workers are disposable. PostgreSQL is the source of truth for every job.",
        "Workers are disposable. PostgreSQL is the source of truth for every job.",
        "TinyGPT writes checkpoints so a replacement worker can resume training.",
        "Search ranks chunks with pgvector cosine distance after embeddings exist.",
        "The quality gate publishes a candidate alias only when perplexity is acceptable. " * 3,
    ]
    async with session_factory() as session:
        try:
            await session.execute(text("SELECT 1 FROM pipeline_runs LIMIT 1"))
        except Exception as exc:
            await session.rollback()
            pytest.skip(f"phase 3-7 schema missing: {exc}")
        version = await _seed_ready_version(session, texts)
        version_id = version.id
        run = await PipelineRepository(session).create_run(
            version=version, include_training=True, version_already_ready=True
        )
        run_id = run.id
        await session.commit()

    provider = LocalEmbeddingProvider(
        dim=settings.embedding_dim,
        model=settings.embedding_model,
        model_version=settings.embedding_model_version,
    )
    handlers = {
        "dummy": DummyHandler(),
        "dedupe": DedupeHandler(),
        "embedding": EmbeddingHandler(provider),
        "index": IndexHandler(),
        "training": TrainingHandler(store, settings),
        "evaluation": EvaluationHandler(store, settings),
        "artifact": ArtifactHandler(),
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
            worker_id=f"p37-w{i}",
            capabilities=list(handlers),
        )
        for i in range(2)
    ]
    sched_task = asyncio.create_task(scheduler.run_forever())
    worker_tasks = [asyncio.create_task(w.run()) for w in workers]
    try:
        finished = await _wait_pipeline(session_factory, run_id, timeout=90)
        assert finished.status == "completed", finished.status
        async with session_factory() as session:
            nodes = await PipelineRepository(session).list_nodes(run_id)
            by_name = {n.name: n.status for n in nodes}
            assert by_name["ingest"] == "succeeded"
            assert by_name["etl"] == "succeeded"
            assert by_name["dedupe"] == "succeeded"
            assert by_name["embed"] == "succeeded"
            assert by_name["index"] == "succeeded"
            assert by_name["train"] == "succeeded"
            assert by_name["evaluate"] == "succeeded"
            assert by_name["promote"] == "succeeded"
            dups = (
                await session.execute(
                    select(func.count()).select_from(Chunk).where(
                        Chunk.dataset_version_id == version_id, Chunk.is_duplicate.is_(True)
                    )
                )
            ).scalar_one()
            assert dups >= 1
            embedded = (
                await session.execute(
                    text(
                        "SELECT count(*) FROM chunk_embeddings e "
                        "JOIN chunks c ON c.content_hash = e.content_hash "
                        "WHERE c.dataset_version_id = :vid AND c.is_duplicate = false"
                    ),
                    {"vid": str(version_id)},
                )
            ).scalar_one()
            assert int(embedded) >= 3
            train = (
                await session.execute(
                    select(TrainingRun)
                    .where(TrainingRun.dataset_version_id == version_id)
                    .order_by(TrainingRun.created_at.desc())
                )
            ).scalars().first()
            assert train is not None
            assert train.current_step >= settings.train_max_steps
            assert train.latest_checkpoint_key
            assert store.exists(train.latest_checkpoint_key)
            hits = (
                await session.execute(
                    text(
                        """
                        SELECT c.text
                        FROM chunks c
                        JOIN chunk_embeddings e ON e.content_hash = c.content_hash
                        WHERE c.dataset_version_id = :vid AND c.is_duplicate = false
                        LIMIT 3
                        """
                    ),
                    {"vid": str(version_id)},
                )
            ).all()
            assert hits
            registry = ModelRegistry(session)
            candidate = await registry.resolve("tinygpt", "candidate")
            assert candidate is not None
            before = (
                await session.execute(text("SELECT count(*) FROM chunk_embeddings"))
            ).scalar_one()
            from dml.worker.handlers.base import TaskContext

            handler = EmbeddingHandler(provider)
            chunks = list(
                (
                    await session.execute(
                        select(Chunk).where(Chunk.dataset_version_id == version_id)
                    )
                ).scalars()
            )
            result = await handler.execute(
                TaskContext(
                    task_id=uuid4(),
                    job_id=uuid4(),
                    attempt_id=uuid4(),
                    attempt_number=1,
                    task_type="embedding",
                    payload={"dataset_version_id": str(version_id)},
                    worker_id="skip-test",
                    session=session,
                )
            )
            after = (
                await session.execute(text("SELECT count(*) FROM chunk_embeddings"))
            ).scalar_one()
            assert result.data["skipped"] >= 1
            assert int(after) == int(before)
            assert chunks

            # Incremental update: only changed content gets a new vector.
            new_text = f"Unique changed content {uuid4().hex}"
            changed = Chunk(
                id=uuid4(),
                dataset_version_id=version_id,
                shard_id=chunks[0].shard_id,
                document_id="doc-changed",
                ordinal=999,
                text=new_text,
                content_hash=_hash(new_text),
                byte_count=len(new_text.encode()),
                token_count=len(new_text.split()),
                source_metadata={"filename": "tiny-corpus.txt"},
                is_duplicate=False,
            )
            session.add(changed)
            await session.flush()
            before_inc = int(
                (await session.execute(text("SELECT count(*) FROM chunk_embeddings"))).scalar_one()
            )
            inc = await handler.execute(
                TaskContext(
                    task_id=uuid4(),
                    job_id=uuid4(),
                    attempt_id=uuid4(),
                    attempt_number=1,
                    task_type="embedding",
                    payload={"dataset_version_id": str(version_id)},
                    worker_id="inc-test",
                    session=session,
                )
            )
            after_inc = int(
                (await session.execute(text("SELECT count(*) FROM chunk_embeddings"))).scalar_one()
            )
            assert inc.data["embedded"] == 1
            assert inc.data["skipped"] >= 1
            assert after_inc == before_inc + 1
            await session.commit()
    finally:
        scheduler.request_stop()
        for w in workers:
            w.request_stop()
        await asyncio.gather(sched_task, *worker_tasks, return_exceptions=True)
