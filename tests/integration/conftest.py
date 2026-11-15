from __future__ import annotations

import os
import uuid

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from dml.config import Settings, get_settings


@pytest.fixture
def settings() -> Settings:
    get_settings.cache_clear()
    prefix = f"dml.test.{uuid.uuid4().hex[:8]}.tasks"
    return Settings(
        stream_prefix=prefix,
        scheduler_poll_seconds=0.1,
        heartbeat_interval_seconds=0.2,
        worker_stale_after_seconds=2.0,
        task_lease_seconds=2.0,
        claim_min_idle_ms=400,
        claim_block_ms=200,
        retry_jitter=0.0,
        object_store="local",
        local_object_root=str(
            __import__("pathlib").Path(__import__("tempfile").gettempdir())
            / "dml-objects"
            / uuid.uuid4().hex[:8]
        ),
        database_url=os.environ.get(
            "DML_DATABASE_URL", "postgresql+asyncpg://dml:dml@localhost:5432/dml"
        ),
        database_url_sync=os.environ.get(
            "DML_DATABASE_URL_SYNC", "postgresql+psycopg://dml:dml@localhost:5432/dml"
        ),
        redis_url=os.environ.get("DML_REDIS_URL", "redis://localhost:6379/0"),
        train_max_steps=8,
        train_checkpoint_every=4,
        quality_max_perplexity=10_000.0,
        embedding_backend="local",
        embedding_model="local-hash",
        embedding_dim=384,
    )


@pytest_asyncio.fixture
async def session_factory(settings: Settings):
    engine = create_async_engine(settings.database_url, pool_pre_ping=True)
    try:
        async with engine.connect() as conn:
            await conn.exec_driver_sql("SELECT 1")
    except Exception as exc:
        await engine.dispose()
        pytest.skip(f"postgres unavailable: {exc}")
    factory = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
    yield factory
    await engine.dispose()
