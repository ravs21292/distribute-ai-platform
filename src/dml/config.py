"""Environment-driven platform settings."""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="DML_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    env: str = "dev"
    log_level: str = "INFO"
    log_json: bool = True

    database_url: str = "postgresql+asyncpg://dml:dml@localhost:5432/dml"
    database_url_sync: str = "postgresql+psycopg://dml:dml@localhost:5432/dml"
    redis_url: str = "redis://localhost:6379/0"

    s3_endpoint_url: str = "http://localhost:9000"
    s3_access_key: str = "dmlminio"
    s3_secret_key: str = "dmlminio"
    s3_bucket: str = "dml-artifacts"
    s3_region: str = "us-east-1"

    api_host: str = "0.0.0.0"
    api_port: int = 8000

    heartbeat_interval_seconds: float = 5.0
    worker_stale_after_seconds: float = 15.0
    task_lease_seconds: float = 30.0
    claim_min_idle_ms: int = 15_000
    scheduler_poll_seconds: float = 0.5
    shutdown_grace_seconds: float = 20.0
    max_attempts: int = 5
    claim_block_ms: int = 1000
    worker_metrics_port: int = 9100
    scheduler_metrics_port: int = 9105

    worker_id: str | None = None
    worker_capabilities: str = "ingest,etl"
    worker_device: str = "cpu"

    retry_base_seconds: float = 1.0
    retry_cap_seconds: float = 16.0
    retry_jitter: float = 0.2

    consumer_group: str = "dml-workers"
    stream_prefix: str = "dml.tasks"
    publish_batch_size: int = 50

    object_store: str = "s3"
    local_object_root: str = ".data/objects"
    ingest_lines_per_shard: int = 500
    etl_chunk_chars: int = 1000
    etl_chunk_overlap: int = 100
    etl_transform_version: str = "etl-v1"
    embedding_backend: str = "huggingface"
    embedding_model: str = "sentence-transformers/all-MiniLM-L6-v2"
    embedding_model_version: str = "v1"
    embedding_dim: int = 384
    embedding_batch_size: int = 32
    # Enterprise ingest: mark version ready when enough shards processed even if a
    # tiny fraction of ETL tasks dead-lettered (corrupt source lines).
    dataset_ready_min_shard_ratio: float = 0.999
    train_max_steps: int = 40
    train_checkpoint_every: int = 10
    train_batch_size: int = 16
    train_seq_len: int = 12
    train_lr: float = 0.05
    quality_max_perplexity: float = 400.0
    api_key: str | None = None
    # JSON map: {"secret-admin":{"subject":"alice","role":"admin"},"secret-reader":{"subject":"bob","role":"reader"}}
    api_keys: str | None = None
    auth_required: bool = False
    pii_redaction_enabled: bool = True
    version: str = Field(default="0.1.0")


@lru_cache
def get_settings() -> Settings:
    return Settings()


def parse_capabilities(raw: str) -> list[str]:
    return [part.strip() for part in raw.split(",") if part.strip()]
