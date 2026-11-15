"""phase 2 datasets, shards, chunks

Revision ID: 0002_phase2
Revises: 0001_phase1
Create Date: 2026-09-09
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

revision: str = "0002_phase2"
down_revision: Union[str, None] = "0001_phase1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE datasets (
            id UUID PRIMARY KEY,
            name VARCHAR(256) NOT NULL UNIQUE,
            description TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        """
        CREATE TABLE dataset_versions (
            id UUID PRIMARY KEY,
            dataset_id UUID NOT NULL REFERENCES datasets(id) ON DELETE RESTRICT,
            version_number INTEGER NOT NULL,
            version_label VARCHAR(32) NOT NULL,
            status VARCHAR(32) NOT NULL DEFAULT 'ingesting',
            transform_version VARCHAR(64) NOT NULL DEFAULT 'etl-v1',
            chunking_config JSONB NOT NULL DEFAULT '{}'::jsonb,
            ingest_job_id UUID,
            etl_job_id UUID,
            source_bytes INTEGER NOT NULL DEFAULT 0,
            document_count INTEGER NOT NULL DEFAULT 0,
            chunk_count INTEGER NOT NULL DEFAULT 0,
            shard_count INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT uq_dataset_version UNIQUE (dataset_id, version_number)
        )
        """
    )
    op.execute(
        """
        CREATE TABLE dataset_sources (
            id UUID PRIMARY KEY,
            dataset_version_id UUID NOT NULL REFERENCES dataset_versions(id) ON DELETE CASCADE,
            filename VARCHAR(512) NOT NULL,
            content_type VARCHAR(16) NOT NULL,
            checksum_sha256 VARCHAR(64) NOT NULL,
            byte_count INTEGER NOT NULL DEFAULT 0,
            object_key VARCHAR(1024) NOT NULL
        )
        """
    )
    op.execute(
        """
        CREATE TABLE shards (
            id UUID PRIMARY KEY,
            dataset_version_id UUID NOT NULL REFERENCES dataset_versions(id) ON DELETE CASCADE,
            source_id UUID REFERENCES dataset_sources(id) ON DELETE SET NULL,
            shard_key VARCHAR(256) NOT NULL,
            status VARCHAR(32) NOT NULL DEFAULT 'pending',
            raw_object_key VARCHAR(1024),
            processed_object_key VARCHAR(1024),
            line_count INTEGER NOT NULL DEFAULT 0,
            document_count INTEGER NOT NULL DEFAULT 0,
            chunk_count INTEGER NOT NULL DEFAULT 0,
            byte_count INTEGER NOT NULL DEFAULT 0,
            CONSTRAINT uq_shard_key UNIQUE (dataset_version_id, shard_key)
        )
        """
    )
    op.execute(
        """
        CREATE TABLE chunks (
            id UUID PRIMARY KEY,
            dataset_version_id UUID NOT NULL REFERENCES dataset_versions(id) ON DELETE CASCADE,
            shard_id UUID NOT NULL REFERENCES shards(id) ON DELETE CASCADE,
            document_id VARCHAR(256) NOT NULL,
            ordinal INTEGER NOT NULL,
            text TEXT NOT NULL,
            content_hash VARCHAR(64) NOT NULL,
            byte_count INTEGER NOT NULL DEFAULT 0,
            token_count INTEGER NOT NULL DEFAULT 0,
            source_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
            CONSTRAINT uq_chunk_ordinal UNIQUE (shard_id, ordinal)
        )
        """
    )
    op.execute("CREATE INDEX ix_chunks_content_hash ON chunks (dataset_version_id, content_hash)")
    op.execute("CREATE INDEX ix_chunks_version ON chunks (dataset_version_id)")
    op.execute("ALTER TABLE jobs ADD COLUMN dataset_version_id UUID REFERENCES dataset_versions(id) ON DELETE SET NULL")
    op.execute("CREATE INDEX ix_jobs_dataset_version ON jobs (dataset_version_id)")


def downgrade() -> None:
    op.execute("ALTER TABLE jobs DROP COLUMN IF EXISTS dataset_version_id")
    op.execute("DROP TABLE IF EXISTS chunks")
    op.execute("DROP TABLE IF EXISTS shards")
    op.execute("DROP TABLE IF EXISTS dataset_sources")
    op.execute("DROP TABLE IF EXISTS dataset_versions")
    op.execute("DROP TABLE IF EXISTS datasets")
