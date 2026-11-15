"""embeddings, dag, training, registry

Revision ID: 0003_phase37
Revises: 0002_phase2
Create Date: 2026-09-09
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

revision: str = "0003_phase37"
down_revision: Union[str, None] = "0002_phase2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")
    op.execute("ALTER TABLE chunks ADD COLUMN IF NOT EXISTS is_duplicate BOOLEAN NOT NULL DEFAULT FALSE")
    op.execute("ALTER TABLE chunks ADD COLUMN IF NOT EXISTS canonical_chunk_id UUID")
    op.execute(
        """
        CREATE TABLE pipeline_runs (
            id UUID PRIMARY KEY,
            dataset_version_id UUID NOT NULL REFERENCES dataset_versions(id) ON DELETE RESTRICT,
            status VARCHAR(32) NOT NULL DEFAULT 'pending',
            include_training BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            finished_at TIMESTAMPTZ
        )
        """
    )
    op.execute(
        """
        CREATE TABLE pipeline_nodes (
            id UUID PRIMARY KEY,
            pipeline_run_id UUID NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
            name VARCHAR(64) NOT NULL,
            task_type VARCHAR(64) NOT NULL,
            depends_on TEXT[] NOT NULL DEFAULT '{}',
            status VARCHAR(32) NOT NULL DEFAULT 'pending',
            job_id UUID,
            CONSTRAINT uq_pipeline_node UNIQUE (pipeline_run_id, name)
        )
        """
    )
    op.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS pipeline_run_id UUID REFERENCES pipeline_runs(id) ON DELETE SET NULL")
    op.execute(
        """
        CREATE TABLE chunk_embeddings (
            id UUID PRIMARY KEY,
            chunk_id UUID NOT NULL REFERENCES chunks(id) ON DELETE CASCADE,
            dataset_version_id UUID NOT NULL REFERENCES dataset_versions(id) ON DELETE CASCADE,
            content_hash VARCHAR(64) NOT NULL,
            embedding_model VARCHAR(64) NOT NULL,
            embedding_model_version VARCHAR(32) NOT NULL,
            embedding vector(64) NOT NULL,
            CONSTRAINT uq_embedding_hash_model UNIQUE (content_hash, embedding_model, embedding_model_version)
        )
        """
    )
    op.execute("CREATE INDEX ix_embeddings_version ON chunk_embeddings (dataset_version_id)")
    op.execute(
        """
        CREATE TABLE training_runs (
            id UUID PRIMARY KEY,
            dataset_version_id UUID NOT NULL REFERENCES dataset_versions(id) ON DELETE RESTRICT,
            pipeline_run_id UUID,
            status VARCHAR(32) NOT NULL DEFAULT 'running',
            current_step INTEGER NOT NULL DEFAULT 0,
            max_steps INTEGER NOT NULL DEFAULT 40,
            last_loss DOUBLE PRECISION,
            latest_checkpoint_key VARCHAR(1024),
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        """
        CREATE TABLE training_checkpoints (
            id UUID PRIMARY KEY,
            training_run_id UUID NOT NULL REFERENCES training_runs(id) ON DELETE CASCADE,
            step INTEGER NOT NULL,
            object_key VARCHAR(1024) NOT NULL,
            loss DOUBLE PRECISION,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        """
        CREATE TABLE evaluations (
            id UUID PRIMARY KEY,
            training_run_id UUID NOT NULL REFERENCES training_runs(id) ON DELETE CASCADE,
            checkpoint_key VARCHAR(1024) NOT NULL,
            validation_loss DOUBLE PRECISION NOT NULL,
            perplexity DOUBLE PRECISION NOT NULL,
            smoke_ok BOOLEAN NOT NULL DEFAULT FALSE,
            artifact_ok BOOLEAN NOT NULL DEFAULT FALSE,
            quality_passed BOOLEAN NOT NULL DEFAULT FALSE,
            sample_text TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        """
        CREATE TABLE registered_models (
            name VARCHAR(128) PRIMARY KEY,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        """
        CREATE TABLE model_versions (
            id UUID PRIMARY KEY,
            model_name VARCHAR(128) NOT NULL REFERENCES registered_models(name) ON DELETE CASCADE,
            version INTEGER NOT NULL,
            artifact_key VARCHAR(1024) NOT NULL,
            evaluation_id UUID,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT uq_model_version UNIQUE (model_name, version)
        )
        """
    )
    op.execute(
        """
        CREATE TABLE model_aliases (
            model_name VARCHAR(128) NOT NULL,
            alias VARCHAR(32) NOT NULL,
            version INTEGER NOT NULL,
            PRIMARY KEY (model_name, alias)
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS model_aliases")
    op.execute("DROP TABLE IF EXISTS model_versions")
    op.execute("DROP TABLE IF EXISTS registered_models")
    op.execute("DROP TABLE IF EXISTS evaluations")
    op.execute("DROP TABLE IF EXISTS training_checkpoints")
    op.execute("DROP TABLE IF EXISTS training_runs")
    op.execute("DROP TABLE IF EXISTS chunk_embeddings")
    op.execute("ALTER TABLE jobs DROP COLUMN IF EXISTS pipeline_run_id")
    op.execute("DROP TABLE IF EXISTS pipeline_nodes")
    op.execute("DROP TABLE IF EXISTS pipeline_runs")
    op.execute("ALTER TABLE chunks DROP COLUMN IF EXISTS canonical_chunk_id")
    op.execute("ALTER TABLE chunks DROP COLUMN IF EXISTS is_duplicate")
