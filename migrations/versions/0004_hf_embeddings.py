"""embeddings use HuggingFace MiniLM (384-d)

Revision ID: 0004_hf_vec
Revises: 0003_phase37
Create Date: 2026-09-10
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

revision: str = "0004_hf_vec"
down_revision: Union[str, None] = "0003_phase37"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_chunk_embeddings_vec")
    op.execute("DROP TABLE IF EXISTS chunk_embeddings")
    op.execute(
        """
        CREATE TABLE chunk_embeddings (
            id UUID PRIMARY KEY,
            chunk_id UUID NOT NULL REFERENCES chunks(id) ON DELETE CASCADE,
            dataset_version_id UUID NOT NULL REFERENCES dataset_versions(id) ON DELETE CASCADE,
            content_hash VARCHAR(64) NOT NULL,
            embedding_model VARCHAR(128) NOT NULL,
            embedding_model_version VARCHAR(32) NOT NULL,
            embedding vector(384) NOT NULL,
            CONSTRAINT uq_embedding_hash_model UNIQUE (content_hash, embedding_model, embedding_model_version)
        )
        """
    )
    op.execute("CREATE INDEX ix_embeddings_version ON chunk_embeddings (dataset_version_id)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS chunk_embeddings")
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
