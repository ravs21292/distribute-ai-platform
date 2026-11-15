"""governance, PII metadata, source fingerprints, audit log

Revision ID: 0005_gov
Revises: 0004_hf_vec
Create Date: 2026-09-10
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

revision: str = "0005_gov"
down_revision: Union[str, None] = "0004_hf_vec"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE datasets
            ADD COLUMN IF NOT EXISTS classification VARCHAR(32) NOT NULL DEFAULT 'internal',
            ADD COLUMN IF NOT EXISTS pii_policy VARCHAR(32) NOT NULL DEFAULT 'redact'
        """
    )
    op.execute(
        """
        ALTER TABLE dataset_versions
            ADD COLUMN IF NOT EXISTS source_fingerprint VARCHAR(64),
            ADD COLUMN IF NOT EXISTS pii_chunks INT NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS reused_from_version_id UUID
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_dataset_versions_fingerprint "
        "ON dataset_versions (dataset_id, source_fingerprint)"
    )
    op.execute(
        """
        ALTER TABLE chunks
            ADD COLUMN IF NOT EXISTS pii_categories TEXT[] NOT NULL DEFAULT '{}',
            ADD COLUMN IF NOT EXISTS pii_redacted BOOLEAN NOT NULL DEFAULT false
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS audit_events (
            id UUID PRIMARY KEY,
            actor VARCHAR(256) NOT NULL,
            role VARCHAR(32) NOT NULL,
            action VARCHAR(64) NOT NULL,
            resource_type VARCHAR(64) NOT NULL,
            resource_id VARCHAR(128),
            detail JSONB NOT NULL DEFAULT '{}',
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_audit_events_created ON audit_events (created_at DESC)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_audit_events_action ON audit_events (action, resource_type)"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS audit_events")
    op.execute("ALTER TABLE chunks DROP COLUMN IF EXISTS pii_categories")
    op.execute("ALTER TABLE chunks DROP COLUMN IF EXISTS pii_redacted")
    op.execute("DROP INDEX IF EXISTS ix_dataset_versions_fingerprint")
    op.execute("ALTER TABLE dataset_versions DROP COLUMN IF EXISTS source_fingerprint")
    op.execute("ALTER TABLE dataset_versions DROP COLUMN IF EXISTS pii_chunks")
    op.execute("ALTER TABLE dataset_versions DROP COLUMN IF EXISTS reused_from_version_id")
    op.execute("ALTER TABLE datasets DROP COLUMN IF EXISTS classification")
    op.execute("ALTER TABLE datasets DROP COLUMN IF EXISTS pii_policy")
