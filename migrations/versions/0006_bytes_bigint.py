"""widen byte counters to bigint for multi-GB corpora

Revision ID: 0006_bytes64
Revises: 0005_gov
Create Date: 2026-09-10
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

revision: str = "0006_bytes64"
down_revision: Union[str, None] = "0005_gov"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE dataset_versions ALTER COLUMN source_bytes TYPE BIGINT")
    op.execute("ALTER TABLE dataset_sources ALTER COLUMN byte_count TYPE BIGINT")
    op.execute("ALTER TABLE shards ALTER COLUMN byte_count TYPE BIGINT")
    op.execute("ALTER TABLE chunks ALTER COLUMN byte_count TYPE BIGINT")


def downgrade() -> None:
    op.execute("ALTER TABLE chunks ALTER COLUMN byte_count TYPE INTEGER")
    op.execute("ALTER TABLE shards ALTER COLUMN byte_count TYPE INTEGER")
    op.execute("ALTER TABLE dataset_sources ALTER COLUMN byte_count TYPE INTEGER")
    op.execute("ALTER TABLE dataset_versions ALTER COLUMN source_bytes TYPE INTEGER")
