"""phase 1 distributed task runtime

Revision ID: 0001_phase1
Revises:
Create Date: 2026-09-09
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

revision: str = "0001_phase1"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")
    op.execute(
        """
        CREATE TABLE jobs (
            id UUID PRIMARY KEY,
            idempotency_key VARCHAR(256) UNIQUE,
            job_type VARCHAR(64) NOT NULL,
            name VARCHAR(256),
            status VARCHAR(32) NOT NULL DEFAULT 'pending',
            payload JSONB NOT NULL DEFAULT '{}'::jsonb,
            priority INTEGER NOT NULL DEFAULT 0,
            task_count INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            started_at TIMESTAMPTZ,
            finished_at TIMESTAMPTZ
        )
        """
    )
    op.execute(
        """
        CREATE TABLE workers (
            worker_id VARCHAR(128) PRIMARY KEY,
            hostname VARCHAR(256) NOT NULL,
            process_id INTEGER NOT NULL,
            capabilities TEXT[] NOT NULL DEFAULT '{}'::text[],
            status VARCHAR(32) NOT NULL DEFAULT 'starting',
            cpu_count INTEGER NOT NULL DEFAULT 1,
            memory_mb INTEGER NOT NULL DEFAULT 0,
            gpu_count INTEGER NOT NULL DEFAULT 0,
            gpu_model VARCHAR(256),
            gpu_memory_mb INTEGER NOT NULL DEFAULT 0,
            device VARCHAR(16) NOT NULL DEFAULT 'cpu',
            runtime VARCHAR(64) NOT NULL DEFAULT 'dml-worker/0.1.0',
            started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT now(),
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        """
        CREATE TABLE tasks (
            id UUID PRIMARY KEY,
            job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE RESTRICT,
            task_type VARCHAR(64) NOT NULL,
            payload JSONB NOT NULL DEFAULT '{}'::jsonb,
            status VARCHAR(32) NOT NULL DEFAULT 'pending',
            priority INTEGER NOT NULL DEFAULT 0,
            max_attempts INTEGER NOT NULL DEFAULT 5,
            current_attempt INTEGER NOT NULL DEFAULT 0,
            required_cpu INTEGER NOT NULL DEFAULT 1,
            required_memory_mb INTEGER NOT NULL DEFAULT 256,
            required_gpu INTEGER NOT NULL DEFAULT 0,
            required_gpu_memory_mb INTEGER NOT NULL DEFAULT 0,
            gpu_preferred BOOLEAN NOT NULL DEFAULT FALSE,
            next_run_at TIMESTAMPTZ,
            lease_until TIMESTAMPTZ,
            leased_by_worker_id VARCHAR(128),
            stream_name VARCHAR(256),
            stream_id VARCHAR(64),
            result JSONB,
            last_error TEXT,
            last_error_type VARCHAR(64),
            first_failed_at TIMESTAMPTZ,
            last_failed_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            scheduled_at TIMESTAMPTZ,
            started_at TIMESTAMPTZ,
            finished_at TIMESTAMPTZ
        )
        """
    )
    op.execute("CREATE INDEX ix_tasks_status_next_run ON tasks (status, next_run_at)")
    op.execute("CREATE INDEX ix_tasks_job_id ON tasks (job_id)")
    op.execute("CREATE INDEX ix_tasks_lease ON tasks (status, lease_until)")
    op.execute("CREATE INDEX ix_tasks_leased_by ON tasks (leased_by_worker_id)")
    op.execute(
        """
        CREATE TABLE task_attempts (
            id UUID PRIMARY KEY,
            task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
            attempt_number INTEGER NOT NULL,
            worker_id VARCHAR(128),
            started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            finished_at TIMESTAMPTZ,
            status VARCHAR(32) NOT NULL DEFAULT 'running',
            error_type VARCHAR(64),
            error_message TEXT,
            retryable BOOLEAN,
            duration_ms INTEGER,
            CONSTRAINT uq_attempt_number UNIQUE (task_id, attempt_number)
        )
        """
    )
    op.execute(
        """
        CREATE TABLE dummy_results (
            task_id UUID PRIMARY KEY REFERENCES tasks(id) ON DELETE CASCADE,
            seed TEXT NOT NULL,
            digest VARCHAR(64) NOT NULL,
            executions INTEGER NOT NULL DEFAULT 1,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute("CREATE INDEX ix_workers_heartbeat ON workers (status, last_heartbeat)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS dummy_results")
    op.execute("DROP TABLE IF EXISTS task_attempts")
    op.execute("DROP TABLE IF EXISTS tasks")
    op.execute("DROP TABLE IF EXISTS workers")
    op.execute("DROP TABLE IF EXISTS jobs")
