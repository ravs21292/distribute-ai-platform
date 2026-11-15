from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


class JobCreate(BaseModel):
    job_type: str = "dummy"
    name: str | None = None
    idempotency_key: str | None = None
    task_count: int = Field(default=1, ge=1, le=10_000)
    priority: int = 0
    max_attempts: int | None = None
    sleep_ms: int = Field(default=50, ge=0, le=60_000)
    seed_prefix: str = "seed"
    required_cpu: int = Field(default=1, ge=0)
    required_memory_mb: int = Field(default=256, ge=0)
    required_gpu: int = Field(default=0, ge=0)
    required_gpu_memory_mb: int = Field(default=0, ge=0)
    gpu_preferred: bool = False


class JobOut(BaseModel):
    id: UUID
    job_type: str
    name: str | None
    status: str
    payload: dict[str, Any]
    priority: int
    task_count: int
    idempotency_key: str | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None

    model_config = {"from_attributes": True}


class TaskOut(BaseModel):
    id: UUID
    job_id: UUID
    task_type: str
    status: str
    payload: dict[str, Any]
    priority: int
    max_attempts: int
    current_attempt: int
    required_cpu: int
    required_memory_mb: int
    required_gpu: int
    required_gpu_memory_mb: int
    gpu_preferred: bool
    result: dict[str, Any] | None
    last_error: str | None
    last_error_type: str | None
    first_failed_at: datetime | None
    last_failed_at: datetime | None
    leased_by_worker_id: str | None
    stream_name: str | None
    created_at: datetime
    scheduled_at: datetime | None
    started_at: datetime | None
    finished_at: datetime | None

    model_config = {"from_attributes": True}


class AttemptOut(BaseModel):
    id: UUID
    task_id: UUID
    attempt_number: int
    worker_id: str | None
    started_at: datetime
    finished_at: datetime | None
    status: str
    error_type: str | None
    error_message: str | None
    retryable: bool | None
    duration_ms: int | None

    model_config = {"from_attributes": True}


class WorkerOut(BaseModel):
    worker_id: str
    hostname: str
    process_id: int
    capabilities: list[str]
    status: str
    healthy: bool = False
    role: str = "pipeline"
    cpu_count: int
    memory_mb: int
    gpu_count: int
    gpu_model: str | None
    gpu_memory_mb: int
    device: str
    last_heartbeat: datetime | None = None
    heartbeat_age_s: float | None = None
    current_task_id: UUID | None = None
    current_task_type: str | None = None
    current_job_id: UUID | None = None
    current_attempt: int | None = None

    model_config = {"from_attributes": True}
