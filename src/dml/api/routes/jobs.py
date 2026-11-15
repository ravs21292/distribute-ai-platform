from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, HTTPException, Request

from dml.api.deps import MetricsDep, SessionDep, SettingsDep
from dml.api.schemas import JobCreate, JobOut, TaskOut
from dml.db.repositories import JobRepository, TaskRepository
from dml.security.rbac import principal_from_request, require_permission

router = APIRouter(prefix="/v1/jobs", tags=["jobs"])


@router.post("", response_model=JobOut)
async def create_job(
    body: JobCreate,
    request: Request,
    session: SessionDep,
    settings: SettingsDep,
    metrics: MetricsDep,
) -> JobOut:
    require_permission(principal_from_request(request), "jobs:write")
    if body.job_type != "dummy":
        raise HTTPException(status_code=400, detail="Phase 1 only supports job_type=dummy")
    repo = JobRepository(session)
    job = await repo.create_dummy_job(
        task_count=body.task_count,
        payload={"seed_prefix": body.seed_prefix},
        name=body.name,
        idempotency_key=body.idempotency_key,
        priority=body.priority,
        max_attempts=body.max_attempts or settings.max_attempts,
        required_cpu=body.required_cpu,
        required_memory_mb=body.required_memory_mb,
        required_gpu=body.required_gpu,
        required_gpu_memory_mb=body.required_gpu_memory_mb,
        gpu_preferred=body.gpu_preferred,
        sleep_ms=body.sleep_ms,
    )
    metrics.tasks_created_total.inc(body.task_count)
    return JobOut.model_validate(job)


@router.get("", response_model=list[JobOut])
async def list_jobs(request: Request, session: SessionDep) -> list[JobOut]:
    require_permission(principal_from_request(request), "jobs:read")
    jobs = await JobRepository(session).list_jobs()
    return [JobOut.model_validate(j) for j in jobs]


@router.get("/{job_id}", response_model=JobOut)
async def get_job(job_id: UUID, request: Request, session: SessionDep) -> JobOut:
    require_permission(principal_from_request(request), "jobs:read")
    job = await JobRepository(session).get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return JobOut.model_validate(job)


@router.get("/{job_id}/tasks", response_model=list[TaskOut])
async def list_job_tasks(job_id: UUID, request: Request, session: SessionDep) -> list[TaskOut]:
    require_permission(principal_from_request(request), "jobs:read")
    job = await JobRepository(session).get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    tasks = await TaskRepository(session).list_for_job(job_id)
    return [TaskOut.model_validate(t) for t in tasks]


@router.post("/{job_id}/cancel", response_model=JobOut)
async def cancel_job(job_id: UUID, request: Request, session: SessionDep) -> JobOut:
    require_permission(principal_from_request(request), "jobs:write")
    job = await JobRepository(session).cancel_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return JobOut.model_validate(job)
