from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, HTTPException, Query, Request

from dml.api.deps import SessionDep, SettingsDep
from dml.api.schemas import WorkerOut
from dml.config import Settings
from dml.db.repositories import TaskRepository, WorkerRepository
from dml.security.rbac import principal_from_request, require_permission

router = APIRouter(prefix="/v1/workers", tags=["workers"])


def _healthy(last_heartbeat: datetime, status: str, stale_after: float) -> bool:
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=stale_after)
    if status in {"stopped", "unhealthy"}:
        return False
    hb = last_heartbeat
    if hb.tzinfo is None:
        hb = hb.replace(tzinfo=timezone.utc)
    return hb >= cutoff


def _role(capabilities: list[str] | None) -> str:
    caps = set(capabilities or [])
    if "embedding" in caps:
        return "embedding"
    if {"training", "evaluation"} & caps:
        return "training"
    if {"ingest", "etl", "dedupe", "index"} & caps:
        return "pipeline"
    return "generic"


def decorate(worker, inflight: dict, settings: Settings) -> WorkerOut:
    payload = WorkerOut.model_validate(worker)
    payload.healthy = _healthy(
        worker.last_heartbeat, worker.status, settings.worker_stale_after_seconds
    )
    payload.role = _role(list(worker.capabilities or []))
    hb = worker.last_heartbeat
    if hb is not None:
        if hb.tzinfo is None:
            hb = hb.replace(tzinfo=timezone.utc)
        payload.last_heartbeat = hb
        payload.heartbeat_age_s = max(0.0, (datetime.now(timezone.utc) - hb).total_seconds())
    current = inflight.get(worker.worker_id)
    if current is not None:
        payload.current_task_id = current.id
        payload.current_task_type = current.task_type
        payload.current_job_id = current.job_id
        payload.current_attempt = current.current_attempt
        if payload.status == "idle":
            payload.status = "busy"
    return payload


@router.get("", response_model=list[WorkerOut])
async def list_workers(
    request: Request,
    session: SessionDep,
    settings: SettingsDep,
    alive_only: bool = Query(default=True, description="Only workers with a fresh heartbeat"),
) -> list[WorkerOut]:
    require_permission(principal_from_request(request), "workers:read")
    inflight = await TaskRepository(session).inflight_by_worker()
    out = []
    for worker in await WorkerRepository(session).list_workers():
        payload = decorate(worker, inflight, settings)
        if alive_only and not payload.healthy:
            continue
        out.append(payload)
    return out


@router.get("/{worker_id}", response_model=WorkerOut)
async def get_worker(
    worker_id: str, request: Request, session: SessionDep, settings: SettingsDep
) -> WorkerOut:
    require_permission(principal_from_request(request), "workers:read")
    worker = await WorkerRepository(session).get(worker_id)
    if worker is None:
        raise HTTPException(status_code=404, detail="worker not found")
    inflight = await TaskRepository(session).inflight_by_worker()
    return decorate(worker, inflight, settings)
