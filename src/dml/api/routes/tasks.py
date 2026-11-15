from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, HTTPException, Request

from dml.api.deps import SessionDep
from dml.api.schemas import AttemptOut, TaskOut
from dml.db.repositories import TaskRepository
from dml.security.rbac import principal_from_request, require_permission

router = APIRouter(prefix="/v1/tasks", tags=["tasks"])


@router.get("/{task_id}", response_model=TaskOut)
async def get_task(task_id: UUID, request: Request, session: SessionDep) -> TaskOut:
    require_permission(principal_from_request(request), "jobs:read")
    task = await TaskRepository(session).get(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="task not found")
    return TaskOut.model_validate(task)


@router.get("/{task_id}/attempts", response_model=list[AttemptOut])
async def list_attempts(task_id: UUID, request: Request, session: SessionDep) -> list[AttemptOut]:
    require_permission(principal_from_request(request), "jobs:read")
    repo = TaskRepository(session)
    task = await repo.get(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="task not found")
    attempts = await repo.list_attempts(task_id)
    return [AttemptOut.model_validate(a) for a in attempts]


@router.post("/{task_id}/retry", response_model=TaskOut)
async def retry_task(task_id: UUID, request: Request, session: SessionDep) -> TaskOut:
    require_permission(principal_from_request(request), "jobs:write")
    repo = TaskRepository(session)
    task = await repo.retry_dead_letter(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="task not found")
    return TaskOut.model_validate(task)
