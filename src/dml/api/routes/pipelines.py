from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from dml.api.deps import SessionDep, SettingsDep
from dml.db.datasets import DatasetRepository
from dml.db.models import PipelineNode, PipelineRun
from dml.db.pipelines import DATA_GRAPH, TRAIN_GRAPH, PipelineRepository
from dml.security.governance import write_audit
from dml.security.rbac import principal_from_request, require_permission

router = APIRouter(prefix="/v1/pipelines", tags=["pipelines"])


class PipelineRunBody(BaseModel):
    dataset_id: UUID | None = None
    dataset_version_id: UUID | None = None
    include_training: bool = True


class NodeOut(BaseModel):
    name: str
    task_type: str
    depends_on: list[str]
    status: str
    job_id: UUID | None
    worker_id: str | None = None
    model_config = {"from_attributes": True}


class PipelineOut(BaseModel):
    id: UUID
    dataset_version_id: UUID
    status: str
    include_training: bool
    nodes: list[NodeOut] = []
    model_config = {"from_attributes": True}


@router.post("/run", response_model=PipelineOut)
async def run_pipeline(
    body: PipelineRunBody, request: Request, session: SessionDep, settings: SettingsDep
) -> PipelineOut:
    principal = require_permission(principal_from_request(request), "pipelines:write")
    datasets = DatasetRepository(session)
    version = None
    if body.dataset_version_id:
        version = await datasets.get_version(body.dataset_version_id)
    elif body.dataset_id:
        versions = await datasets.list_versions(body.dataset_id)
        version = next((v for v in versions if v.status == "ready"), versions[0] if versions else None)
    if version is None:
        raise HTTPException(status_code=404, detail="dataset version not found")
    ready = version.status == "ready"
    repo = PipelineRepository(session)
    run = await repo.create_run(
        version=version, include_training=body.include_training, version_already_ready=ready
    )
    await repo.advance(run, settings_max_attempts=settings.max_attempts)
    await write_audit(
        session,
        principal=principal,
        action="pipeline.run",
        resource_type="pipeline",
        resource_id=str(run.id),
        detail={
            "dataset_version_id": str(version.id),
            "include_training": body.include_training,
        },
    )
    nodes = await repo.list_nodes(run.id)
    return _out(run, nodes)


@router.get("", response_model=list[PipelineOut])
async def list_pipelines(request: Request, session: SessionDep) -> list[PipelineOut]:
    require_permission(principal_from_request(request), "pipelines:read")
    repo = PipelineRepository(session)
    runs = await repo.list_runs()
    out = []
    for run in runs:
        out.append(_out(run, await repo.list_nodes(run.id)))
    return out


@router.get("/{pipeline_id}", response_model=PipelineOut)
async def get_pipeline(pipeline_id: UUID, request: Request, session: SessionDep) -> PipelineOut:
    require_permission(principal_from_request(request), "pipelines:read")
    repo = PipelineRepository(session)
    run = await repo.get(pipeline_id)
    if run is None:
        raise HTTPException(status_code=404, detail="pipeline not found")
    return _out(run, await repo.list_nodes(run.id))


@router.post("/{pipeline_id}/cancel", response_model=PipelineOut)
async def cancel_pipeline(pipeline_id: UUID, request: Request, session: SessionDep) -> PipelineOut:
    principal = require_permission(principal_from_request(request), "pipelines:write")
    repo = PipelineRepository(session)
    run = await repo.get(pipeline_id)
    if run is None:
        raise HTTPException(status_code=404, detail="pipeline not found")
    run = await repo.cancel(run)
    await write_audit(
        session,
        principal=principal,
        action="pipeline.cancel",
        resource_type="pipeline",
        resource_id=str(run.id),
        detail={},
    )
    return _out(run, await repo.list_nodes(run.id))


def _out(run: PipelineRun, nodes: list[PipelineNode]) -> PipelineOut:
    order = [name for name, _deps in (*DATA_GRAPH, *TRAIN_GRAPH)]
    ranked = sorted(nodes, key=lambda n: order.index(n.name) if n.name in order else 99)
    payload = PipelineOut.model_validate(run)
    payload.nodes = [NodeOut.model_validate(n) for n in ranked]
    return payload
