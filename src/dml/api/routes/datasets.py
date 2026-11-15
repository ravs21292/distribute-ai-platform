from __future__ import annotations

import hashlib
from pathlib import Path
from uuid import UUID, uuid4

from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile
from pydantic import BaseModel, Field
from sqlalchemy.exc import IntegrityError

from dml.api.deps import SessionDep, SettingsDep
from dml.db.datasets import DatasetRepository
from dml.db.models import DatasetSource
from dml.security.governance import (
    can_access_classification,
    source_fingerprint,
    write_audit,
)
from dml.security.rbac import (
    principal_from_request,
    require_classification_access,
    require_permission,
)
from dml.storage import build_object_store

router = APIRouter(prefix="/v1/datasets", tags=["datasets"])

SUPPORTED = {".txt": "txt", ".md": "md", ".jsonl": "jsonl"}
_SENSITIVE = {"confidential", "restricted"}


class DatasetCreate(BaseModel):
    name: str = Field(min_length=1, max_length=256)
    description: str | None = None
    classification: str = Field(default="internal", pattern="^(public|internal|confidential|restricted)$")
    pii_policy: str = Field(default="redact", pattern="^(redact|reject|allow)$")


class DatasetGovernancePatch(BaseModel):
    classification: str | None = Field(
        default=None, pattern="^(public|internal|confidential|restricted)$"
    )
    pii_policy: str | None = Field(default=None, pattern="^(redact|reject|allow)$")


class DatasetOut(BaseModel):
    id: UUID
    name: str
    description: str | None
    classification: str = "internal"
    pii_policy: str = "redact"
    model_config = {"from_attributes": True}


class VersionOut(BaseModel):
    id: UUID
    dataset_id: UUID
    version_number: int
    version_label: str
    status: str
    transform_version: str
    chunking_config: dict
    ingest_job_id: UUID | None
    etl_job_id: UUID | None
    source_bytes: int
    document_count: int
    chunk_count: int
    shard_count: int
    source_fingerprint: str | None = None
    pii_chunks: int = 0
    reused_from_version_id: UUID | None = None
    model_config = {"from_attributes": True}


class ShardOut(BaseModel):
    id: UUID
    shard_key: str
    status: str
    raw_object_key: str | None
    processed_object_key: str | None
    line_count: int
    document_count: int
    chunk_count: int
    byte_count: int
    model_config = {"from_attributes": True}


class SourceOut(BaseModel):
    id: UUID
    filename: str
    content_type: str
    checksum_sha256: str
    byte_count: int
    object_key: str
    model_config = {"from_attributes": True}


def _content_type(filename: str) -> str:
    suffix = Path(filename).suffix.lower()
    if suffix not in SUPPORTED:
        raise HTTPException(status_code=400, detail=f"unsupported format {suffix}; use txt, md, jsonl")
    return SUPPORTED[suffix]


class _Hasher:
    def __init__(self, inner) -> None:
        self.inner = inner
        self.digest = hashlib.sha256()
        self.n = 0

    def read(self, size: int = -1) -> bytes:
        data = self.inner.read(size)
        if data:
            self.digest.update(data)
            self.n += len(data)
        return data


@router.post("", response_model=DatasetOut)
async def create_dataset(body: DatasetCreate, request: Request, session: SessionDep) -> DatasetOut:
    principal = require_permission(principal_from_request(request), "datasets:write")
    if body.classification in _SENSITIVE or body.pii_policy == "allow":
        require_permission(principal, "governance:write")
    repo = DatasetRepository(session)
    if await repo.get_by_name(body.name):
        raise HTTPException(status_code=409, detail="dataset name already exists")
    try:
        dataset = await repo.create(
            name=body.name,
            description=body.description,
            classification=body.classification,
            pii_policy=body.pii_policy,
        )
    except IntegrityError as exc:
        raise HTTPException(status_code=409, detail="dataset name already exists") from exc
    await write_audit(
        session,
        principal=principal,
        action="dataset.create",
        resource_type="dataset",
        resource_id=str(dataset.id),
        detail={
            "name": dataset.name,
            "classification": dataset.classification,
            "pii_policy": dataset.pii_policy,
        },
    )
    return DatasetOut.model_validate(dataset)


@router.patch("/{dataset_id}/governance", response_model=DatasetOut)
async def patch_dataset_governance(
    dataset_id: UUID, body: DatasetGovernancePatch, request: Request, session: SessionDep
) -> DatasetOut:
    principal = require_permission(principal_from_request(request), "governance:write")
    if body.classification is None and body.pii_policy is None:
        raise HTTPException(status_code=400, detail="classification or pii_policy required")
    repo = DatasetRepository(session)
    dataset = await repo.get(dataset_id)
    if dataset is None:
        raise HTTPException(status_code=404, detail="dataset not found")
    before = {"classification": dataset.classification, "pii_policy": dataset.pii_policy}
    dataset = await repo.update_governance(
        dataset, classification=body.classification, pii_policy=body.pii_policy
    )
    await write_audit(
        session,
        principal=principal,
        action="dataset.governance.update",
        resource_type="dataset",
        resource_id=str(dataset.id),
        detail={"before": before, "after": {
            "classification": dataset.classification,
            "pii_policy": dataset.pii_policy,
        }},
    )
    return DatasetOut.model_validate(dataset)


@router.get("", response_model=list[DatasetOut])
async def list_datasets(request: Request, session: SessionDep) -> list[DatasetOut]:
    principal = require_permission(principal_from_request(request), "datasets:read")
    rows = await DatasetRepository(session).list_datasets()
    return [
        DatasetOut.model_validate(d)
        for d in rows
        if can_access_classification(principal, d.classification)
    ]


@router.get("/{dataset_id}", response_model=DatasetOut)
async def get_dataset(dataset_id: UUID, request: Request, session: SessionDep) -> DatasetOut:
    principal = require_permission(principal_from_request(request), "datasets:read")
    dataset = await DatasetRepository(session).get(dataset_id)
    if dataset is None:
        raise HTTPException(status_code=404, detail="dataset not found")
    require_classification_access(principal, dataset.classification)
    return DatasetOut.model_validate(dataset)


@router.post("/{dataset_id}/versions", response_model=VersionOut)
async def create_version(
    dataset_id: UUID,
    session: SessionDep,
    settings: SettingsDep,
    request: Request,
    files: list[UploadFile] | None = File(default=None),
    lines_per_shard: int = Form(default=500),
    chunk_chars: int = Form(default=1000),
    chunk_overlap: int = Form(default=100),
    commit: bool = Form(default=True),
) -> VersionOut:
    require_permission(principal_from_request(request), "datasets:write")
    repo = DatasetRepository(session)
    dataset = await repo.get(dataset_id)
    if dataset is None:
        raise HTTPException(status_code=404, detail="dataset not found")
    require_classification_access(principal_from_request(request), dataset.classification)
    uploads = files or []
    if commit and not uploads:
        raise HTTPException(status_code=400, detail="at least one source file is required")

    store = getattr(request.app.state, "object_store", None) or build_object_store(settings)
    store.ensure_ready()
    version_number = await repo.next_version_number(dataset.id)
    version_label = f"v{version_number}"
    sources: list[DatasetSource] = []
    for upload in uploads:
        filename = upload.filename or "upload.txt"
        content_type = _content_type(filename)
        object_key = f"raw/{dataset.id}/{version_label}/sources/{filename}"
        hasher = _Hasher(upload.file)
        store.put_fileobj(object_key, hasher)
        sources.append(
            DatasetSource(
                id=uuid4(),
                dataset_version_id=uuid4(),
                filename=filename,
                content_type=content_type,
                checksum_sha256=hasher.digest.hexdigest(),
                byte_count=hasher.n,
                object_key=object_key,
            )
        )

    if commit and sources:
        fingerprint = source_fingerprint([s.checksum_sha256 for s in sources])
        existing = await repo.find_ready_by_fingerprint(dataset.id, fingerprint)
        if existing is not None:
            await write_audit(
                session,
                principal=principal_from_request(request),
                action="dataset.version.reuse",
                resource_type="dataset_version",
                resource_id=str(existing.id),
                detail={"fingerprint": fingerprint, "reason": "identical_sources"},
            )
            return VersionOut.model_validate(existing)

    version = await repo.create_version(
        dataset=dataset,
        sources=sources,
        chunking_config={
            "lines_per_shard": lines_per_shard,
            "chunk_chars": chunk_chars,
            "chunk_overlap": chunk_overlap,
            "pii_policy": dataset.pii_policy,
        },
        transform_version=settings.etl_transform_version,
        max_attempts=settings.max_attempts,
        lines_per_shard=lines_per_shard,
        version_number=version_number,
        commit=commit and bool(sources),
    )
    await write_audit(
        session,
        principal=principal_from_request(request),
        action="dataset.version.create",
        resource_type="dataset_version",
        resource_id=str(version.id),
        detail={"commit": commit, "files": [s.filename for s in sources]},
    )
    return VersionOut.model_validate(version)


@router.post("/{dataset_id}/versions/{version_id}/sources", response_model=VersionOut)
async def append_sources(
    dataset_id: UUID,
    version_id: UUID,
    session: SessionDep,
    settings: SettingsDep,
    request: Request,
    files: list[UploadFile] = File(...),
) -> VersionOut:
    require_permission(principal_from_request(request), "datasets:write")
    repo = DatasetRepository(session)
    dataset = await repo.get(dataset_id)
    version = await repo.get_version(version_id)
    if dataset is None or version is None or version.dataset_id != dataset_id:
        raise HTTPException(status_code=404, detail="version not found")
    require_classification_access(principal_from_request(request), dataset.classification)
    if version.status != "draft":
        raise HTTPException(status_code=409, detail="version is not a draft")
    if not files:
        raise HTTPException(status_code=400, detail="at least one file required")
    store = getattr(request.app.state, "object_store", None) or build_object_store(settings)
    store.ensure_ready()
    sources: list[DatasetSource] = []
    for upload in files:
        filename = upload.filename or "upload.txt"
        content_type = _content_type(filename)
        object_key = f"raw/{dataset.id}/{version.version_label}/sources/{filename}"
        hasher = _Hasher(upload.file)
        store.put_fileobj(object_key, hasher)
        sources.append(
            DatasetSource(
                id=uuid4(),
                dataset_version_id=version.id,
                filename=filename,
                content_type=content_type,
                checksum_sha256=hasher.digest.hexdigest(),
                byte_count=hasher.n,
                object_key=object_key,
            )
        )
    try:
        version = await repo.add_sources(version, sources)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    await write_audit(
        session,
        principal=principal_from_request(request),
        action="dataset.version.append",
        resource_type="dataset_version",
        resource_id=str(version.id),
        detail={"files": [s.filename for s in sources], "source_bytes": version.source_bytes},
    )
    return VersionOut.model_validate(version)


@router.post("/{dataset_id}/versions/{version_id}/commit", response_model=VersionOut)
async def commit_version(
    dataset_id: UUID,
    version_id: UUID,
    session: SessionDep,
    settings: SettingsDep,
    request: Request,
) -> VersionOut:
    require_permission(principal_from_request(request), "datasets:write")
    repo = DatasetRepository(session)
    dataset = await repo.get(dataset_id)
    version = await repo.get_version(version_id)
    if dataset is None or version is None or version.dataset_id != dataset_id:
        raise HTTPException(status_code=404, detail="version not found")
    require_classification_access(principal_from_request(request), dataset.classification)
    lines_per_shard = int((version.chunking_config or {}).get("lines_per_shard") or 500)
    try:
        version = await repo.start_ingest(
            dataset=dataset,
            version=version,
            max_attempts=settings.max_attempts,
            lines_per_shard=lines_per_shard,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    await write_audit(
        session,
        principal=principal_from_request(request),
        action="dataset.version.commit",
        resource_type="dataset_version",
        resource_id=str(version.id),
        detail={"status": version.status, "source_bytes": version.source_bytes},
    )
    return VersionOut.model_validate(version)


@router.post("/{dataset_id}/versions/{version_id}/recover", response_model=VersionOut)
async def recover_version(
    dataset_id: UUID,
    version_id: UUID,
    session: SessionDep,
    settings: SettingsDep,
    request: Request,
) -> VersionOut:
    """Enterprise recovery: re-ETL unprocessed shards, or mark ready if coverage is enough."""
    principal = require_permission(principal_from_request(request), "datasets:write")
    repo = DatasetRepository(session)
    dataset = await repo.get(dataset_id)
    version = await repo.get_version(version_id)
    if dataset is None or version is None or version.dataset_id != dataset_id:
        raise HTTPException(status_code=404, detail="version not found")
    require_classification_access(principal, dataset.classification)

    shards = await repo.list_shards(version_id)
    pending = [s for s in shards if s.status != "processed" and s.raw_object_key]
    if pending:
        job = await repo.create_recovery_etl_job(version, max_attempts=settings.max_attempts)
        await write_audit(
            session,
            principal=principal,
            action="dataset.version.recover",
            resource_type="dataset_version",
            resource_id=str(version.id),
            detail={"pending_shards": len(pending), "etl_job_id": str(job.id) if job else None},
        )
        return VersionOut.model_validate(version)

    processed = sum(1 for s in shards if s.status == "processed")
    if processed == 0:
        raise HTTPException(status_code=409, detail="no processed shards to recover")
    await repo.mark_ready(version)
    await write_audit(
        session,
        principal=principal,
        action="dataset.version.recover_ready",
        resource_type="dataset_version",
        resource_id=str(version.id),
        detail={"processed_shards": processed, "total_shards": len(shards), "chunks": version.chunk_count},
    )
    return VersionOut.model_validate(version)


@router.get("/{dataset_id}/versions", response_model=list[VersionOut])
async def list_versions(dataset_id: UUID, request: Request, session: SessionDep) -> list[VersionOut]:
    principal = require_permission(principal_from_request(request), "datasets:read")
    repo = DatasetRepository(session)
    dataset = await repo.get(dataset_id)
    if dataset is None:
        raise HTTPException(status_code=404, detail="dataset not found")
    require_classification_access(principal, dataset.classification)
    return [VersionOut.model_validate(v) for v in await repo.list_versions(dataset_id)]


@router.get("/{dataset_id}/versions/{version_id}", response_model=VersionOut)
async def get_version(
    dataset_id: UUID, version_id: UUID, request: Request, session: SessionDep
) -> VersionOut:
    principal = require_permission(principal_from_request(request), "datasets:read")
    repo = DatasetRepository(session)
    dataset = await repo.get(dataset_id)
    version = await repo.get_version(version_id)
    if dataset is None or version is None or version.dataset_id != dataset_id:
        raise HTTPException(status_code=404, detail="version not found")
    require_classification_access(principal, dataset.classification)
    return VersionOut.model_validate(version)


@router.get("/{dataset_id}/versions/{version_id}/sources", response_model=list[SourceOut])
async def list_sources(
    dataset_id: UUID, version_id: UUID, request: Request, session: SessionDep
) -> list[SourceOut]:
    principal = require_permission(principal_from_request(request), "datasets:read")
    repo = DatasetRepository(session)
    dataset = await repo.get(dataset_id)
    version = await repo.get_version(version_id)
    if dataset is None or version is None or version.dataset_id != dataset_id:
        raise HTTPException(status_code=404, detail="version not found")
    require_classification_access(principal, dataset.classification)
    return [SourceOut.model_validate(s) for s in await repo.list_sources(version_id)]


@router.get("/{dataset_id}/versions/{version_id}/shards", response_model=list[ShardOut])
async def list_shards(
    dataset_id: UUID, version_id: UUID, request: Request, session: SessionDep
) -> list[ShardOut]:
    principal = require_permission(principal_from_request(request), "datasets:read")
    repo = DatasetRepository(session)
    dataset = await repo.get(dataset_id)
    version = await repo.get_version(version_id)
    if dataset is None or version is None or version.dataset_id != dataset_id:
        raise HTTPException(status_code=404, detail="version not found")
    require_classification_access(principal, dataset.classification)
    return [ShardOut.model_validate(s) for s in await repo.list_shards(version_id)]
