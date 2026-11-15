from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from dml.api.deps import SessionDep, SettingsDep
from dml.db.registry import ModelRegistry
from dml.ml.tinygpt import TinyGPT
from dml.security.rbac import principal_from_request, require_permission
from dml.storage import build_object_store

router = APIRouter(prefix="/v1/models", tags=["models"])


class AliasMove(BaseModel):
    alias: str
    version: int | None = None
    from_alias: str | None = None


class GenerateIn(BaseModel):
    alias: str = "production"
    prompt: str = "The "
    max_tokens: int = 40


class ModelOut(BaseModel):
    name: str
    versions: list[int]
    aliases: dict[str, int]


@router.get("", response_model=list[ModelOut])
async def list_models(request: Request, session: SessionDep) -> list[ModelOut]:
    require_permission(principal_from_request(request), "models:read")
    registry = ModelRegistry(session)
    out = []
    for model in await registry.list_models():
        _, versions, aliases = await registry.get_model(model.name)
        out.append(
            ModelOut(
                name=model.name,
                versions=[v.version for v in versions],
                aliases={a.alias: a.version for a in aliases},
            )
        )
    return out


@router.get("/{name}", response_model=ModelOut)
async def get_model(name: str, request: Request, session: SessionDep) -> ModelOut:
    require_permission(principal_from_request(request), "models:read")
    model, versions, aliases = await ModelRegistry(session).get_model(name)
    if model is None:
        raise HTTPException(status_code=404, detail="model not found")
    return ModelOut(
        name=name,
        versions=[v.version for v in versions],
        aliases={a.alias: a.version for a in aliases},
    )


@router.post("/{name}/promote", response_model=ModelOut)
async def promote(name: str, body: AliasMove, request: Request, session: SessionDep) -> ModelOut:
    require_permission(principal_from_request(request), "models:write")
    registry = ModelRegistry(session)
    model, versions, aliases = await registry.get_model(name)
    if model is None:
        raise HTTPException(status_code=404, detail="model not found")
    version = body.version
    if version is None and body.from_alias:
        current = next((a.version for a in aliases if a.alias == body.from_alias), None)
        version = current
    if version is None:
        raise HTTPException(status_code=400, detail="version or from_alias required")
    if version not in {v.version for v in versions}:
        raise HTTPException(status_code=404, detail="version not found")
    await registry.set_alias(name, body.alias, version)
    return await get_model(name, request, session)


@router.post("/{name}/rollback", response_model=ModelOut)
async def rollback(name: str, body: AliasMove, request: Request, session: SessionDep) -> ModelOut:
    require_permission(principal_from_request(request), "models:write")
    if body.version is None:
        raise HTTPException(status_code=400, detail="version is required for rollback")
    return await promote(name, body, request, session)


@router.post("/{name}/generate")
async def generate(
    name: str, body: GenerateIn, request: Request, session: SessionDep, settings: SettingsDep
) -> dict:
    require_permission(principal_from_request(request), "models:read")
    registry = ModelRegistry(session)
    version = await registry.resolve(name, body.alias)
    if version is None:
        raise HTTPException(status_code=404, detail=f"alias {body.alias} not set")
    store = build_object_store(settings)
    if not store.exists(version.artifact_key):
        raise HTTPException(status_code=404, detail="artifact missing")
    model = TinyGPT.loads(store.get_bytes(version.artifact_key))
    return {
        "model": name,
        "alias": body.alias,
        "version": version.version,
        "text": model.generate(body.prompt, body.max_tokens),
    }
