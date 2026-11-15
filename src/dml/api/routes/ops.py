from __future__ import annotations

import asyncio

from fastapi import APIRouter, Request, WebSocket, WebSocketDisconnect
from sqlalchemy.ext.asyncio import AsyncSession

from dml.api.deps import SessionDep, SettingsDep
from dml.api.ops_service import OverviewOut, build_overview
from dml.config import Settings
from dml.db.session import create_session_factory
from dml.security.rbac import (
    parse_api_keys,
    principal_from_request,
    require_permission,
    resolve_principal,
)

router = APIRouter(prefix="/v1/ops", tags=["ops"])


@router.get("/overview", response_model=OverviewOut)
async def overview(request: Request, session: SessionDep, settings: SettingsDep) -> OverviewOut:
    require_permission(principal_from_request(request), "ops:read")
    return await build_overview(session, settings)


@router.websocket("/ws")
async def ops_ws(websocket: WebSocket) -> None:
    """Push a full ops snapshot every ~2s so the React console stays live."""
    await websocket.accept()
    settings: Settings = websocket.app.state.settings
    api_keys = getattr(websocket.app.state, "api_keys", None) or parse_api_keys(settings.api_keys)
    key = websocket.query_params.get("api_key") or websocket.headers.get("x-api-key")
    principal = resolve_principal(
        header_key=key,
        api_keys=api_keys,
        legacy_key=settings.api_key,
        auth_required=settings.auth_required,
    )
    if principal is None:
        await websocket.close(code=4401)
        return
    try:
        require_permission(principal, "ops:read")
    except Exception:
        await websocket.close(code=4403)
        return

    factory = websocket.app.state.session_factory or create_session_factory(websocket.app.state.engine)
    try:
        while True:
            async with factory() as session:  # type: AsyncSession
                try:
                    snap = await build_overview(session, settings)
                    await session.commit()
                    await websocket.send_json(snap.model_dump(mode="json"))
                except Exception as exc:
                    await session.rollback()
                    await websocket.send_json({"error": str(exc)})
            await asyncio.sleep(2.0)
    except WebSocketDisconnect:
        return
