from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request, Response
from sqlalchemy import text

from dml.api.deps import SessionDep, get_metrics
from dml.queue.redis_streams import RedisTaskQueue

router = APIRouter()


@router.get("/health/live")
async def live() -> dict[str, str]:
    return {"status": "live"}


@router.get("/health/ready")
async def ready(session: SessionDep, request: Request) -> dict[str, str]:
    try:
        await session.execute(text("SELECT 1"))
        queue: RedisTaskQueue = request.app.state.queue
        await queue.ping()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"not ready: {exc}") from exc
    return {"status": "ready"}


@router.get("/metrics")
async def metrics(request: Request) -> Response:
    payload, content_type = get_metrics(request).render()
    return Response(content=payload, media_type=content_type)
