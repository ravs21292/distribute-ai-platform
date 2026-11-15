from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import select

from dml.api.deps import SessionDep
from dml.db.models import AuditEvent
from dml.security.rbac import principal_from_request, require_permission

router = APIRouter(prefix="/v1/audit", tags=["audit"])


class AuditOut(BaseModel):
    id: UUID
    actor: str
    role: str
    action: str
    resource_type: str
    resource_id: str | None
    detail: dict
    created_at: str | None
    model_config = {"from_attributes": True}


@router.get("/events", response_model=list[AuditOut])
async def list_audit_events(request: Request, session: SessionDep, limit: int = 100) -> list[AuditOut]:
    require_permission(principal_from_request(request), "audit:read")
    limit = max(1, min(limit, 500))
    rows = list(
        (
            await session.execute(
                select(AuditEvent).order_by(AuditEvent.created_at.desc()).limit(limit)
            )
        ).scalars()
    )
    out = []
    for row in rows:
        out.append(
            AuditOut(
                id=row.id,
                actor=row.actor,
                role=row.role,
                action=row.action,
                resource_type=row.resource_type,
                resource_id=row.resource_id,
                detail=row.detail or {},
                created_at=row.created_at.isoformat() if row.created_at else None,
            )
        )
    return out
