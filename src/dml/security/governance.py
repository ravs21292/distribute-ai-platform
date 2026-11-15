"""Governance helpers: classification labels and audit persistence."""

from __future__ import annotations

from typing import Any
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from dml.db.models import AuditEvent
from dml.security.rbac import (
    CLASSIFICATION_MIN_ROLE,
    Principal,
    can_access_classification,
    classification_min_role,
    require_classification_access,
)

__all__ = [
    "CLASSIFICATION_MIN_ROLE",
    "can_access_classification",
    "classification_min_role",
    "require_classification_access",
    "source_fingerprint",
    "write_audit",
]


async def write_audit(
    session: AsyncSession,
    *,
    principal: Principal | None,
    action: str,
    resource_type: str,
    resource_id: str | None = None,
    detail: dict[str, Any] | None = None,
) -> None:
    session.add(
        AuditEvent(
            id=uuid4(),
            actor=principal.subject if principal else "anonymous",
            role=principal.role.value if principal else "anonymous",
            action=action,
            resource_type=resource_type,
            resource_id=resource_id,
            detail=detail or {},
        )
    )
    await session.flush()


def source_fingerprint(checksums: list[str]) -> str:
    """Stable fingerprint of an ordered set of source SHA-256 digests."""
    import hashlib

    joined = "|".join(sorted(checksums))
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()
