"""Role-based API access. Keys map to roles; endpoints declare required permission."""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import StrEnum

from fastapi import HTTPException, Request


class Role(StrEnum):
    ADMIN = "admin"
    OPERATOR = "operator"
    READER = "reader"


_ROLE_RANK = {Role.READER: 1, Role.OPERATOR: 2, Role.ADMIN: 3}

PERMISSIONS: dict[str, Role] = {
    "datasets:read": Role.READER,
    "search:read": Role.READER,
    "ops:read": Role.READER,
    "workers:read": Role.READER,
    "models:read": Role.READER,
    "jobs:read": Role.READER,
    "pipelines:read": Role.READER,
    "datasets:write": Role.OPERATOR,
    "pipelines:write": Role.OPERATOR,
    "jobs:write": Role.OPERATOR,
    "models:write": Role.ADMIN,
    "governance:write": Role.ADMIN,
    "audit:read": Role.ADMIN,
}

# Dataset classification → minimum role allowed to retrieve / list that data.
CLASSIFICATION_MIN_ROLE: dict[str, Role] = {
    "public": Role.READER,
    "internal": Role.READER,
    "confidential": Role.OPERATOR,
    "restricted": Role.ADMIN,
}


@dataclass(frozen=True, slots=True)
class Principal:
    subject: str
    role: Role
    api_key_id: str


def parse_api_keys(raw: str | None) -> dict[str, Principal]:
    """Parse DML_API_KEYS JSON: {"key": {"subject":"alice","role":"admin"}, ...}."""
    out: dict[str, Principal] = {}
    if not raw:
        return out
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError("DML_API_KEYS must be a JSON object")
    for key, meta in data.items():
        if not key or not isinstance(meta, dict):
            continue
        role = Role(str(meta.get("role", "reader")).lower())
        subject = str(meta.get("subject") or meta.get("name") or "user")
        out[str(key)] = Principal(subject=subject, role=role, api_key_id=str(key)[:8])
    return out


def resolve_principal(
    *,
    header_key: str | None,
    api_keys: dict[str, Principal],
    legacy_key: str | None,
    auth_required: bool,
) -> Principal | None:
    auth_configured = bool(api_keys) or bool(legacy_key) or auth_required
    if header_key:
        if header_key in api_keys:
            return api_keys[header_key]
        if legacy_key and header_key == legacy_key:
            return Principal(subject="legacy-admin", role=Role.ADMIN, api_key_id="legacy")
        return None
    if not auth_configured:
        # Local demo mode: open API with admin principal for convenience.
        return Principal(subject="anonymous", role=Role.ADMIN, api_key_id="dev")
    return None


def require_role(principal: Principal | None, minimum: Role) -> Principal:
    if principal is None:
        raise HTTPException(status_code=401, detail="unauthorized")
    if _ROLE_RANK[principal.role] < _ROLE_RANK[minimum]:
        raise HTTPException(
            status_code=403,
            detail=f"role {principal.role} cannot perform action requiring {minimum}",
        )
    return principal


def require_permission(principal: Principal | None, permission: str) -> Principal:
    needed = PERMISSIONS.get(permission)
    if needed is None:
        raise HTTPException(status_code=500, detail=f"unknown permission {permission}")
    return require_role(principal, needed)


def classification_min_role(classification: str) -> Role:
    return CLASSIFICATION_MIN_ROLE.get((classification or "internal").lower(), Role.ADMIN)


def can_access_classification(principal: Principal | None, classification: str) -> bool:
    if principal is None:
        return False
    return _ROLE_RANK[principal.role] >= _ROLE_RANK[classification_min_role(classification)]


def require_classification_access(principal: Principal | None, classification: str) -> Principal:
    """Gate RAG/dataset reads by governance classification label."""
    principal = require_role(principal, Role.READER)
    needed = classification_min_role(classification)
    if _ROLE_RANK[principal.role] < _ROLE_RANK[needed]:
        raise HTTPException(
            status_code=403,
            detail=(
                f"role {principal.role} cannot access classification "
                f"{classification!r} (requires {needed})"
            ),
        )
    return principal


def principal_from_request(request: Request) -> Principal | None:
    return getattr(request.state, "principal", None)
