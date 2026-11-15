"""Security package: RBAC, PII, governance/audit, RAG retrieval gates."""

from dml.security.pii import PiiResult, scan_and_redact
from dml.security.rag import (
    RagAccessContext,
    RagQueryPrep,
    authorize_rag_search,
    prepare_rag_query,
    sanitize_rag_hit,
)
from dml.security.rbac import (
    CLASSIFICATION_MIN_ROLE,
    PERMISSIONS,
    Principal,
    Role,
    can_access_classification,
    parse_api_keys,
    principal_from_request,
    require_classification_access,
    require_permission,
    require_role,
    resolve_principal,
)

__all__ = [
    "CLASSIFICATION_MIN_ROLE",
    "PERMISSIONS",
    "PiiResult",
    "Principal",
    "RagAccessContext",
    "RagQueryPrep",
    "Role",
    "authorize_rag_search",
    "can_access_classification",
    "parse_api_keys",
    "prepare_rag_query",
    "principal_from_request",
    "require_classification_access",
    "require_permission",
    "require_role",
    "resolve_principal",
    "sanitize_rag_hit",
    "scan_and_redact",
]
