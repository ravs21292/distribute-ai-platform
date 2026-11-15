"""Phase-8 RAG retrieval security: auth + classification + search-time PII.

Pipeline ownership (RAG side):
  1. Auth / RBAC        — caller needs search:read
  2. Governance         — dataset classification must match role
  3. Ready gate         — only searchable when version status is ready
  4. Query PII          — apply dataset pii_policy before embedding the query
  5. Hit PII (defense)  — re-scan returned chunks when policy is redact
  6. Audit              — search.query without logging raw query text
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from fastapi import HTTPException

from dml.db.models import Dataset, DatasetVersion
from dml.security.pii import PiiResult, scan_and_redact
from dml.security.rbac import Principal, require_classification_access, require_permission


@dataclass(frozen=True, slots=True)
class RagAccessContext:
    """Authorized RAG retrieval context for one dataset version."""

    principal: Principal
    dataset: Dataset
    version: DatasetVersion
    classification: str
    pii_policy: str


@dataclass(frozen=True, slots=True)
class RagQueryPrep:
    """Query after RAG-side PII handling, safe to embed."""

    text: str
    pii: PiiResult


def authorize_rag_search(
    principal: Principal | None,
    *,
    dataset: Dataset | None,
    version: DatasetVersion | None,
) -> RagAccessContext:
    """Auth + governance + ready-version gate for POST /v1/search."""
    principal = require_permission(principal, "search:read")
    if version is None:
        raise HTTPException(status_code=404, detail="dataset version not found")
    if version.status != "ready":
        raise HTTPException(
            status_code=409,
            detail=f"dataset version is {version.status!r}; search requires status 'ready'",
        )
    if dataset is None:
        raise HTTPException(status_code=404, detail="dataset not found")
    require_classification_access(principal, dataset.classification)
    policy = (dataset.pii_policy or "redact").lower()
    if policy not in {"redact", "reject", "allow"}:
        policy = "redact"
    return RagAccessContext(
        principal=principal,
        dataset=dataset,
        version=version,
        classification=(dataset.classification or "internal").lower(),
        pii_policy=policy,
    )


def prepare_rag_query(
    query: str,
    *,
    pii_policy: str,
    pii_redaction_enabled: bool,
) -> RagQueryPrep:
    """Apply dataset PII policy to the user query before MiniLM embedding."""
    text = (query or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="query must not be empty")

    # Global kill-switch mirrors ETL: when disabled, treat as allow.
    effective = "allow" if not pii_redaction_enabled else (pii_policy or "redact").lower()
    if effective == "allow":
        return RagQueryPrep(text=text, pii=PiiResult(text=text, categories=[], redacted=False))

    pii = scan_and_redact(text, enabled=True)
    if pii.categories and effective == "reject":
        raise HTTPException(
            status_code=400,
            detail={
                "error": "query_pii_rejected",
                "categories": pii.categories,
                "message": "query contains PII and dataset pii_policy is reject",
            },
        )
    if effective == "redact" and pii.redacted:
        return RagQueryPrep(text=pii.text, pii=pii)
    return RagQueryPrep(text=text, pii=PiiResult(text=text, categories=[], redacted=False))


def sanitize_rag_hit(
    text: str,
    *,
    pii_policy: str,
    pii_redaction_enabled: bool,
    already_redacted: bool,
) -> tuple[str, bool]:
    """Defense-in-depth: re-redact hit text when policy is redact."""
    effective = "allow" if not pii_redaction_enabled else (pii_policy or "redact").lower()
    if effective != "redact":
        return text, bool(already_redacted)
    pii = scan_and_redact(text, enabled=True)
    if pii.redacted:
        return pii.text, True
    return text, bool(already_redacted)


def rag_audit_detail(
    ctx: RagAccessContext,
    *,
    limit: int,
    hits: int,
    query_pii_redacted: bool,
    query_pii_categories: list[str],
) -> dict[str, Any]:
    """Audit payload for search.query — never includes the raw query string."""
    return {
        "limit": limit,
        "hits": hits,
        "classification": ctx.classification,
        "pii_policy": ctx.pii_policy,
        "dataset_id": str(ctx.dataset.id),
        "query_pii_redacted": query_pii_redacted,
        "query_pii_categories": query_pii_categories,
    }


def rag_governance_payload(ctx: RagAccessContext) -> dict[str, Any]:
    """Governance block returned with every RAG search response."""
    return {
        "dataset_id": ctx.dataset.id,
        "dataset_version_id": ctx.version.id,
        "version_status": ctx.version.status,
        "classification": ctx.classification,
        "pii_policy": ctx.pii_policy,
    }
