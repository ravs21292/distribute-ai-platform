"""RAG retrieval API — Phase-8 auth / PII / governance live on this path."""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field
from sqlalchemy import text

from dml.api.deps import SessionDep, SettingsDep
from dml.db.models import Dataset, DatasetVersion
from dml.ml.embeddings import build_embedding_provider, vector_literal
from dml.security.governance import write_audit
from dml.security.rag import (
    authorize_rag_search,
    prepare_rag_query,
    rag_audit_detail,
    rag_governance_payload,
    sanitize_rag_hit,
)
from dml.security.rbac import principal_from_request

router = APIRouter(prefix="/v1", tags=["search"])


class SearchIn(BaseModel):
    query: str
    dataset_version: UUID
    limit: int = Field(default=5, ge=1, le=50)


class SearchHit(BaseModel):
    chunk: str
    source: dict
    similarity: float
    pii_redacted: bool = False


class RagGovernanceOut(BaseModel):
    """Phase-8 governance metadata attached to every RAG response."""

    dataset_id: UUID
    dataset_version_id: UUID
    version_status: str
    classification: str
    pii_policy: str


class SearchOut(BaseModel):
    """RAG result envelope: governance + PII flags + ranked hits."""

    governance: RagGovernanceOut
    query_pii_redacted: bool = False
    query_pii_categories: list[str] = Field(default_factory=list)
    hits: list[SearchHit]


@router.post("/search", response_model=SearchOut)
async def search(
    body: SearchIn, request: Request, session: SessionDep, settings: SettingsDep
) -> SearchOut:
    """RAG: auth → classification → ready → query PII → MiniLM → pgvector → hit PII → audit."""
    version = await session.get(DatasetVersion, body.dataset_version)
    dataset = await session.get(Dataset, version.dataset_id) if version is not None else None
    ctx = authorize_rag_search(
        principal_from_request(request),
        dataset=dataset,
        version=version,
    )

    prepared = prepare_rag_query(
        body.query,
        pii_policy=ctx.pii_policy,
        pii_redaction_enabled=settings.pii_redaction_enabled,
    )

    provider = build_embedding_provider(settings)
    query_vec = provider.embed_batch([prepared.text])[0]
    rows = (
        await session.execute(
            text(
                """
                SELECT c.text, c.source_metadata, c.pii_redacted,
                       1 - (e.embedding <=> CAST(:q AS vector)) AS similarity
                FROM chunks c
                JOIN chunk_embeddings e
                  ON e.content_hash = c.content_hash
                 AND e.embedding_model = :model
                 AND e.embedding_model_version = :ver
                WHERE c.dataset_version_id = :vid
                  AND c.is_duplicate = false
                ORDER BY e.embedding <=> CAST(:q AS vector)
                LIMIT :lim
                """
            ),
            {
                "q": vector_literal(query_vec),
                "vid": str(body.dataset_version),
                "lim": body.limit,
                "model": settings.embedding_model,
                "ver": settings.embedding_model_version,
            },
        )
    ).all()

    hits: list[SearchHit] = []
    for row in rows:
        chunk_text, redacted = sanitize_rag_hit(
            row[0],
            pii_policy=ctx.pii_policy,
            pii_redaction_enabled=settings.pii_redaction_enabled,
            already_redacted=bool(row[2]),
        )
        hits.append(
            SearchHit(
                chunk=chunk_text,
                source=row[1] or {},
                pii_redacted=redacted,
                similarity=float(row[3]),
            )
        )

    await write_audit(
        session,
        principal=ctx.principal,
        action="search.query",
        resource_type="dataset_version",
        resource_id=str(body.dataset_version),
        detail=rag_audit_detail(
            ctx,
            limit=body.limit,
            hits=len(hits),
            query_pii_redacted=prepared.pii.redacted,
            query_pii_categories=list(prepared.pii.categories),
        ),
    )

    gov = rag_governance_payload(ctx)
    return SearchOut(
        governance=RagGovernanceOut(**gov),
        query_pii_redacted=prepared.pii.redacted,
        query_pii_categories=list(prepared.pii.categories),
        hits=hits,
    )
