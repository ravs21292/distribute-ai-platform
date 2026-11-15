"""Confirm embeddings cover every canonical chunk, then ensure a vector index."""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import text

from dml.contracts.errors import PermanentTaskError, RetryableTaskError
from dml.worker.handlers.base import HandlerResult, TaskContext


class IndexHandler:
    task_type = "index"

    async def execute(self, ctx: TaskContext) -> HandlerResult:
        try:
            version_id = UUID(ctx.payload["dataset_version_id"])
        except (KeyError, ValueError) as exc:
            raise PermanentTaskError(f"invalid index payload: {exc}", error_type="invalid_payload") from exc

        totals = (
            await ctx.session.execute(
                text(
                    """
                    SELECT
                      (SELECT count(*) FROM chunks c
                        WHERE c.dataset_version_id = :vid AND c.is_duplicate = false) AS canonical,
                      (SELECT count(*) FROM chunks c
                        WHERE c.dataset_version_id = :vid
                          AND c.is_duplicate = false
                          AND EXISTS (
                            SELECT 1 FROM chunk_embeddings e
                            WHERE e.content_hash = c.content_hash
                          )) AS embedded
                    """
                ),
                {"vid": str(version_id)},
            )
        ).one()
        canonical = int(totals[0] or 0)
        embedded = int(totals[1] or 0)
        if canonical == 0:
            raise PermanentTaskError("no canonical chunks to index", error_type="missing_artifact")
        if embedded < canonical:
            raise RetryableTaskError(
                f"embedding coverage incomplete: {embedded}/{canonical} canonical chunks",
                error_type="incomplete_embeddings",
            )

        indexed = "none"
        try:
            async with ctx.session.begin_nested():
                await ctx.session.execute(
                    text(
                        """
                        CREATE INDEX IF NOT EXISTS ix_chunk_embeddings_vec
                        ON chunk_embeddings USING ivfflat (embedding vector_cosine_ops)
                        WITH (lists = 100)
                        """
                    )
                )
            indexed = "ivfflat"
        except Exception:
            indexed = "seqscan"
        return HandlerResult(
            data={
                "embeddings": embedded,
                "canonical_chunks": canonical,
                "coverage": 1.0,
                "index": indexed,
            }
        )
