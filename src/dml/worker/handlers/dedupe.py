"""Exact SHA-256 dedupe at SQL scale. Marks later copies so embed can skip them."""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import text

from dml.contracts.errors import PermanentTaskError, RetryableTaskError
from dml.worker.handlers.base import HandlerResult, TaskContext


class DedupeHandler:
    task_type = "dedupe"

    async def execute(self, ctx: TaskContext) -> HandlerResult:
        try:
            version_id = UUID(ctx.payload["dataset_version_id"])
        except (KeyError, ValueError) as exc:
            raise PermanentTaskError(f"invalid dedupe payload: {exc}", error_type="invalid_payload") from exc

        try:
            # Enterprise path: never load millions of ORM rows into the worker.
            await ctx.session.execute(
                text(
                    """
                    UPDATE chunks
                       SET is_duplicate = false,
                           canonical_chunk_id = NULL
                     WHERE dataset_version_id = :vid
                    """
                ),
                {"vid": str(version_id)},
            )
            result = await ctx.session.execute(
                text(
                    """
                    WITH canon AS (
                        SELECT content_hash, (array_agg(id ORDER BY document_id, ordinal, id))[1] AS canonical_id
                          FROM chunks
                         WHERE dataset_version_id = :vid
                         GROUP BY content_hash
                    )
                    UPDATE chunks c
                       SET is_duplicate = true,
                           canonical_chunk_id = canon.canonical_id
                      FROM canon
                     WHERE c.dataset_version_id = :vid
                       AND c.content_hash = canon.content_hash
                       AND c.id <> canon.canonical_id
                    """
                ),
                {"vid": str(version_id)},
            )
            duplicates = int(result.rowcount or 0)
            totals = (
                await ctx.session.execute(
                    text(
                        """
                        SELECT count(*) AS total,
                               count(*) FILTER (WHERE is_duplicate = false) AS unique_chunks
                          FROM chunks
                         WHERE dataset_version_id = :vid
                        """
                    ),
                    {"vid": str(version_id)},
                )
            ).one()
        except Exception as exc:
            raise RetryableTaskError(str(exc), error_type="dedupe_sql") from exc

        return HandlerResult(
            data={
                "chunks": int(totals[0] or 0),
                "unique": int(totals[1] or 0),
                "duplicates": duplicates,
            }
        )
