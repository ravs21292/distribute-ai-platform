"""Embed canonical chunks. Content-addressed: same text → skip encode; changed text → new vector."""

from __future__ import annotations

from uuid import UUID, uuid4

from sqlalchemy import bindparam, select, text

from dml.contracts.errors import PermanentTaskError, RetryableTaskError
from dml.db.models import Chunk
from dml.ml.embeddings import EmbeddingProvider, vector_literal
from dml.worker.handlers.base import HandlerResult, TaskContext


class EmbeddingHandler:
    task_type = "embedding"

    def __init__(self, provider: EmbeddingProvider) -> None:
        self.provider = provider

    async def execute(self, ctx: TaskContext) -> HandlerResult:
        try:
            version_id = UUID(ctx.payload["dataset_version_id"])
        except (KeyError, ValueError) as exc:
            raise PermanentTaskError(f"invalid embedding payload: {exc}", error_type="invalid_payload") from exc

        shard_id = ctx.payload.get("shard_id")
        stmt = (
            select(Chunk)
            .where(Chunk.dataset_version_id == version_id)
            .where(Chunk.is_duplicate.is_(False))
        )
        if shard_id:
            stmt = stmt.where(Chunk.shard_id == UUID(shard_id))
        chunks = list((await ctx.session.execute(stmt)).scalars())
        if not chunks:
            return HandlerResult(data={"embedded": 0, "skipped": 0, "relinked": 0})

        existing = (
            await ctx.session.execute(
                text(
                    """
                    SELECT content_hash FROM chunk_embeddings
                    WHERE embedding_model = :model AND embedding_model_version = :ver
                      AND content_hash IN :hashes
                    """
                ).bindparams(bindparam("hashes", expanding=True)),
                {
                    "model": self.provider.model,
                    "ver": self.provider.model_version,
                    "hashes": [c.content_hash for c in chunks],
                },
            )
        ).scalars()
        have = set(existing)
        todo = [c for c in chunks if c.content_hash not in have]
        known = [c for c in chunks if c.content_hash in have]
        skipped = len(known)
        relinked = 0
        for chunk in known:
            await ctx.session.execute(
                text(
                    """
                    UPDATE chunk_embeddings
                       SET chunk_id = :chunk_id,
                           dataset_version_id = :version_id
                     WHERE content_hash = :hash
                       AND embedding_model = :model
                       AND embedding_model_version = :ver
                    """
                ),
                {
                    "chunk_id": str(chunk.id),
                    "version_id": str(version_id),
                    "hash": chunk.content_hash,
                    "model": self.provider.model,
                    "ver": self.provider.model_version,
                },
            )
            relinked += 1

        embedded = 0
        batch_size = int(getattr(self.provider, "batch_size", 0) or 32)
        try:
            for start in range(0, len(todo), batch_size):
                batch = todo[start : start + batch_size]
                vectors = self.provider.embed_batch([c.text for c in batch])
                for chunk, vec in zip(batch, vectors, strict=True):
                    await ctx.session.execute(
                        text(
                            """
                            INSERT INTO chunk_embeddings (
                                id, chunk_id, dataset_version_id, content_hash,
                                embedding_model, embedding_model_version, embedding
                            ) VALUES (
                                :id, :chunk_id, :version_id, :hash,
                                :model, :ver, CAST(:vec AS vector)
                            )
                            ON CONFLICT (content_hash, embedding_model, embedding_model_version)
                            DO UPDATE SET
                                chunk_id = EXCLUDED.chunk_id,
                                dataset_version_id = EXCLUDED.dataset_version_id
                            """
                        ),
                        {
                            "id": str(uuid4()),
                            "chunk_id": str(chunk.id),
                            "version_id": str(version_id),
                            "hash": chunk.content_hash,
                            "model": self.provider.model,
                            "ver": self.provider.model_version,
                            "vec": vector_literal(vec),
                        },
                    )
                    embedded += 1
        except Exception as exc:
            raise RetryableTaskError(str(exc), error_type="embedding_persist") from exc
        return HandlerResult(
            data={
                "embedded": embedded,
                "skipped": skipped,
                "relinked": relinked,
                "model": self.provider.model,
                "model_version": self.provider.model_version,
            }
        )
