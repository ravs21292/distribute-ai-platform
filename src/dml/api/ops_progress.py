"""Pure helpers for ops progress messaging (no FastAPI / DB imports)."""

from __future__ import annotations


def resolve_searchable_progress(
    *,
    phase: str,
    latest_status: str | None,
    version_chunks: int,
    version_embeddings: int,
) -> tuple[str, str, float]:
    """Decide ops phase/message when a pipeline claims completion.

    Returns (phase, message, percent). Never claims searchable unless the
    latest version is ready and embeddings cover canonical chunks.
    """
    embeds_complete = version_chunks > 0 and version_embeddings >= version_chunks
    embed_total = max(version_chunks, version_embeddings, 1)
    embed_done = min(version_embeddings, embed_total)
    if latest_status == "failed" or phase == "failed":
        return (
            "failed",
            (
                "Latest dataset version failed — embeddings are not searchable for this version. "
                f"Canonical chunks embedded: {version_embeddings}/{version_chunks or 0}."
            ),
            min(99.0, (embed_done / embed_total) * 100) if version_chunks else 0.0,
        )
    if phase == "completed" and latest_status == "ready" and embeds_complete:
        return (
            "completed",
            "Pipeline finished. Embeddings are searchable in pgvector.",
            100.0,
        )
    if phase == "completed":
        return (
            "incomplete",
            (
                f"Pipeline reported complete, but embeddings are incomplete "
                f"({version_embeddings}/{version_chunks or 0}) or version is not ready "
                f"({latest_status or 'none'})."
            ),
            min(99.0, (embed_done / embed_total) * 100) if version_chunks else 50.0,
        )
    return (phase, "", 0.0)
