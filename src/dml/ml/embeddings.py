"""Embedding providers. HuggingFace MiniLM in production; local hash for tests."""

from __future__ import annotations

import hashlib
from typing import Protocol

import numpy as np

from dml.config import Settings


class EmbeddingProvider(Protocol):
    model: str
    model_version: str
    dim: int

    def embed_batch(self, texts: list[str]) -> list[list[float]]: ...


class LocalEmbeddingProvider:
    """Signed hashed n-grams projected into a fixed unit vector."""

    def __init__(self, *, dim: int = 384, model: str = "local-hash", model_version: str = "v1") -> None:
        self.dim = dim
        self.model = model
        self.model_version = model_version

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        return [self._embed_one(text) for text in texts]

    def _embed_one(self, text: str) -> list[float]:
        vec = np.zeros(self.dim, dtype=np.float64)
        tokens = text.lower().split() or [text.lower() or " "]
        for token in tokens:
            digest = hashlib.sha256(token.encode("utf-8")).digest()
            for i in range(self.dim):
                byte = digest[i % len(digest)]
                vec[i] += (byte / 127.5) - 1.0
        norm = np.linalg.norm(vec)
        if norm > 0:
            vec = vec / norm
        return [float(x) for x in vec]


class HuggingFaceEmbeddingProvider:
    """sentence-transformers model from the Hugging Face Hub. CPU by default."""

    def __init__(
        self,
        *,
        model: str = "sentence-transformers/all-MiniLM-L6-v2",
        model_version: str = "v1",
        dim: int = 384,
        batch_size: int = 16,
    ) -> None:
        self.model = model
        self.model_version = model_version
        self.dim = dim
        self.batch_size = batch_size
        self._encoder = None

    def _load(self):
        if self._encoder is None:
            try:
                from sentence_transformers import SentenceTransformer
            except ImportError as exc:
                raise RuntimeError(
                    "HuggingFace embeddings need sentence-transformers. "
                    "Build embedding-worker from infra/docker/Dockerfile.embed."
                ) from exc
            self._encoder = SentenceTransformer(self.model)
        return self._encoder

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        encoded = self._load().encode(
            texts,
            batch_size=self.batch_size,
            normalize_embeddings=True,
            show_progress_bar=False,
        )
        rows = encoded.tolist() if hasattr(encoded, "tolist") else [list(map(float, encoded))]
        if rows and len(rows[0]) != self.dim:
            self.dim = len(rows[0])
        return rows


def build_embedding_provider(settings: Settings) -> LocalEmbeddingProvider | HuggingFaceEmbeddingProvider:
    backend = (settings.embedding_backend or "huggingface").lower()
    if backend in {"local", "hash", "local-hash"}:
        return LocalEmbeddingProvider(
            dim=settings.embedding_dim,
            model=settings.embedding_model,
            model_version=settings.embedding_model_version,
        )
    return HuggingFaceEmbeddingProvider(
        model=settings.embedding_model,
        model_version=settings.embedding_model_version,
        dim=settings.embedding_dim,
        batch_size=settings.embedding_batch_size,
    )


def vector_literal(values: list[float]) -> str:
    return "[" + ",".join(f"{v:.8f}" for v in values) + "]"
