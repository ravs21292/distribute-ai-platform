"""Embedding providers for the embed service."""

from __future__ import annotations

import hashlib
from typing import Protocol

import numpy as np

from dap_common.config import Settings


class EmbeddingProvider(Protocol):
    model: str
    model_version: str
    dim: int

    def embed_batch(self, texts: list[str]) -> list[list[float]]: ...


class DeterministicEmbeddingProvider:
    """Fast local vectors for CI / low-memory laptops. Not a production model."""

    def __init__(self, *, dim: int = 384, model: str = "deterministic", model_version: str = "v1") -> None:
        self.dim = dim
        self.model = model
        self.model_version = model_version

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        return [self._one(t) for t in texts]

    def _one(self, text: str) -> list[float]:
        vec = np.zeros(self.dim, dtype=np.float64)
        tokens = text.lower().split() or [" "]
        for token in tokens:
            digest = hashlib.sha256(token.encode("utf-8")).digest()
            for i in range(self.dim):
                vec[i] += (digest[i % len(digest)] / 127.5) - 1.0
        norm = np.linalg.norm(vec)
        if norm > 0:
            vec = vec / norm
        return [float(x) for x in vec]


class HuggingFaceEmbeddingProvider:
    def __init__(
        self,
        *,
        model: str,
        model_version: str,
        dim: int,
        batch_size: int,
    ) -> None:
        self.model = model
        self.model_version = model_version
        self.dim = dim
        self.batch_size = batch_size
        self._encoder = None

    def _load(self):
        if self._encoder is None:
            from sentence_transformers import SentenceTransformer

            self._encoder = SentenceTransformer(self.model)
        return self._encoder

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        encoded = self._load().encode(texts, batch_size=self.batch_size, normalize_embeddings=True)
        return [row.tolist() for row in encoded]


def build_embedding_provider(settings: Settings) -> EmbeddingProvider:
    backend = (settings.embedding_backend or "huggingface").lower()
    if backend in {"deterministic", "local"}:
        return DeterministicEmbeddingProvider(dim=settings.embedding_dim)
    return HuggingFaceEmbeddingProvider(
        model=settings.embedding_model,
        model_version=settings.embedding_model_version,
        dim=settings.embedding_dim,
        batch_size=settings.embedding_batch_size,
    )


def vector_literal(values: list[float]) -> str:
    return "[" + ",".join(f"{v:.8f}" for v in values) + "]"
