"""Text chunking helpers."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class TextPiece:
    text: str
    index: int


def content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def chunk_text(text: str, *, chunk_chars: int, overlap: int) -> list[TextPiece]:
    cleaned = text.replace("\r\n", "\n").strip()
    if not cleaned:
        return []
    if chunk_chars <= 0:
        return [TextPiece(text=cleaned, index=0)]
    step = max(chunk_chars - max(overlap, 0), 1)
    pieces: list[TextPiece] = []
    start = 0
    idx = 0
    while start < len(cleaned):
        end = min(start + chunk_chars, len(cleaned))
        piece = cleaned[start:end].strip()
        if piece:
            pieces.append(TextPiece(text=piece, index=idx))
            idx += 1
        if end >= len(cleaned):
            break
        start += step
    return pieces


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()
