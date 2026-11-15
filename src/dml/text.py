"""Streaming text helpers. Never require the whole corpus in RAM."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable, Iterator
from dataclasses import dataclass


def normalize_text(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def content_hash(text: str) -> str:
    return hashlib.sha256(normalize_text(text).encode("utf-8")).hexdigest()


def split_line_shards(lines: Iterable[str], *, lines_per_shard: int) -> Iterator[tuple[int, list[str]]]:
    if lines_per_shard < 1:
        raise ValueError("lines_per_shard must be >= 1")
    buffer: list[str] = []
    index = 0
    for line in lines:
        if not line.endswith("\n"):
            line = line + "\n"
        buffer.append(line)
        if len(buffer) >= lines_per_shard:
            yield index, buffer
            index += 1
            buffer = []
    if buffer:
        yield index, buffer


def parse_records(
    text: str, *, content_type: str, return_skip_stats: bool = False
) -> list[dict] | tuple[list[dict], int]:
    """Turn a shard body into logical documents. Shard-sized, not corpus-sized."""
    skipped = 0
    if content_type == "jsonl":
        records = []
        for raw in text.splitlines():
            line = raw.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                # Truncated or corrupt lines (e.g. mid-record shard splits) are skipped.
                skipped += 1
                continue
            if not isinstance(obj, dict):
                skipped += 1
                continue
            body = obj.get("text") or obj.get("content") or obj.get("body")
            if not isinstance(body, str) or not normalize_text(body):
                skipped += 1
                continue
            meta = {k: v for k, v in obj.items() if k not in {"text", "content", "body"}}
            records.append({"text": body, "metadata": meta})
        if return_skip_stats:
            return records, skipped
        return records

    paragraphs = re.split(r"\n\s*\n", text)
    records = []
    for para in paragraphs:
        cleaned = normalize_text(para)
        if cleaned:
            records.append({"text": cleaned, "metadata": {"content_type": content_type}})
    if return_skip_stats:
        return records, skipped
    return records


@dataclass(frozen=True, slots=True)
class TextChunk:
    text: str
    ordinal: int
    byte_count: int
    token_count: int


def chunk_text(text: str, *, max_chars: int, overlap: int) -> list[TextChunk]:
    normalized = normalize_text(text)
    if not normalized:
        return []
    if max_chars < 1:
        raise ValueError("max_chars must be >= 1")
    overlap = max(0, min(overlap, max_chars - 1)) if max_chars > 1 else 0
    chunks: list[TextChunk] = []
    start = 0
    ordinal = 0
    length = len(normalized)
    while start < length:
        end = min(length, start + max_chars)
        piece = normalized[start:end].strip()
        if piece:
            chunks.append(
                TextChunk(
                    text=piece,
                    ordinal=ordinal,
                    byte_count=len(piece.encode("utf-8")),
                    token_count=len(piece.split()),
                )
            )
            ordinal += 1
        if end >= length:
            break
        start = end - overlap
    return chunks
