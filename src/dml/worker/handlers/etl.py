"""Normalize, chunk, and persist a single raw shard. Idempotent per shard_key.

Applies governance PII policy before hashing so embeddings never see raw PII
when policy is redact/reject.
"""

from __future__ import annotations

import asyncio
import json
from uuid import UUID, uuid4

from dml.contracts.errors import PermanentTaskError, RetryableTaskError
from dml.db.datasets import DatasetRepository
from dml.db.models import Chunk, Dataset
from dml.security.pii import scan_and_redact
from dml.storage import ObjectStore
from dml.text import chunk_text, content_hash, parse_records
from dml.worker.handlers.base import HandlerResult, TaskContext


class ETLHandler:
    task_type = "etl"

    def __init__(self, store: ObjectStore, *, pii_redaction_enabled: bool = True) -> None:
        self.store = store
        self.pii_redaction_enabled = pii_redaction_enabled

    async def execute(self, ctx: TaskContext) -> HandlerResult:
        try:
            version_id = UUID(ctx.payload["dataset_version_id"])
            dataset_id = ctx.payload["dataset_id"]
            version_label = ctx.payload["version_label"]
            shard_key = ctx.payload["shard_key"]
            raw_key = ctx.payload["raw_object_key"]
            content_type = ctx.payload.get("content_type") or "txt"
            chunk_chars = int(ctx.payload.get("chunk_chars") or 1000)
            chunk_overlap = int(ctx.payload.get("chunk_overlap") or 100)
        except (KeyError, ValueError, TypeError) as exc:
            raise PermanentTaskError(f"invalid etl payload: {exc}", error_type="invalid_payload") from exc

        dataset = await ctx.session.get(Dataset, UUID(str(dataset_id)))
        pii_policy = (dataset.pii_policy if dataset else None) or ctx.payload.get("pii_policy") or "redact"
        # Global kill-switch forces allow; when enabled, dataset policy decides.
        if not self.pii_redaction_enabled:
            pii_policy = "allow"
        pii_enabled = pii_policy != "allow"

        processed_key = f"processed/{dataset_id}/{version_label}/{shard_key}.jsonl"

        def _transform() -> tuple[list[dict], bytes, int]:
            if not self.store.exists(raw_key):
                raise PermanentTaskError(f"missing raw shard {raw_key}", error_type="missing_artifact")
            raw = self.store.get_bytes(raw_key)
            try:
                text = raw.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise PermanentTaskError("shard is not valid utf-8", error_type="unsupported_format") from exc
            # JSONL: skip corrupt/truncated lines; never dead-letter a whole shard for one bad line.
            documents, skipped_lines = parse_records(
                text, content_type=content_type, return_skip_stats=True
            )
            if not documents and content_type == "jsonl" and skipped_lines > 0:
                # Shard had only unusable lines — succeed empty so the version can still reach ready.
                return [], b"", 0
            rows: list[dict] = []
            ordinal = 0
            pii_chunk_count = 0
            for doc_index, document in enumerate(documents):
                document_id = f"{shard_key}-doc-{doc_index:05d}"
                for piece in chunk_text(
                    document["text"], max_chars=chunk_chars, overlap=chunk_overlap
                ):
                    pii = scan_and_redact(piece.text, enabled=pii_enabled)
                    if pii.categories and pii_policy == "reject":
                        raise PermanentTaskError(
                            f"PII detected ({','.join(pii.categories)}) under reject policy",
                            error_type="pii_rejected",
                        )
                    text_out = pii.text if pii_policy == "redact" else piece.text
                    if pii.categories:
                        pii_chunk_count += 1
                    rows.append(
                        {
                            "document_id": document_id,
                            "ordinal": ordinal,
                            "text": text_out,
                            "content_hash": content_hash(text_out),
                            "byte_count": len(text_out.encode("utf-8")),
                            "token_count": len(text_out.split()),
                            "pii_categories": pii.categories,
                            "pii_redacted": bool(pii.redacted and pii_policy == "redact"),
                            "source_metadata": {
                                **document.get("metadata", {}),
                                "shard_key": shard_key,
                                "pii_categories": pii.categories,
                            },
                        }
                    )
                    ordinal += 1
            payload = "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows).encode("utf-8")
            self.store.put_bytes(processed_key, payload)
            return rows, payload, pii_chunk_count

        try:
            rows, payload, pii_chunk_count = await asyncio.to_thread(_transform)
        except PermanentTaskError:
            raise
        except OSError as exc:
            raise RetryableTaskError(str(exc), error_type="object_store") from exc

        repo = DatasetRepository(ctx.session)
        shard = await repo.upsert_shard(
            dataset_version_id=version_id,
            source_id=None,
            shard_key=shard_key,
            status="processed",
            raw_object_key=raw_key,
            processed_object_key=processed_key,
            document_count=len({row["document_id"] for row in rows}),
            chunk_count=len(rows),
            byte_count=len(payload),
        )
        chunks = [
            Chunk(
                id=uuid4(),
                dataset_version_id=version_id,
                shard_id=shard.id,
                document_id=row["document_id"],
                ordinal=row["ordinal"],
                text=row["text"],
                content_hash=row["content_hash"],
                byte_count=row["byte_count"],
                token_count=row["token_count"],
                source_metadata=row["source_metadata"],
                pii_categories=row["pii_categories"],
                pii_redacted=row["pii_redacted"],
            )
            for row in rows
        ]
        await repo.replace_shard_chunks(shard=shard, chunks=chunks)
        version = await repo.get_version(version_id)
        if version is not None and pii_chunk_count:
            version.pii_chunks = int(version.pii_chunks or 0) + pii_chunk_count
        return HandlerResult(
            data={
                "shard_key": shard_key,
                "processed_object_key": processed_key,
                "document_count": shard.document_count,
                "chunk_count": shard.chunk_count,
                "pii_chunks": pii_chunk_count,
                "pii_policy": pii_policy,
            }
        )
