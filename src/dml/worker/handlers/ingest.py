"""Stream a source object into bounded raw shards in object storage."""

from __future__ import annotations

import asyncio
import os
import tempfile
from pathlib import Path
from uuid import UUID

from dml.contracts.errors import PermanentTaskError, RetryableTaskError
from dml.db.datasets import DatasetRepository
from dml.storage import ObjectStore
from dml.text import split_line_shards
from dml.worker.handlers.base import HandlerResult, TaskContext


class IngestHandler:
    task_type = "ingest"

    def __init__(self, store: ObjectStore) -> None:
        self.store = store

    async def execute(self, ctx: TaskContext) -> HandlerResult:
        try:
            version_id = UUID(ctx.payload["dataset_version_id"])
            dataset_id = ctx.payload["dataset_id"]
            version_label = ctx.payload["version_label"]
            source_id = UUID(ctx.payload["source_id"])
            source_key = ctx.payload["source_key"]
            filename = ctx.payload["filename"]
            lines_per_shard = int(ctx.payload.get("lines_per_shard") or 500)
        except (KeyError, ValueError, TypeError) as exc:
            raise PermanentTaskError(f"invalid ingest payload: {exc}", error_type="invalid_payload") from exc

        stem = Path(filename).stem

        def _split() -> list[dict]:
            if not self.store.exists(source_key):
                raise PermanentTaskError(
                    f"missing source object {source_key}", error_type="missing_artifact"
                )
            # Copy out of the S3 streaming body first. Wrapping boto3 StreamingBody
            # with TextIOWrapper raises "I/O operation on closed file" on close.
            tmp = tempfile.NamedTemporaryFile(delete=False)
            try:
                fh = self.store.open_read(source_key)
                try:
                    while True:
                        chunk = fh.read(8 * 1024 * 1024)
                        if not chunk:
                            break
                        tmp.write(chunk)
                finally:
                    fh.close()
                tmp.close()
                written: list[dict] = []
                with open(tmp.name, encoding="utf-8", errors="replace") as wrapper:
                    for index, lines in split_line_shards(wrapper, lines_per_shard=lines_per_shard):
                        body = "".join(lines).encode("utf-8")
                        shard_key = f"{stem}-{index:05d}"
                        raw_key = f"raw/{dataset_id}/{version_label}/{shard_key}.txt"
                        self.store.put_bytes(raw_key, body)
                        written.append(
                            {
                                "shard_key": shard_key,
                                "raw_object_key": raw_key,
                                "line_count": len(lines),
                                "byte_count": len(body),
                            }
                        )
                return written
            finally:
                try:
                    os.unlink(tmp.name)
                except OSError:
                    pass

        try:
            written = await asyncio.to_thread(_split)
        except PermanentTaskError:
            raise
        except OSError as exc:
            raise RetryableTaskError(str(exc), error_type="object_store") from exc

        if not written:
            raise PermanentTaskError("source produced no shards", error_type="empty_source")

        repo = DatasetRepository(ctx.session)
        for item in written:
            await repo.upsert_shard(
                dataset_version_id=version_id,
                source_id=source_id,
                shard_key=item["shard_key"],
                status="raw_ready",
                raw_object_key=item["raw_object_key"],
                line_count=item["line_count"],
                byte_count=item["byte_count"],
            )
        return HandlerResult(
            data={"shard_count": len(written), "source_key": source_key, "filename": filename}
        )