"""Separate evaluation stage: loss, perplexity, smoke generation, artifact check."""

from __future__ import annotations

import asyncio
from uuid import UUID, uuid4

from sqlalchemy import select

from dml.config import Settings
from dml.contracts.errors import PermanentTaskError
from dml.db.models import Chunk, Evaluation, TrainingRun
from dml.ml.tinygpt import TinyGPT
from dml.storage import ObjectStore
from dml.worker.handlers.base import HandlerResult, TaskContext


class EvaluationHandler:
    task_type = "evaluation"

    def __init__(self, store: ObjectStore, settings: Settings) -> None:
        self.store = store
        self.settings = settings

    async def execute(self, ctx: TaskContext) -> HandlerResult:
        try:
            version_id = UUID(ctx.payload["dataset_version_id"])
        except (KeyError, ValueError) as exc:
            raise PermanentTaskError(f"invalid evaluation payload: {exc}", error_type="invalid_payload") from exc

        run = (
            await ctx.session.execute(
                select(TrainingRun)
                .where(TrainingRun.dataset_version_id == version_id)
                .order_by(TrainingRun.created_at.desc())
            )
        ).scalars().first()
        if run is None or not run.latest_checkpoint_key:
            raise PermanentTaskError("no training checkpoint to evaluate", error_type="missing_artifact")
        artifact_ok = self.store.exists(run.latest_checkpoint_key)
        if not artifact_ok:
            raise PermanentTaskError("checkpoint object missing", error_type="missing_artifact")

        texts = list(
            (
                await ctx.session.execute(
                    select(Chunk.text)
                    .where(Chunk.dataset_version_id == version_id)
                    .where(Chunk.is_duplicate.is_(False))
                    .limit(200)
                )
            ).scalars()
        )
        corpus = "\n".join(texts) or "hello world"

        def _eval() -> tuple[float, float, str]:
            model = TinyGPT.loads(self.store.get_bytes(run.latest_checkpoint_key or ""))
            loss, ppl = model.evaluate(corpus)
            sample = model.generate("The ", 24)
            return loss, ppl, sample

        loss, ppl, sample = await asyncio.to_thread(_eval)
        smoke_ok = bool(sample and sample.strip())
        quality = artifact_ok and smoke_ok and ppl <= self.settings.quality_max_perplexity
        row = Evaluation(
            id=uuid4(),
            training_run_id=run.id,
            checkpoint_key=run.latest_checkpoint_key,
            validation_loss=loss,
            perplexity=ppl,
            smoke_ok=smoke_ok,
            artifact_ok=artifact_ok,
            quality_passed=quality,
            sample_text=sample,
        )
        ctx.session.add(row)
        return HandlerResult(
            data={
                "evaluation_id": str(row.id),
                "validation_loss": loss,
                "perplexity": ppl,
                "smoke_ok": smoke_ok,
                "artifact_ok": artifact_ok,
                "quality_passed": quality,
                "sample_text": sample,
            }
        )
