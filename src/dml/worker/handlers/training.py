"""TinyGPT training with durable MinIO/local checkpoints."""

from __future__ import annotations

import asyncio
from uuid import UUID, uuid4

from sqlalchemy import select

from dml.config import Settings
from dml.contracts.errors import PermanentTaskError, RetryableTaskError
from dml.db.models import Chunk, TrainingCheckpoint, TrainingRun
from dml.ml.tinygpt import TinyGPT
from dml.storage import ObjectStore
from dml.worker.handlers.base import HandlerResult, TaskContext


class TrainingHandler:
    task_type = "training"

    def __init__(self, store: ObjectStore, settings: Settings) -> None:
        self.store = store
        self.settings = settings

    async def execute(self, ctx: TaskContext) -> HandlerResult:
        try:
            version_id = UUID(ctx.payload["dataset_version_id"])
        except (KeyError, ValueError) as exc:
            raise PermanentTaskError(f"invalid training payload: {exc}", error_type="invalid_payload") from exc

        pipeline_id = ctx.payload.get("pipeline_run_id")
        existing = (
            await ctx.session.execute(
                select(TrainingRun)
                .where(TrainingRun.dataset_version_id == version_id)
                .order_by(TrainingRun.created_at.desc())
            )
        ).scalars().first()
        run = existing
        if run is None:
            run = TrainingRun(
                id=uuid4(),
                dataset_version_id=version_id,
                pipeline_run_id=UUID(pipeline_id) if pipeline_id else None,
                status="running",
                max_steps=self.settings.train_max_steps,
            )
            ctx.session.add(run)
            await ctx.session.flush()

        texts = list(
            (
                await ctx.session.execute(
                    select(Chunk.text)
                    .where(Chunk.dataset_version_id == version_id)
                    .where(Chunk.is_duplicate.is_(False))
                    .limit(400)
                )
            ).scalars()
        )
        corpus = "\n".join(texts)
        if len(corpus) < 32:
            raise PermanentTaskError("not enough text to train", error_type="missing_artifact")

        start_key = run.latest_checkpoint_key
        run_id = run.id
        max_steps = self.settings.train_max_steps
        every = self.settings.train_checkpoint_every
        batch_size = self.settings.train_batch_size
        lr = self.settings.train_lr
        store = self.store

        def _train() -> tuple[int, float, str | None]:
            model = TinyGPT()
            if start_key and store.exists(start_key):
                model = TinyGPT.loads(store.get_bytes(start_key))
            last_loss = 0.0
            last_key = start_key
            while model.step < max_steps:
                last_loss = model.train_step(corpus, batch_size=batch_size, lr=lr)
                if model.step % every == 0 or model.step >= max_steps:
                    last_key = f"training/{run_id}/checkpoint-{model.step}.npz"
                    store.put_bytes(last_key, model.dumps())
            return model.step, last_loss, last_key

        try:
            step, last_loss, last_key = await asyncio.to_thread(_train)
        except OSError as exc:
            raise RetryableTaskError(str(exc), error_type="object_store") from exc

        run.current_step = step
        run.last_loss = last_loss
        run.latest_checkpoint_key = last_key
        run.status = "completed"
        if last_key:
            ctx.session.add(
                TrainingCheckpoint(
                    id=uuid4(),
                    training_run_id=run.id,
                    step=step,
                    object_key=last_key,
                    loss=last_loss,
                )
            )
        return HandlerResult(
            data={
                "training_run_id": str(run.id),
                "step": step,
                "loss": last_loss,
                "checkpoint_key": last_key,
            }
        )
