"""Quality-gate promotion into the immutable model registry."""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import select

from dml.contracts.errors import PermanentTaskError
from dml.db.models import Evaluation, TrainingRun
from dml.db.registry import ModelRegistry
from dml.worker.handlers.base import HandlerResult, TaskContext


class ArtifactHandler:
    task_type = "artifact"

    async def execute(self, ctx: TaskContext) -> HandlerResult:
        try:
            version_id = UUID(ctx.payload["dataset_version_id"])
        except (KeyError, ValueError) as exc:
            raise PermanentTaskError(f"invalid promote payload: {exc}", error_type="invalid_payload") from exc

        run = (
            await ctx.session.execute(
                select(TrainingRun)
                .where(TrainingRun.dataset_version_id == version_id)
                .order_by(TrainingRun.created_at.desc())
            )
        ).scalars().first()
        if run is None:
            raise PermanentTaskError("no training run", error_type="missing_artifact")
        evaluation = (
            await ctx.session.execute(
                select(Evaluation)
                .where(Evaluation.training_run_id == run.id)
                .order_by(Evaluation.created_at.desc())
            )
        ).scalars().first()
        if evaluation is None:
            raise PermanentTaskError("no evaluation result", error_type="missing_artifact")
        if not evaluation.quality_passed:
            return HandlerResult(
                data={"promoted": False, "reason": "quality_gate_failed", "perplexity": evaluation.perplexity}
            )
        registry = ModelRegistry(ctx.session)
        model_version = await registry.publish(
            name="tinygpt",
            artifact_key=evaluation.checkpoint_key,
            evaluation_id=evaluation.id,
            alias="candidate",
        )
        return HandlerResult(
            data={
                "promoted": True,
                "model": "tinygpt",
                "version": model_version.version,
                "alias": "candidate",
                "artifact_key": evaluation.checkpoint_key,
            }
        )
