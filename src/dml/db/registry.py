"""Immutable model versions + movable aliases. Serving must resolve aliases here."""

from __future__ import annotations

from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dml.db.models import ModelAlias, ModelVersion, RegisteredModel


class ModelRegistry:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def ensure_model(self, name: str) -> RegisteredModel:
        model = await self.session.get(RegisteredModel, name)
        if model is None:
            model = RegisteredModel(name=name)
            self.session.add(model)
            await self.session.flush()
        return model

    async def publish(
        self, *, name: str, artifact_key: str, evaluation_id: UUID | None, alias: str
    ) -> ModelVersion:
        await self.ensure_model(name)
        current = (
            await self.session.execute(
                select(ModelVersion.version).where(ModelVersion.model_name == name)
            )
        ).scalars().all()
        next_version = (max(current) if current else 0) + 1
        row = ModelVersion(
            id=uuid4(),
            model_name=name,
            version=next_version,
            artifact_key=artifact_key,
            evaluation_id=evaluation_id,
        )
        self.session.add(row)
        await self.set_alias(name, alias, next_version)
        await self.session.flush()
        return row

    async def set_alias(self, name: str, alias: str, version: int) -> ModelAlias:
        row = await self.session.get(ModelAlias, (name, alias))
        if row is None:
            row = ModelAlias(model_name=name, alias=alias, version=version)
            self.session.add(row)
        else:
            row.version = version
        await self.session.flush()
        return row

    async def resolve(self, name: str, alias: str) -> ModelVersion | None:
        alias_row = await self.session.get(ModelAlias, (name, alias))
        if alias_row is None:
            return None
        return (
            await self.session.execute(
                select(ModelVersion).where(
                    ModelVersion.model_name == name, ModelVersion.version == alias_row.version
                )
            )
        ).scalar_one_or_none()

    async def list_models(self) -> list[RegisteredModel]:
        return list((await self.session.execute(select(RegisteredModel))).scalars())

    async def get_model(self, name: str) -> tuple[RegisteredModel | None, list[ModelVersion], list[ModelAlias]]:
        model = await self.session.get(RegisteredModel, name)
        versions = list(
            (
                await self.session.execute(
                    select(ModelVersion)
                    .where(ModelVersion.model_name == name)
                    .order_by(ModelVersion.version)
                )
            ).scalars()
        )
        aliases = list(
            (await self.session.execute(select(ModelAlias).where(ModelAlias.model_name == name))).scalars()
        )
        return model, versions, aliases
