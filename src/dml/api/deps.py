from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from dml.config import Settings
from dml.db.session import create_session_factory
from dml.telemetry.metrics import Metrics


def get_settings(request: Request) -> Settings:
    return request.app.state.settings


def get_metrics(request: Request) -> Metrics:
    return request.app.state.metrics


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    factory = request.app.state.session_factory
    async with factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


SettingsDep = Annotated[Settings, Depends(get_settings)]
MetricsDep = Annotated[Metrics, Depends(get_metrics)]
SessionDep = Annotated[AsyncSession, Depends(get_session)]

# Imported by tests that construct apps without Request.app.state.
_ = create_session_factory
