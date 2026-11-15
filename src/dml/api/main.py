from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from dml.api.routes import (
    audit,
    datasets,
    health,
    jobs,
    models,
    ops,
    pipelines,
    search,
    tasks,
    ui,
    workers,
)
from dml.config import Settings, get_settings
from dml.db.session import create_engine, create_session_factory
from dml.queue.redis_streams import RedisTaskQueue
from dml.security.rbac import parse_api_keys, resolve_principal
from dml.storage import build_object_store
from dml.telemetry.logging import configure_logging, get_logger
from dml.telemetry.metrics import Metrics

log = get_logger("dml.api")

_OPEN_PREFIXES = ("/health", "/metrics", "/docs", "/openapi", "/redoc", "/v1/ops/ws")
_OPEN_PATHS = {"/", "/ui"}


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings: Settings = app.state.settings
    configure_logging(settings)
    engine = create_engine(settings)
    app.state.engine = engine
    app.state.session_factory = create_session_factory(engine)
    app.state.queue = RedisTaskQueue.from_settings(settings)
    store = build_object_store(settings)
    store.ensure_ready()
    app.state.object_store = store
    try:
        app.state.api_keys = parse_api_keys(settings.api_keys)
    except Exception as exc:
        raise RuntimeError(f"invalid DML_API_KEYS: {exc}") from exc
    log.info(
        "api_started",
        env=settings.env,
        auth_keys=len(app.state.api_keys),
        auth_required=settings.auth_required,
        pii_redaction=settings.pii_redaction_enabled,
    )
    try:
        yield
    finally:
        await app.state.queue.close()
        await engine.dispose()
        log.info("api_stopped")


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    app = FastAPI(
        title="Distributed AI Execution Platform",
        version=settings.version,
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.state.metrics = Metrics()
    app.state.api_keys = parse_api_keys(settings.api_keys) if settings.api_keys else {}
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.middleware("http")
    async def rbac_guard(request: Request, call_next):
        path = request.url.path
        if path in _OPEN_PATHS or path.startswith(_OPEN_PREFIXES):
            request.state.principal = resolve_principal(
                header_key=None,
                api_keys={},
                legacy_key=None,
                auth_required=False,
            )
            return await call_next(request)
        principal = resolve_principal(
            header_key=request.headers.get("X-API-Key"),
            api_keys=getattr(request.app.state, "api_keys", {}) or {},
            legacy_key=settings.api_key,
            auth_required=settings.auth_required,
        )
        if principal is None:
            return JSONResponse({"detail": "unauthorized"}, status_code=401)
        request.state.principal = principal
        return await call_next(request)

    app.include_router(ui.router)
    app.include_router(health.router)
    app.include_router(jobs.router)
    app.include_router(tasks.router)
    app.include_router(workers.router)
    app.include_router(datasets.router)
    app.include_router(pipelines.router)
    app.include_router(search.router)
    app.include_router(models.router)
    app.include_router(ops.router)
    app.include_router(audit.router)
    return app


app = create_app()


def run() -> None:
    import uvicorn

    settings = get_settings()
    configure_logging(settings)
    uvicorn.run(
        "dml.api.main:app",
        host=settings.api_host,
        port=settings.api_port,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    run()
