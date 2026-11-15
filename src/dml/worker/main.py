"""CLI: python -m dml.worker --capabilities ingest,etl --device cpu"""

from __future__ import annotations

import argparse
import asyncio
import os
import socket
import uuid

from dml.config import get_settings, parse_capabilities
from dml.db.session import create_engine, create_session_factory
from dml.queue.redis_streams import RedisTaskQueue
from dml.telemetry.logging import configure_logging, get_logger
from dml.telemetry.metrics import Metrics
from dml.ml.embeddings import build_embedding_provider
from dml.storage import build_object_store
from dml.worker.handlers.artifact import ArtifactHandler
from dml.worker.handlers.dedupe import DedupeHandler
from dml.worker.handlers.dummy import DummyHandler
from dml.worker.handlers.embedding import EmbeddingHandler
from dml.worker.handlers.etl import ETLHandler
from dml.worker.handlers.evaluation import EvaluationHandler
from dml.worker.handlers.indexing import IndexHandler
from dml.worker.handlers.ingest import IngestHandler
from dml.worker.handlers.training import TrainingHandler
from dml.worker.resources import detect_resources
from dml.worker.runtime import WorkerRuntime

log = get_logger("dml.worker.main")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="DML worker runtime")
    parser.add_argument(
        "--capabilities",
        default=os.environ.get("DML_WORKER_CAPABILITIES", "ingest,etl"),
        help="Comma-separated handler names this process will execute (e.g. ingest,etl,dedupe,index)",
    )
    parser.add_argument(
        "--device",
        default=os.environ.get("DML_WORKER_DEVICE", "cpu"),
        choices=["cpu", "cuda", "auto"],
    )
    parser.add_argument("--worker-id", default=os.environ.get("DML_WORKER_ID"))
    parser.add_argument(
        "--metrics-port",
        type=int,
        default=int(os.environ.get("DML_WORKER_METRICS_PORT", "9100")),
    )
    return parser


def resolve_worker_id(explicit: str | None) -> str:
    if explicit:
        return explicit
    return f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:8]}"


def handler_map(capabilities: list[str], store, settings) -> dict:
    provider = build_embedding_provider(settings)
    available = {
        DummyHandler.task_type: DummyHandler(),
        IngestHandler.task_type: IngestHandler(store),
        ETLHandler.task_type: ETLHandler(
            store, pii_redaction_enabled=settings.pii_redaction_enabled
        ),
        DedupeHandler.task_type: DedupeHandler(),
        EmbeddingHandler.task_type: EmbeddingHandler(provider),
        IndexHandler.task_type: IndexHandler(),
        TrainingHandler.task_type: TrainingHandler(store, settings),
        EvaluationHandler.task_type: EvaluationHandler(store, settings),
        ArtifactHandler.task_type: ArtifactHandler(),
    }
    missing = [cap for cap in capabilities if cap not in available]
    if missing:
        raise SystemExit(f"unsupported capabilities for this build: {missing}")
    return {cap: available[cap] for cap in capabilities}


async def _amain(args: argparse.Namespace) -> None:
    settings = get_settings()
    configure_logging(settings)
    capabilities = parse_capabilities(args.capabilities)
    if not capabilities:
        raise SystemExit("at least one capability is required")
    resources = detect_resources(device=args.device)
    metrics = Metrics()
    try:
        metrics.serve(args.metrics_port)
    except OSError as exc:
        log.warning("metrics_port_unavailable", port=args.metrics_port, error=str(exc))

    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    queue = RedisTaskQueue.from_settings(settings)
    store = build_object_store(settings)
    store.ensure_ready()
    runtime = WorkerRuntime(
        settings=settings,
        session_factory=session_factory,
        queue=queue,
        handlers=handler_map(capabilities, store, settings),
        resources=resources,
        metrics=metrics,
        worker_id=resolve_worker_id(args.worker_id),
        capabilities=capabilities,
    )
    try:
        await runtime.run()
    finally:
        await queue.close()
        await engine.dispose()


def run() -> None:
    args = build_parser().parse_args()
    try:
        asyncio.run(_amain(args))
    except KeyboardInterrupt:
        return


if __name__ == "__main__":
    run()
