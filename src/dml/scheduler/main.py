from __future__ import annotations

import asyncio
import signal

from dml.config import get_settings
from dml.db.session import create_engine, create_session_factory
from dml.queue.redis_streams import RedisTaskQueue
from dml.scheduler.service import Scheduler
from dml.telemetry.logging import configure_logging, get_logger
from dml.telemetry.metrics import Metrics

log = get_logger("dml.scheduler.main")


async def _amain() -> None:
    settings = get_settings()
    configure_logging(settings)
    metrics = Metrics()
    try:
        metrics.serve(settings.scheduler_metrics_port)
    except OSError as exc:
        log.warning("metrics_port_unavailable", error=str(exc))

    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    queue = RedisTaskQueue.from_settings(settings)
    scheduler = Scheduler(
        settings=settings,
        session_factory=session_factory,
        queue=queue,
        metrics=metrics,
    )

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, scheduler.request_stop)
        except NotImplementedError:
            pass

    try:
        await scheduler.run_forever()
    finally:
        await queue.close()
        await engine.dispose()


def run() -> None:
    try:
        asyncio.run(_amain())
    except KeyboardInterrupt:
        return


if __name__ == "__main__":
    run()
