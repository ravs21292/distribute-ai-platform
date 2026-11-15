"""Index service: refresh fleet embedding coverage stats after alert embeds."""

from __future__ import annotations

import asyncio
import logging

from sqlalchemy import text

from dap_common.config import get_settings
from dap_common.db import make_session_factory
from dap_common.kafka_bus import build_consumer, build_producer, consume_loop
from dap_common.logging_setup import setup_logging
from dap_common.metrics import maybe_start_metrics_server
from dap_common.models import FleetPipelineStats
from dap_common.topics import IndexCheckMessage, Topics

log = logging.getLogger("dap.index")


async def handle_index(payload: dict, *, sessions) -> None:
    msg = IndexCheckMessage.model_validate(payload)
    async with sessions() as session:
        stats = await session.get(FleetPipelineStats, msg.fleet_id)
        if stats is None:
            stats = FleetPipelineStats(fleet_id=msg.fleet_id)
            session.add(stats)
            await session.flush()
        row = (
            await session.execute(
                text(
                    """
                    SELECT
                      (SELECT COUNT(*) FROM telematics_events WHERE fleet_id = :fid) AS events,
                      (SELECT COUNT(*) FROM alert_documents WHERE fleet_id = :fid) AS alerts,
                      (SELECT COUNT(*) FROM alert_documents d
                         JOIN alert_embeddings e ON e.content_hash = d.content_hash
                        WHERE d.fleet_id = :fid) AS embeds
                    """
                ),
                {"fid": msg.fleet_id},
            )
        ).one()
        stats.events_cleaned = int(row[0] or 0)
        stats.alerts_indexed = int(row[1] or 0)
        stats.embeddings_ready = int(row[2] or 0)
        await session.commit()
        log.info(
            "index_stats fleet=%s events=%s alerts=%s embeds=%s",
            msg.fleet_id,
            stats.events_cleaned,
            stats.alerts_indexed,
            stats.embeddings_ready,
        )


async def run() -> None:
    settings = get_settings()
    settings.service_name = "index"
    setup_logging(settings)
    maybe_start_metrics_server(settings)
    sessions = make_session_factory(settings)
    producer = await build_producer(settings)
    consumer = await build_consumer(
        settings, topic=Topics.INDEX_CHECKS, group_id=settings.consumer_group_index
    )

    async def _handler(payload: dict) -> None:
        await handle_index(payload, sessions=sessions)

    try:
        await consume_loop(
            consumer,
            _handler,
            dlq_producer=producer,
            dlq_topic=Topics.DLQ,
            service_name="index",
        )
    finally:
        await consumer.stop()
        await producer.stop()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
