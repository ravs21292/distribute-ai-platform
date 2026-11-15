"""Embed service: vectorize EV alert narratives into pgvector."""

from __future__ import annotations

import asyncio
import logging
from uuid import uuid4

from sqlalchemy import select, text

from dap_common.config import get_settings
from dap_common.db import make_session_factory
from dap_common.embeddings import build_embedding_provider, vector_literal
from dap_common.kafka_bus import build_consumer, build_producer, consume_loop, publish
from dap_common.logging_setup import setup_logging
from dap_common.metrics import maybe_start_metrics_server
from dap_common.models import AlertDocument, FleetPipelineStats
from dap_common.topics import EmbedAlertMessage, IndexCheckMessage, Topics

log = logging.getLogger("dap.embed")


async def handle_embed(payload: dict, *, sessions, producer, settings) -> None:
    msg = EmbedAlertMessage.model_validate(payload)
    provider = build_embedding_provider(settings)

    async with sessions() as session:
        exists = await session.scalar(
            text(
                """
                SELECT 1 FROM alert_embeddings
                WHERE content_hash = :h
                  AND embedding_model = :m
                  AND embedding_model_version = :v
                LIMIT 1
                """
            ),
            {"h": msg.content_hash, "m": provider.model, "v": provider.model_version},
        )
        if not exists:
            vec = provider.embed_batch([msg.text])[0]
            await session.execute(
                text(
                    """
                    INSERT INTO alert_embeddings
                      (id, content_hash, embedding_model, embedding_model_version, embedding, dim)
                    VALUES
                      (:id, :h, :m, :v, CAST(:emb AS vector), :dim)
                    ON CONFLICT (content_hash, embedding_model, embedding_model_version)
                    DO NOTHING
                    """
                ),
                {
                    "id": str(uuid4()),
                    "h": msg.content_hash,
                    "m": provider.model,
                    "v": provider.model_version,
                    "emb": vector_literal(vec),
                    "dim": provider.dim,
                },
            )

        doc = await session.scalar(
            select(AlertDocument).where(AlertDocument.content_hash == msg.content_hash)
        )
        if doc is not None:
            doc.embedded = True

        stats = await session.get(FleetPipelineStats, msg.fleet_id)
        if stats is not None:
            count = await session.scalar(
                text(
                    """
                    SELECT COUNT(*) FROM alert_documents d
                    JOIN alert_embeddings e ON e.content_hash = d.content_hash
                    WHERE d.fleet_id = :fid
                    """
                ),
                {"fid": msg.fleet_id},
            )
            stats.embeddings_ready = int(count or 0)
        await session.commit()

    await publish(
        producer,
        Topics.INDEX_CHECKS,
        IndexCheckMessage(fleet_id=msg.fleet_id, reason="alert_embedded", alert_id=msg.alert_id),
        key=msg.vehicle_id,
    )
    log.info("embed_complete alert=%s vehicle=%s", msg.alert_id, msg.vehicle_id)


async def run() -> None:
    settings = get_settings()
    settings.service_name = "embed"
    setup_logging(settings)
    maybe_start_metrics_server(settings)
    sessions = make_session_factory(settings)
    producer = await build_producer(settings)
    consumer = await build_consumer(
        settings, topic=Topics.EMBED_ALERTS, group_id=settings.consumer_group_embed
    )

    async def _handler(payload: dict) -> None:
        await handle_embed(payload, sessions=sessions, producer=producer, settings=settings)

    try:
        await consume_loop(
            consumer,
            _handler,
            dlq_producer=producer,
            dlq_topic=Topics.DLQ,
            service_name="embed",
        )
    finally:
        await consumer.stop()
        await producer.stop()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
