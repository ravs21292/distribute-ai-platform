"""Ingest service: consume live EV telemetry from Kafka, register fleet/vehicle, forward to ETL."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from dap_common.config import get_settings
from dap_common.db import make_session_factory
from dap_common.kafka_bus import build_consumer, build_producer, consume_loop, publish
from dap_common.logging_setup import setup_logging
from dap_common.metrics import maybe_start_metrics_server
from dap_common.models import Fleet, FleetPipelineStats, Vehicle
from dap_common.topics import EtlEventMessage, EvSensorReading, Topics

log = logging.getLogger("dap.ingest")


async def handle_telemetry(payload: dict, *, sessions, producer) -> None:
    event = EvSensorReading.model_validate(payload)
    now = datetime.now(timezone.utc)

    async with sessions() as session:
        fleet = await session.get(Fleet, event.fleet_id)
        if fleet is None:
            session.add(
                Fleet(
                    id=event.fleet_id,
                    name=event.fleet_id,
                    classification=event.classification,
                    pii_policy=event.pii_policy,
                )
            )
        vehicle = await session.get(Vehicle, event.vehicle_id)
        if vehicle is None:
            # Telemetry for unknown vehicle is rejected; must provision + activate first.
            raise RuntimeError(f"vehicle_not_provisioned:{event.vehicle_id}")
        if vehicle.status not in {"streaming", "provisioned"}:
            # Allow brief race after activate; ignore paused/revoked.
            if vehicle.status == "paused":
                return
        vehicle.last_seen_at = event.event_time
        vehicle.last_soc_percent = event.soc_percent
        vehicle.last_speed_kph = event.speed_kph
        if event.location:
            vehicle.last_lat = event.location.lat
            vehicle.last_lon = event.location.lon
        if event.vin:
            vehicle.vin = event.vin
        if vehicle.status == "provisioned":
            vehicle.status = "streaming"

        stats = await session.get(FleetPipelineStats, event.fleet_id)
        if stats is None:
            stats = FleetPipelineStats(fleet_id=event.fleet_id, events_ingested=0)
            session.add(stats)
            await session.flush()
        stats.events_ingested = (stats.events_ingested or 0) + 1
        stats.last_event_at = event.event_time
        await session.commit()

    await publish(
        producer,
        Topics.ETL_EVENTS,
        EtlEventMessage(event=event, ingested_at=now, ingest_service="ingest"),
        key=event.vehicle_id,
    )
    log.info(
        "telemetry_accepted vehicle=%s soc=%.1f alert=%s",
        event.vehicle_id,
        event.soc_percent,
        event.alert_code or "-",
    )


async def run() -> None:
    settings = get_settings()
    settings.service_name = "ingest"
    setup_logging(settings)
    maybe_start_metrics_server(settings)
    sessions = make_session_factory(settings)
    producer = await build_producer(settings)
    consumer = await build_consumer(
        settings, topic=Topics.IOT_TELEMETRY, group_id=settings.consumer_group_ingest
    )

    async def _handler(payload: dict) -> None:
        await handle_telemetry(payload, sessions=sessions, producer=producer)

    try:
        await consume_loop(
            consumer,
            _handler,
            dlq_producer=producer,
            dlq_topic=Topics.DLQ,
            service_name="ingest",
        )
    finally:
        await consumer.stop()
        await producer.stop()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
