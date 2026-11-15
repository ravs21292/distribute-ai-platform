"""ETL service: clean EV telematics, redact PII in notes/alerts, store events, emit embed jobs."""

from __future__ import annotations

import asyncio
import logging
from uuid import uuid4

from sqlalchemy import select

from dap_common.config import get_settings
from dap_common.db import make_session_factory
from dap_common.kafka_bus import build_consumer, build_producer, consume_loop, publish
from dap_common.logging_setup import setup_logging
from dap_common.metrics import maybe_start_metrics_server
from dap_common.models import AlertDocument, FleetPipelineStats, TelematicsEvent
from dap_common.pii import scan_and_redact
from dap_common.textutil import content_hash
from dap_common.topics import EmbedAlertMessage, EtlEventMessage, Topics

log = logging.getLogger("dap.etl")


def _compose_alert_text(event, note: str | None, alert_message: str | None) -> str:
    parts = [
        f"Fleet {event.fleet_id} vehicle {event.vehicle_id}",
        f"alert={event.alert_code} severity={event.alert_severity}",
        f"soc={event.soc_percent}% speed={event.speed_kph}kph charging={event.charging_state}",
    ]
    if event.location:
        parts.append(f"location={event.location.lat},{event.location.lon}")
    if alert_message:
        parts.append(alert_message)
    if note:
        parts.append(note)
    return ". ".join(parts)


async def handle_etl(payload: dict, *, sessions, producer, pii_enabled: bool) -> None:
    msg = EtlEventMessage.model_validate(payload)
    event = msg.event
    policy = (event.pii_policy or "redact").lower()
    enabled = pii_enabled and policy != "allow"

    note = event.note or ""
    alert_message = event.alert_message or ""
    categories: list[str] = []
    redacted = False

    if note or alert_message:
        joined = f"{note}\n{alert_message}".strip()
        pii = scan_and_redact(joined, enabled=enabled)
        if pii.categories and policy == "reject":
            raise RuntimeError(f"pii_rejected categories={pii.categories}")
        if policy == "redact" and pii.redacted:
            # Apply redaction separately to keep fields aligned
            note_pii = scan_and_redact(note, enabled=True)
            alert_pii = scan_and_redact(alert_message, enabled=True)
            note = note_pii.text
            alert_message = alert_pii.text
            categories = sorted(set(note_pii.categories + alert_pii.categories))
            redacted = True
        elif not enabled:
            categories = []
            redacted = False

    async with sessions() as session:
        existing = await session.scalar(
            select(TelematicsEvent.id).where(TelematicsEvent.event_id == event.event_id)
        )
        if existing:
            return

        row = TelematicsEvent(
            id=uuid4(),
            event_id=event.event_id,
            event_time=event.event_time,
            fleet_id=event.fleet_id,
            vehicle_id=event.vehicle_id,
            speed_kph=event.speed_kph,
            soc_percent=event.soc_percent,
            soh_percent=event.soh_percent,
            battery_temp_c=event.battery_temp_c,
            motor_temp_c=event.motor_temp_c,
            odometer_km=event.odometer_km,
            range_remaining_km=event.range_remaining_km,
            charging_state=event.charging_state,
            power_kw=event.power_kw,
            lat=event.location.lat if event.location else None,
            lon=event.location.lon if event.location else None,
            trip_id=event.trip_id,
            driver_id=event.driver_id,
            alert_code=event.alert_code,
            alert_severity=event.alert_severity,
            note=note or None,
            alert_message=alert_message or None,
            pii_redacted=redacted,
            pii_categories=categories,
            raw_payload=event.model_dump(mode="json"),
        )
        session.add(row)

        stats = await session.get(FleetPipelineStats, event.fleet_id)
        if stats is None:
            stats = FleetPipelineStats(fleet_id=event.fleet_id)
            session.add(stats)
            await session.flush()
        stats.events_cleaned = (stats.events_cleaned or 0) + 1
        stats.last_event_at = event.event_time

        embed_msg: EmbedAlertMessage | None = None
        if event.alert_severity and event.alert_severity != "none" and event.alert_code:
            text = _compose_alert_text(event, note, alert_message)
            digest = content_hash(text)
            prior = await session.scalar(
                select(AlertDocument.id).where(AlertDocument.content_hash == digest)
            )
            alert_id = uuid4()
            if prior is None:
                session.add(
                    AlertDocument(
                        id=alert_id,
                        event_id=event.event_id,
                        fleet_id=event.fleet_id,
                        vehicle_id=event.vehicle_id,
                        content_hash=digest,
                        text=text,
                        severity=event.alert_severity,
                        pii_redacted=redacted,
                        pii_categories=categories,
                        metadata_json={
                            "alert_code": event.alert_code,
                            "soc_percent": event.soc_percent,
                            "charging_state": event.charging_state,
                        },
                        embedded=False,
                    )
                )
                stats.alerts_indexed = (stats.alerts_indexed or 0) + 1
                embed_msg = EmbedAlertMessage(
                    alert_id=alert_id,
                    fleet_id=event.fleet_id,
                    vehicle_id=event.vehicle_id,
                    event_id=event.event_id,
                    content_hash=digest,
                    severity=event.alert_severity,
                    text=text,
                    pii_redacted=redacted,
                    pii_categories=categories,
                    metadata={
                        "alert_code": event.alert_code,
                        "vehicle_id": event.vehicle_id,
                    },
                )
        await session.commit()

    if embed_msg is not None:
        await publish(producer, Topics.EMBED_ALERTS, embed_msg, key=event.vehicle_id)

    log.info(
        "etl_complete vehicle=%s event=%s pii=%s alert=%s",
        event.vehicle_id,
        event.event_id,
        redacted,
        event.alert_code or "-",
    )


async def run() -> None:
    settings = get_settings()
    settings.service_name = "etl"
    setup_logging(settings)
    maybe_start_metrics_server(settings)
    sessions = make_session_factory(settings)
    producer = await build_producer(settings)
    consumer = await build_consumer(
        settings, topic=Topics.ETL_EVENTS, group_id=settings.consumer_group_etl
    )

    async def _handler(payload: dict) -> None:
        await handle_etl(
            payload,
            sessions=sessions,
            producer=producer,
            pii_enabled=settings.pii_redaction_enabled,
        )

    try:
        await consume_loop(
            consumer,
            _handler,
            dlq_producer=producer,
            dlq_topic=Topics.DLQ,
            service_name="etl",
        )
    finally:
        await consumer.stop()
        await producer.stop()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
