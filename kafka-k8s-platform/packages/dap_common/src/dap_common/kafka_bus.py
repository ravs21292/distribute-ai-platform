"""Kafka producer/consumer helpers with backpressure-aware settings."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError, KafkaError

from dap_common.config import Settings
from dap_common.metrics import Timer, record_consume, record_publish
from dap_common.tracing import ensure_trace_id

log = logging.getLogger(__name__)


def _dumps(value: Any) -> bytes:
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json")
    return json.dumps(value, default=str).encode("utf-8")


def _loads(raw: bytes) -> dict[str, Any]:
    return json.loads(raw.decode("utf-8"))


async def build_producer(settings: Settings) -> AIOKafkaProducer:
    last_exc: Exception | None = None
    for attempt in range(1, settings.kafka_connect_retries + 1):
        producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            client_id=f"{settings.kafka_client_id}-{settings.service_name}",
            acks=settings.kafka_acks,
            linger_ms=settings.kafka_linger_ms,
            enable_idempotence=True,
            value_serializer=_dumps,
            key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
        )
        try:
            await producer.start()
            return producer
        except (KafkaConnectionError, OSError) as exc:
            last_exc = exc
            log.warning(
                "kafka_producer_bootstrap_retry attempt=%s/%s error=%s",
                attempt,
                settings.kafka_connect_retries,
                exc,
            )
            await asyncio.sleep(settings.kafka_connect_retry_seconds)
    raise RuntimeError(f"kafka producer bootstrap failed: {last_exc}")


async def build_consumer(
    settings: Settings,
    *,
    topic: str,
    group_id: str,
) -> AIOKafkaConsumer:
    last_exc: Exception | None = None
    for attempt in range(1, settings.kafka_connect_retries + 1):
        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            client_id=f"{settings.kafka_client_id}-{settings.service_name}",
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            max_poll_records=settings.kafka_max_poll_records,
            session_timeout_ms=settings.kafka_session_timeout_ms,
            value_deserializer=_loads,
        )
        try:
            await consumer.start()
            return consumer
        except (KafkaConnectionError, OSError) as exc:
            last_exc = exc
            log.warning(
                "kafka_consumer_bootstrap_retry attempt=%s/%s error=%s",
                attempt,
                settings.kafka_connect_retries,
                exc,
            )
            await asyncio.sleep(settings.kafka_connect_retry_seconds)
    raise RuntimeError(f"kafka consumer bootstrap failed: {last_exc}")


async def publish(
    producer: AIOKafkaProducer,
    topic: str,
    value: Any,
    *,
    key: str | None = None,
    service_name: str | None = None,
) -> None:
    # Prefer vehicle_id partition key when callers omit key (ordering per vehicle).
    if key is None and hasattr(value, "vehicle_id"):
        key = getattr(value, "vehicle_id", None)
    if key is None and isinstance(value, dict) and value.get("vehicle_id"):
        key = str(value["vehicle_id"])
    if isinstance(value, dict):
        ensure_trace_id(value)
    svc = service_name or "dap"
    try:
        await producer.send_and_wait(topic, value=value, key=key)
        record_publish(svc, topic, "ok")
    except KafkaError:
        record_publish(svc, topic, "error")
        log.exception("kafka_publish_failed", extra={"topic": topic})
        raise


Handler = Callable[[dict[str, Any]], Awaitable[None]]


async def consume_loop(
    consumer: AIOKafkaConsumer,
    handler: Handler,
    *,
    dlq_producer: AIOKafkaProducer | None = None,
    dlq_topic: str | None = None,
    service_name: str = "worker",
) -> None:
    async for msg in consumer:
        payload = msg.value if isinstance(msg.value, dict) else {}
        if isinstance(payload, dict):
            ensure_trace_id(payload)
        timer = Timer()
        topic = msg.topic or "unknown"
        try:
            await handler(payload)
            await consumer.commit()
            record_consume(service_name, topic, "ok", timer.seconds())
        except Exception as exc:  # noqa: BLE001
            record_consume(service_name, topic, "error", timer.seconds())
            log.exception(
                "handler_failed topic=%s partition=%s offset=%s",
                msg.topic,
                msg.partition,
                msg.offset,
            )
            if dlq_producer and dlq_topic:
                await publish(
                    dlq_producer,
                    dlq_topic,
                    {
                        "source_topic": msg.topic,
                        "error_type": type(exc).__name__,
                        "error": str(exc)[:2000],
                        "payload": payload,
                        "service": service_name,
                    },
                    key=str(msg.key) if msg.key else None,
                    service_name=service_name,
                )
                await consumer.commit()
            else:
                raise
