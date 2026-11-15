"""Optional MQTT → Kafka bridge (Phase E).

Default path remains services/iot_edge (simulator).
Enable with Compose profile `mqtt` or DAP_EDGE_MODE=mqtt on this service.

Does not replace iot_edge — run alongside or instead for broker demos.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime, timezone
from uuid import uuid4

from dap_common.config import get_settings
from dap_common.kafka_bus import build_producer, publish
from dap_common.logging_setup import setup_logging
from dap_common.metrics import maybe_start_metrics_server
from dap_common.topics import EvSensorReading, GeoPoint, Topics

log = logging.getLogger("dap.mqtt_bridge")

TOPIC_RE = re.compile(r"^fleet/(?P<fleet>[^/]+)/vehicle/(?P<vehicle>[^/]+)/telemetry$")


def _to_reading(fleet_id: str, vehicle_id: str, body: dict) -> EvSensorReading:
    loc = body.get("location")
    location = None
    if isinstance(loc, dict) and "lat" in loc and "lon" in loc:
        location = GeoPoint(lat=float(loc["lat"]), lon=float(loc["lon"]))
    return EvSensorReading(
        event_id=uuid4(),
        event_time=datetime.now(timezone.utc),
        fleet_id=fleet_id,
        vehicle_id=vehicle_id,
        vin=body.get("vin"),
        speed_kph=float(body.get("speed_kph", 0)),
        soc_percent=float(body.get("soc_percent", 50)),
        soh_percent=body.get("soh_percent"),
        battery_temp_c=body.get("battery_temp_c"),
        motor_temp_c=body.get("motor_temp_c"),
        odometer_km=float(body.get("odometer_km", 0)),
        range_remaining_km=body.get("range_remaining_km"),
        charging_state=body.get("charging_state") or "unknown",
        power_kw=body.get("power_kw"),
        location=location,
        trip_id=body.get("trip_id"),
        driver_id=body.get("driver_id"),
        note=body.get("note"),
        alert_code=body.get("alert_code"),
        alert_severity=body.get("alert_severity") or "none",
        alert_message=body.get("alert_message"),
    )


async def run() -> None:
    settings = get_settings()
    settings.service_name = "mqtt-bridge"
    setup_logging(settings)
    maybe_start_metrics_server(settings)

    try:
        from aiomqtt import Client
    except ImportError as exc:
        raise SystemExit(
            "aiomqtt required for mqtt-bridge. Install: pip install 'dap-common[mqtt]'"
        ) from exc

    producer = await build_producer(settings)
    log.info(
        "mqtt_bridge_listening broker=%s:%s topic=%s",
        settings.mqtt_broker_host,
        settings.mqtt_broker_port,
        settings.mqtt_telemetry_topic,
    )

    try:
        async with Client(
            hostname=settings.mqtt_broker_host,
            port=settings.mqtt_broker_port,
        ) as client:
            await client.subscribe(settings.mqtt_telemetry_topic)
            async for message in client.messages:
                topic = str(message.topic)
                m = TOPIC_RE.match(topic)
                if not m:
                    log.warning("mqtt_topic_skip topic=%s", topic)
                    continue
                try:
                    body = json.loads(message.payload.decode("utf-8"))
                except json.JSONDecodeError:
                    log.warning("mqtt_bad_json topic=%s", topic)
                    continue
                reading = _to_reading(m.group("fleet"), m.group("vehicle"), body)
                await publish(
                    producer,
                    Topics.IOT_TELEMETRY,
                    reading,
                    key=reading.vehicle_id,
                    service_name="mqtt-bridge",
                )
                log.info("mqtt_forwarded vehicle=%s", reading.vehicle_id)
    finally:
        await producer.stop()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
