"""IoT edge gateway: streams ONLY activated (onboarded) vehicles into Kafka.

Listens to dap.edge.commands (activate/deactivate) and also refreshes from Postgres
so capture starts after the console clicks Connect.
"""

from __future__ import annotations

import asyncio
import logging
import math
import random
from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy import select

from dap_common.config import get_settings
from dap_common.db import make_session_factory
from dap_common.kafka_bus import build_consumer, build_producer, publish
from dap_common.logging_setup import setup_logging
from dap_common.models import Vehicle
from dap_common.topics import EdgeCommand, EvSensorReading, GeoPoint, Topics

log = logging.getLogger("dap.iot_edge")

ALERTS = [
    ("BATTERY_HIGH_TEMP", "critical", "Battery temperature elevated; thermal derate engaged."),
    ("LOW_SOC", "warning", "State of charge below fleet threshold; recommend charge stop."),
    ("RANGE_ANOMALY", "warning", "Range estimate dropped faster than route model predicted."),
    ("CHARGE_FAULT", "critical", "DC fast charge session aborted; contact depot ops."),
    ("GEOFENCE_EXIT", "info", "Vehicle exited approved service region."),
]


def _sim_state(vehicle_id: str, vin: str | None, model: str | None) -> dict:
    return {
        "vehicle_id": vehicle_id,
        "vin": vin or f"1EV{vehicle_id[-6:]}",
        "model": model or "Sedan-E",
        "base_lat": 40.71 + random.uniform(-0.2, 0.2),
        "base_lon": -74.00 + random.uniform(-0.2, 0.2),
        "soc": random.uniform(35, 95),
        "odometer": random.uniform(1200, 88000),
        "angle": random.uniform(0, 6.28),
    }


def _sample(vehicle: dict, fleet_id: str, alert_p: float) -> EvSensorReading:
    vehicle["angle"] += random.uniform(0.01, 0.08)
    vehicle["soc"] = max(5.0, min(100.0, vehicle["soc"] + random.uniform(-1.2, 0.4)))
    vehicle["odometer"] += random.uniform(0.02, 0.35)
    speed = max(0.0, abs(math.sin(vehicle["angle"])) * random.uniform(15, 95))
    charging = "discharging"
    if vehicle["soc"] < 20 and random.random() < 0.4:
        charging = "fast_charging"
        speed = 0.0
        vehicle["soc"] = min(100.0, vehicle["soc"] + random.uniform(0.5, 2.5))
    elif speed < 1:
        charging = "idle"

    lat = vehicle["base_lat"] + 0.01 * math.sin(vehicle["angle"])
    lon = vehicle["base_lon"] + 0.01 * math.cos(vehicle["angle"])
    batt_temp = 22 + (100 - vehicle["soc"]) * 0.05 + random.uniform(-1, 4)

    alert_code = None
    severity = "none"
    alert_message = None
    note = None
    if random.random() < alert_p:
        alert_code, severity, alert_message = random.choice(ALERTS)
        if random.random() < 0.45:
            note = f"Driver callback alice.driver@fleet.example or +1-415-555-0198 regarding {alert_code}."
        else:
            note = f"Ops ticket opened for {alert_code}."

    return EvSensorReading(
        event_id=uuid4(),
        event_time=datetime.now(timezone.utc),
        fleet_id=fleet_id,
        vehicle_id=vehicle["vehicle_id"],
        vin=vehicle["vin"],
        speed_kph=round(speed, 2),
        soc_percent=round(vehicle["soc"], 2),
        soh_percent=round(random.uniform(88, 99), 2),
        battery_temp_c=round(batt_temp, 2),
        motor_temp_c=round(35 + speed * 0.15 + random.uniform(-2, 3), 2),
        odometer_km=round(vehicle["odometer"], 2),
        range_remaining_km=round(vehicle["soc"] * random.uniform(2.8, 3.4), 1),
        charging_state=charging,  # type: ignore[arg-type]
        power_kw=round((-speed * 0.2) if charging == "discharging" else random.uniform(20, 120), 2),
        location=GeoPoint(lat=round(lat, 6), lon=round(lon, 6)),
        trip_id=f"trip-{vehicle['vehicle_id']}-{datetime.now(timezone.utc):%Y%m%d}",
        driver_id=f"DRV-{random.randint(100, 199)}",
        note=note,
        alert_code=alert_code,
        alert_severity=severity,  # type: ignore[arg-type]
        alert_message=alert_message,
        classification="internal",
        pii_policy="redact",
    )


async def _load_streaming(sessions) -> dict[str, dict]:
    async with sessions() as session:
        rows = (
            await session.scalars(select(Vehicle).where(Vehicle.status == "streaming"))
        ).all()
        return {
            v.id: {"fleet_id": v.fleet_id, "vin": v.vin, "model": v.model} for v in rows
        }


async def run() -> None:
    settings = get_settings()
    settings.service_name = "iot-edge"
    setup_logging(settings)
    from dap_common.metrics import maybe_start_metrics_server

    maybe_start_metrics_server(settings)
    sessions = make_session_factory(settings)
    producer = await build_producer(settings)
    consumer = await build_consumer(
        settings, topic=Topics.EDGE_COMMANDS, group_id="dap-iot-edge"
    )

    active: dict[str, dict] = {}  # vehicle_id -> sim state
    meta: dict[str, dict] = {}

    async def refresh_from_db() -> None:
        nonlocal meta
        streaming = await _load_streaming(sessions)
        meta = streaming
        # Drop paused
        for vid in list(active.keys()):
            if vid not in streaming:
                active.pop(vid, None)
                log.info("capture_stopped vehicle=%s", vid)
        # Add newly activated
        for vid, info in streaming.items():
            if vid not in active:
                active[vid] = _sim_state(vid, info.get("vin"), info.get("model"))
                log.info("capture_started vehicle=%s fleet=%s", vid, info["fleet_id"])

    async def command_loop() -> None:
        async for msg in consumer:
            try:
                cmd = EdgeCommand.model_validate(msg.value if isinstance(msg.value, dict) else {})
                if cmd.action == "activate":
                    active[cmd.vehicle_id] = _sim_state(cmd.vehicle_id, None, None)
                    meta[cmd.vehicle_id] = {"fleet_id": cmd.fleet_id, "vin": None, "model": None}
                    log.info("edge_command activate vehicle=%s", cmd.vehicle_id)
                elif cmd.action == "deactivate":
                    active.pop(cmd.vehicle_id, None)
                    meta.pop(cmd.vehicle_id, None)
                    log.info("edge_command deactivate vehicle=%s", cmd.vehicle_id)
                await consumer.commit()
            except Exception:  # noqa: BLE001
                log.exception("edge_command_failed")
                await consumer.commit()

    await refresh_from_db()
    cmd_task = asyncio.create_task(command_loop(), name="edge-commands")
    log.info("iot_edge_ready waiting_for_activated_vehicles")

    try:
        tick = 0
        while True:
            if tick % 5 == 0:
                await refresh_from_db()
            for vid, state in list(active.items()):
                fleet_id = meta.get(vid, {}).get("fleet_id") or settings.iot_fleet_id
                reading = _sample(state, fleet_id, settings.iot_alert_probability)
                await publish(producer, Topics.IOT_TELEMETRY, reading, key=vid)
            tick += 1
            await asyncio.sleep(settings.iot_publish_interval_seconds)
    finally:
        cmd_task.cancel()
        await consumer.stop()
        await producer.stop()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
