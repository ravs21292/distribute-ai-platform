"""Contract tests for dap_common — protect pipeline contracts without Kafka."""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from dap_common.pii import scan_and_redact
from dap_common.topics import ALL_TOPICS, EvSensorReading, Topics
from dap_common.tracing import TRACE_KEY, ensure_trace_id


def test_topics_stable():
    assert Topics.IOT_TELEMETRY == "dap.iot.telemetry"
    assert Topics.DLQ == "dap.dlq"
    assert len(ALL_TOPICS) == 6


def test_pii_redacts_email_and_phone():
    text = "Contact driver jane@fleet.com at +1-555-0100 about battery"
    result = scan_and_redact(text, enabled=True)
    assert "jane@fleet.com" not in result.text
    assert "+1-555-0100" not in result.text or "REDACTED" in result.text
    assert result.redacted is True


def test_pii_disabled_passthrough():
    text = "email me at a@b.com"
    result = scan_and_redact(text, enabled=False)
    assert result.text == text


def test_ev_sensor_reading_roundtrip():
    reading = EvSensorReading(
        event_id=uuid4(),
        event_time=datetime.now(timezone.utc),
        fleet_id="fleet-east-01",
        vehicle_id="EV-1",
        speed_kph=40.0,
        soc_percent=80.0,
        odometer_km=1000.0,
    )
    data = reading.model_dump(mode="json")
    again = EvSensorReading.model_validate(data)
    assert again.vehicle_id == "EV-1"


def test_trace_id_stamped():
    payload: dict = {}
    tid = ensure_trace_id(payload)
    assert payload[TRACE_KEY] == tid
    assert ensure_trace_id(payload) == tid
