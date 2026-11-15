"""EV fleet telematics contracts and Kafka topic names.

Sensor path (production-shaped):
  Vehicle TCU/ECU sensors → on-board telematics unit → edge gateway
  → Kafka topic dap.iot.telemetry (this platform's intake bus).

Locally, services/iot_edge simulates N vehicles publishing continuously.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field


class Topics(StrEnum):
    IOT_TELEMETRY = "dap.iot.telemetry"
    ETL_EVENTS = "dap.etl.events"
    EMBED_ALERTS = "dap.embed.alerts"
    INDEX_CHECKS = "dap.index.checks"
    EDGE_COMMANDS = "dap.edge.commands"
    DLQ = "dap.dlq"


ALL_TOPICS: tuple[str, ...] = tuple(t.value for t in Topics)


ChargingState = Literal["idle", "charging", "fast_charging", "discharging", "unknown"]
AlertSeverity = Literal["info", "warning", "critical", "none"]


class GeoPoint(BaseModel):
    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)


class EvSensorReading(BaseModel):
    """One telemetry sample from an EV (what the edge publishes)."""

    event_id: UUID
    event_time: datetime
    fleet_id: str
    vehicle_id: str
    vin: str | None = None

    # Drivetrain / battery sensors
    speed_kph: float = Field(ge=0)
    soc_percent: float = Field(ge=0, le=100)
    soh_percent: float | None = Field(default=None, ge=0, le=100)
    battery_temp_c: float | None = None
    motor_temp_c: float | None = None
    odometer_km: float = Field(ge=0)
    range_remaining_km: float | None = Field(default=None, ge=0)
    charging_state: ChargingState = "unknown"
    power_kw: float | None = None

    # Location
    location: GeoPoint | None = None

    # Operational context
    trip_id: str | None = None
    driver_id: str | None = None
    # May contain PII (email/phone) in free text — ETL redacts before store/embed
    note: str | None = None
    alert_code: str | None = None
    alert_severity: AlertSeverity = "none"
    alert_message: str | None = None

    classification: str = "internal"
    pii_policy: str = "redact"
    schema_version: str = "ev-telematics-v1"


class EtlEventMessage(BaseModel):
    """Validated telemetry forwarded to ETL after ingest accept."""

    event: EvSensorReading
    ingested_at: datetime
    ingest_service: str = "ingest"


class EmbedAlertMessage(BaseModel):
    """Alert narrative ready for embedding / semantic search."""

    alert_id: UUID
    fleet_id: str
    vehicle_id: str
    event_id: UUID
    content_hash: str
    severity: AlertSeverity
    text: str
    pii_redacted: bool = False
    pii_categories: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class IndexCheckMessage(BaseModel):
    fleet_id: str
    reason: str = "alert_embedded"
    alert_id: UUID | None = None


class EdgeCommand(BaseModel):
    """Control plane → edge gateway: start/stop continuous capture for a vehicle."""

    action: Literal["activate", "deactivate", "refresh"]
    fleet_id: str
    vehicle_id: str
    device_token: str | None = None
    plugin_protocol: str = "mqtt"
    requested_at: datetime | None = None


class DlqMessage(BaseModel):
    source_topic: str
    error_type: str
    error: str
    payload: dict[str, Any] = Field(default_factory=dict)
    service: str
