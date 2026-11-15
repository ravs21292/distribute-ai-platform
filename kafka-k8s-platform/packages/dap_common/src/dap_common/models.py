"""SQLAlchemy models — EV fleet telematics (Postgres = source of truth)."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID as PGUUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Fleet(Base):
    __tablename__ = "fleets"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(256), nullable=False)
    classification: Mapped[str] = mapped_column(String(32), default="internal")
    pii_policy: Mapped[str] = mapped_column(String(16), default="redact")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class Vehicle(Base):
    __tablename__ = "vehicles"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    fleet_id: Mapped[str] = mapped_column(ForeignKey("fleets.id"), nullable=False)
    vin: Mapped[str | None] = mapped_column(String(32))
    model: Mapped[str | None] = mapped_column(String(128))
    # provisioned | streaming | paused | revoked
    status: Mapped[str] = mapped_column(String(32), default="provisioned")
    device_token: Mapped[str | None] = mapped_column(String(128))
    plugin_protocol: Mapped[str] = mapped_column(String(32), default="mqtt")
    activated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_soc_percent: Mapped[float | None] = mapped_column(Float)
    last_speed_kph: Mapped[float | None] = mapped_column(Float)
    last_lat: Mapped[float | None] = mapped_column(Float)
    last_lon: Mapped[float | None] = mapped_column(Float)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class TelematicsEvent(Base):
    __tablename__ = "telematics_events"
    __table_args__ = (UniqueConstraint("event_id", name="uq_telematics_event_id"),)

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    event_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    fleet_id: Mapped[str] = mapped_column(String(64), nullable=False)
    vehicle_id: Mapped[str] = mapped_column(String(64), nullable=False)
    speed_kph: Mapped[float] = mapped_column(Float, nullable=False)
    soc_percent: Mapped[float] = mapped_column(Float, nullable=False)
    soh_percent: Mapped[float | None] = mapped_column(Float)
    battery_temp_c: Mapped[float | None] = mapped_column(Float)
    motor_temp_c: Mapped[float | None] = mapped_column(Float)
    odometer_km: Mapped[float] = mapped_column(Float, nullable=False)
    range_remaining_km: Mapped[float | None] = mapped_column(Float)
    charging_state: Mapped[str] = mapped_column(String(32), default="unknown")
    power_kw: Mapped[float | None] = mapped_column(Float)
    lat: Mapped[float | None] = mapped_column(Float)
    lon: Mapped[float | None] = mapped_column(Float)
    trip_id: Mapped[str | None] = mapped_column(String(64))
    driver_id: Mapped[str | None] = mapped_column(String(64))
    alert_code: Mapped[str | None] = mapped_column(String(64))
    alert_severity: Mapped[str] = mapped_column(String(16), default="none")
    note: Mapped[str | None] = mapped_column(Text)
    alert_message: Mapped[str | None] = mapped_column(Text)
    pii_redacted: Mapped[bool] = mapped_column(Boolean, default=False)
    pii_categories: Mapped[list[Any]] = mapped_column(JSONB, default=list)
    raw_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class AlertDocument(Base):
    """Searchable alert narrative derived from telematics (embedded in pgvector)."""

    __tablename__ = "alert_documents"
    __table_args__ = (UniqueConstraint("content_hash", name="uq_alert_content_hash"),)

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    event_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    fleet_id: Mapped[str] = mapped_column(String(64), nullable=False)
    vehicle_id: Mapped[str] = mapped_column(String(64), nullable=False)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    text: Mapped[str] = mapped_column(Text, nullable=False)
    severity: Mapped[str] = mapped_column(String(16), default="info")
    pii_redacted: Mapped[bool] = mapped_column(Boolean, default=False)
    pii_categories: Mapped[list[Any]] = mapped_column(JSONB, default=list)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    embedded: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class AlertEmbedding(Base):
    __tablename__ = "alert_embeddings"
    __table_args__ = (
        UniqueConstraint(
            "content_hash",
            "embedding_model",
            "embedding_model_version",
            name="uq_alert_embed_hash_model",
        ),
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    embedding_model: Mapped[str] = mapped_column(String(256), nullable=False)
    embedding_model_version: Mapped[str] = mapped_column(String(32), nullable=False)
    embedding: Mapped[str] = mapped_column(Text, nullable=False)
    dim: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class FleetPipelineStats(Base):
    __tablename__ = "fleet_pipeline_stats"

    fleet_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    events_ingested: Mapped[int] = mapped_column(Integer, default=0)
    events_cleaned: Mapped[int] = mapped_column(Integer, default=0)
    alerts_indexed: Mapped[int] = mapped_column(Integer, default=0)
    embeddings_ready: Mapped[int] = mapped_column(Integer, default=0)
    last_event_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class AuditEvent(Base):
    __tablename__ = "audit_events"

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    actor: Mapped[str] = mapped_column(String(128), nullable=False)
    action: Mapped[str] = mapped_column(String(64), nullable=False)
    resource_type: Mapped[str] = mapped_column(String(64), nullable=False)
    resource_id: Mapped[str | None] = mapped_column(String(64))
    detail: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
