"""SQL schema bootstrap for EV fleet telematics."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

SCHEMA_SQL = """
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS fleets (
  id VARCHAR(64) PRIMARY KEY,
  name VARCHAR(256) NOT NULL,
  classification VARCHAR(32) NOT NULL DEFAULT 'internal',
  pii_policy VARCHAR(16) NOT NULL DEFAULT 'redact',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS vehicles (
  id VARCHAR(64) PRIMARY KEY,
  fleet_id VARCHAR(64) NOT NULL REFERENCES fleets(id),
  vin VARCHAR(32),
  model VARCHAR(128),
  status VARCHAR(32) NOT NULL DEFAULT 'provisioned',
  device_token VARCHAR(128),
  plugin_protocol VARCHAR(32) NOT NULL DEFAULT 'mqtt',
  activated_at TIMESTAMPTZ,
  last_seen_at TIMESTAMPTZ,
  last_soc_percent DOUBLE PRECISION,
  last_speed_kph DOUBLE PRECISION,
  last_lat DOUBLE PRECISION,
  last_lon DOUBLE PRECISION,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS telematics_events (
  id UUID PRIMARY KEY,
  event_id UUID NOT NULL UNIQUE,
  event_time TIMESTAMPTZ NOT NULL,
  fleet_id VARCHAR(64) NOT NULL,
  vehicle_id VARCHAR(64) NOT NULL,
  speed_kph DOUBLE PRECISION NOT NULL,
  soc_percent DOUBLE PRECISION NOT NULL,
  soh_percent DOUBLE PRECISION,
  battery_temp_c DOUBLE PRECISION,
  motor_temp_c DOUBLE PRECISION,
  odometer_km DOUBLE PRECISION NOT NULL,
  range_remaining_km DOUBLE PRECISION,
  charging_state VARCHAR(32) NOT NULL DEFAULT 'unknown',
  power_kw DOUBLE PRECISION,
  lat DOUBLE PRECISION,
  lon DOUBLE PRECISION,
  trip_id VARCHAR(64),
  driver_id VARCHAR(64),
  alert_code VARCHAR(64),
  alert_severity VARCHAR(16) NOT NULL DEFAULT 'none',
  note TEXT,
  alert_message TEXT,
  pii_redacted BOOLEAN NOT NULL DEFAULT false,
  pii_categories JSONB NOT NULL DEFAULT '[]'::jsonb,
  raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS alert_documents (
  id UUID PRIMARY KEY,
  event_id UUID NOT NULL,
  fleet_id VARCHAR(64) NOT NULL,
  vehicle_id VARCHAR(64) NOT NULL,
  content_hash VARCHAR(64) NOT NULL UNIQUE,
  text TEXT NOT NULL,
  severity VARCHAR(16) NOT NULL DEFAULT 'info',
  pii_redacted BOOLEAN NOT NULL DEFAULT false,
  pii_categories JSONB NOT NULL DEFAULT '[]'::jsonb,
  metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  embedded BOOLEAN NOT NULL DEFAULT false,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS alert_embeddings (
  id UUID PRIMARY KEY,
  content_hash VARCHAR(64) NOT NULL,
  embedding_model VARCHAR(256) NOT NULL,
  embedding_model_version VARCHAR(32) NOT NULL,
  embedding vector(384) NOT NULL,
  dim INT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (content_hash, embedding_model, embedding_model_version)
);

CREATE TABLE IF NOT EXISTS fleet_pipeline_stats (
  fleet_id VARCHAR(64) PRIMARY KEY,
  events_ingested INT NOT NULL DEFAULT 0,
  events_cleaned INT NOT NULL DEFAULT 0,
  alerts_indexed INT NOT NULL DEFAULT 0,
  embeddings_ready INT NOT NULL DEFAULT 0,
  last_event_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS audit_events (
  id UUID PRIMARY KEY,
  actor VARCHAR(128) NOT NULL,
  action VARCHAR(64) NOT NULL,
  resource_type VARCHAR(64) NOT NULL,
  resource_id VARCHAR(64),
  detail JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_telematics_vehicle_time ON telematics_events(vehicle_id, event_time DESC);
CREATE INDEX IF NOT EXISTS ix_telematics_fleet_time ON telematics_events(fleet_id, event_time DESC);
CREATE INDEX IF NOT EXISTS ix_alerts_fleet ON alert_documents(fleet_id, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_vehicles_fleet ON vehicles(fleet_id);
"""


async def apply_schema(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        for stmt in SCHEMA_SQL.split(";"):
            piece = stmt.strip()
            if piece:
                await conn.execute(text(piece))
