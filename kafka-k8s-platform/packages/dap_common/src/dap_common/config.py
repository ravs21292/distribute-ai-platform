"""Environment settings shared by all DAP microservices."""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DAP_", env_file=".env", extra="ignore")

    service_name: str = "dap"
    log_level: str = "INFO"
    log_json: bool = True

    database_url: str = "postgresql+asyncpg://dap:dap@localhost:5432/dap"
    database_url_sync: str = "postgresql+psycopg://dap:dap@localhost:5432/dap"

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_client_id: str = "dap"
    kafka_linger_ms: int = 20
    kafka_acks: str = "all"
    kafka_max_in_flight: int = 5
    kafka_max_poll_records: int = 32
    kafka_session_timeout_ms: int = 45_000
    kafka_connect_retries: int = 30
    kafka_connect_retry_seconds: float = 2.0

    s3_endpoint_url: str = "http://localhost:9000"
    s3_access_key: str = "dapminio"
    s3_secret_key: str = "dapminio"
    s3_bucket: str = "dap-artifacts"
    s3_region: str = "us-east-1"

    api_host: str = "0.0.0.0"
    api_port: int = 8080
    api_keys: str | None = None
    auth_required: bool = False
    # Optional JWT (HS256). When set with auth_required, Bearer tokens are accepted.
    jwt_secret: str | None = None
    jwt_issuer: str = "dap-api"
    jwt_ttl_seconds: int = 86_400

    pii_redaction_enabled: bool = True
    default_pii_policy: str = "redact"
    default_classification: str = "internal"

    embedding_backend: str = "deterministic"
    embedding_model: str = "deterministic"
    embedding_model_version: str = "v1"
    embedding_dim: int = 384
    embedding_batch_size: int = 16

    # Observability (additive)
    metrics_enabled: bool = True
    metrics_host: str = "0.0.0.0"
    metrics_port: int = 9100
    otel_enabled: bool = False

    # IoT edge simulator (EV fleet)
    iot_fleet_id: str = "fleet-east-01"
    iot_fleet_name: str = "East Coast EV Fleet"
    iot_vehicle_count: int = 8
    iot_publish_interval_seconds: float = 2.0
    iot_alert_probability: float = 0.18
    # simulator = existing iot_edge path; mqtt = optional broker bridge (Phase E)
    edge_mode: str = "simulator"
    mqtt_broker_host: str = "localhost"
    mqtt_broker_port: int = 1883
    mqtt_telemetry_topic: str = "fleet/+/vehicle/+/telemetry"

    consumer_group_ingest: str = "dap-ingest"
    consumer_group_etl: str = "dap-etl"
    consumer_group_embed: str = "dap-embed"
    consumer_group_index: str = "dap-index"

    version: str = Field(default="0.1.0")


@lru_cache
def get_settings() -> Settings:
    return Settings()
