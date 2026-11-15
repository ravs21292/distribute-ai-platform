"""Control-plane API: fleet onboarding, plugin activate, telemetry query, alert search."""

from __future__ import annotations

import secrets
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dap_common.config import Settings, get_settings
from dap_common.db import make_session_factory
from dap_common.embeddings import build_embedding_provider, vector_literal
from dap_common.kafka_bus import build_producer, publish
from dap_common.logging_setup import setup_logging
from dap_common.metrics import init_service_info, metrics_payload
from dap_common.models import (
    AlertDocument,
    AuditEvent,
    Fleet,
    FleetPipelineStats,
    TelematicsEvent,
    Vehicle,
)
from dap_common.schema import apply_schema
from dap_common.topics import EdgeCommand, Topics


class SearchIn(BaseModel):
    query: str
    fleet_id: str | None = None
    limit: int = Field(default=5, ge=1, le=50)


class FleetCreate(BaseModel):
    id: str = Field(min_length=2, max_length=64)
    name: str
    classification: str = "internal"
    pii_policy: str = "redact"


class VehicleProvision(BaseModel):
    vehicle_id: str = Field(min_length=2, max_length=64)
    vin: str | None = None
    model: str = "Sedan-E"
    plugin_protocol: str = "mqtt"


class TokenRequest(BaseModel):
    api_key: str
    subject: str = "operator"


class DlqReplayIn(BaseModel):
    """Replay a DLQ payload back onto its source topic (additive admin tool)."""

    source_topic: str
    payload: dict[str, Any]
    key: str | None = None
    limit_dry_run: bool = False


def _require_key(request: Request, settings: Settings) -> str:
    """Accept X-API-Key and optional Bearer JWT. Default demo: auth off."""
    if not settings.auth_required and not settings.api_keys and not settings.jwt_secret:
        return "anonymous"

    auth = request.headers.get("authorization") or ""
    if auth.lower().startswith("bearer ") and settings.jwt_secret:
        token = auth.split(" ", 1)[1].strip()
        try:
            import jwt

            claims = jwt.decode(
                token,
                settings.jwt_secret,
                algorithms=["HS256"],
                issuer=settings.jwt_issuer,
            )
            return str(claims.get("sub") or "jwt")[:64]
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(401, f"invalid bearer token: {exc}") from exc

    key = request.headers.get("x-api-key")
    if not key:
        raise HTTPException(401, "X-API-Key or Bearer token required")
    import json

    raw = settings.api_keys or ""
    if raw.strip().startswith("{"):
        mapping = json.loads(raw)
        if key not in mapping:
            raise HTTPException(401, "invalid api key")
    else:
        allowed = {part.strip() for part in raw.split(",") if part.strip()}
        if key not in allowed:
            raise HTTPException(401, "invalid api key")
    return key[:12]


def _vehicle_out(v: Vehicle) -> dict[str, Any]:
    return {
        "id": v.id,
        "fleet_id": v.fleet_id,
        "vin": v.vin,
        "model": v.model,
        "status": v.status,
        "plugin_protocol": v.plugin_protocol,
        "device_token": v.device_token,
        "activated_at": v.activated_at.isoformat() if v.activated_at else None,
        "last_seen_at": v.last_seen_at.isoformat() if v.last_seen_at else None,
        "last_soc_percent": v.last_soc_percent,
        "last_speed_kph": v.last_speed_kph,
        "last_location": {"lat": v.last_lat, "lon": v.last_lon} if v.last_lat is not None else None,
    }


def _plugin_bundle(fleet_id: str, vehicle: Vehicle) -> dict[str, Any]:
    """Instructions the TCU / edge agent would use after onboarding."""
    topic = f"fleet/{fleet_id}/vehicle/{vehicle.id}/telemetry"
    return {
        "protocol": vehicle.plugin_protocol,
        "mqtt": {
            "topic": topic,
            "qos": 1,
            "auth": "device_token as password (username = vehicle_id)",
            "note": "Production: TCU publishes MQTT → IoT broker → Kafka dap.iot.telemetry",
        },
        "platform_intake_topic": Topics.IOT_TELEMETRY.value,
        "activate_api": f"/v1/fleets/{fleet_id}/vehicles/{vehicle.id}/activate",
        "deactivate_api": f"/v1/fleets/{fleet_id}/vehicles/{vehicle.id}/deactivate",
        "local_simulator": "After Activate, dap-iot-edge starts continuous capture for this vehicle_id",
    }


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    setup_logging(settings)
    settings.service_name = "api"
    engine = create_async_engine(settings.database_url, pool_pre_ping=True)
    await apply_schema(engine)
    # Soft-upgrade columns if DB volume already existed.
    async with engine.begin() as conn:
        await conn.execute(text("ALTER TABLE vehicles ADD COLUMN IF NOT EXISTS device_token VARCHAR(128)"))
        await conn.execute(text("ALTER TABLE vehicles ADD COLUMN IF NOT EXISTS plugin_protocol VARCHAR(32) DEFAULT 'mqtt'"))
        await conn.execute(text("ALTER TABLE vehicles ADD COLUMN IF NOT EXISTS activated_at TIMESTAMPTZ"))

    factory = make_session_factory(settings)
    async with factory() as session:
        fleet = await session.get(Fleet, settings.iot_fleet_id)
        if fleet is None:
            session.add(
                Fleet(
                    id=settings.iot_fleet_id,
                    name=settings.iot_fleet_name,
                    classification=settings.default_classification,
                    pii_policy=settings.default_pii_policy,
                )
            )
            session.add(FleetPipelineStats(fleet_id=settings.iot_fleet_id))
            await session.commit()

    producer = await build_producer(settings)
    app.state.settings = settings
    app.state.session_factory = factory
    app.state.engine = engine
    app.state.producer = producer
    init_service_info(settings)
    yield
    await producer.stop()
    await engine.dispose()


app = FastAPI(
    title="DAP EV Fleet Telematics API",
    version="0.3.0",
    description="Onboard vehicles, activate continuous capture, query telemetry/alerts.",
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def get_session(request: Request) -> AsyncSession:
    factory: async_sessionmaker[AsyncSession] = request.app.state.session_factory
    async with factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": "api", "domain": "ev-fleet-telematics"}


@app.get("/metrics")
async def metrics() -> Response:
    body, content_type = metrics_payload()
    return Response(content=body, media_type=content_type)


@app.post("/v1/auth/token")
async def issue_token(body: TokenRequest, request: Request) -> dict[str, Any]:
    """Exchange a valid API key for a short-lived JWT (optional; needs DAP_JWT_SECRET)."""
    settings: Settings = request.app.state.settings
    if not settings.jwt_secret:
        raise HTTPException(501, "JWT not configured (set DAP_JWT_SECRET)")
    import json
    from datetime import datetime, timedelta, timezone

    raw = settings.api_keys or ""
    if not raw:
        raise HTTPException(501, "DAP_API_KEYS required to mint tokens")
    if raw.strip().startswith("{"):
        mapping = json.loads(raw)
        if body.api_key not in mapping:
            raise HTTPException(401, "invalid api key")
    else:
        allowed = {part.strip() for part in raw.split(",") if part.strip()}
        if body.api_key not in allowed:
            raise HTTPException(401, "invalid api key")
    try:
        import jwt
    except ImportError as exc:
        raise HTTPException(501, "PyJWT not installed in api image") from exc
    now = datetime.now(timezone.utc)
    token = jwt.encode(
        {
            "sub": body.subject,
            "iss": settings.jwt_issuer,
            "iat": now,
            "exp": now + timedelta(seconds=settings.jwt_ttl_seconds),
        },
        settings.jwt_secret,
        algorithm="HS256",
    )
    return {"access_token": token, "token_type": "bearer", "expires_in": settings.jwt_ttl_seconds}


@app.post("/v1/admin/dlq/replay")
async def dlq_replay(body: DlqReplayIn, request: Request) -> dict[str, Any]:
    """Re-publish a DLQ payload to its source topic. Does not delete DLQ messages."""
    actor = _require_key(request, request.app.state.settings)
    if body.limit_dry_run:
        return {
            "dry_run": True,
            "would_publish_to": body.source_topic,
            "key": body.key,
            "actor": actor,
        }
    await publish(
        request.app.state.producer,
        body.source_topic,
        body.payload,
        key=body.key,
        service_name="api",
    )
    return {"replayed": True, "topic": body.source_topic, "actor": actor}


@app.post("/v1/fleets")
async def create_fleet(
    body: FleetCreate, request: Request, session: AsyncSession = Depends(get_session)
) -> dict[str, Any]:
    actor = _require_key(request, request.app.state.settings)
    existing = await session.get(Fleet, body.id)
    if existing:
        raise HTTPException(409, "fleet already exists")
    session.add(
        Fleet(
            id=body.id,
            name=body.name,
            classification=body.classification,
            pii_policy=body.pii_policy,
        )
    )
    session.add(FleetPipelineStats(fleet_id=body.id))
    session.add(
        AuditEvent(
            id=uuid4(),
            actor=actor,
            action="fleet.create",
            resource_type="fleet",
            resource_id=body.id,
            detail={"name": body.name},
        )
    )
    await session.flush()
    return {
        "id": body.id,
        "name": body.name,
        "classification": body.classification,
        "pii_policy": body.pii_policy,
    }


@app.get("/v1/fleets")
async def list_fleets(
    request: Request, session: AsyncSession = Depends(get_session)
) -> list[dict[str, Any]]:
    _require_key(request, request.app.state.settings)
    fleets = (await session.scalars(select(Fleet).order_by(Fleet.id))).all()
    out = []
    for f in fleets:
        stats = await session.get(FleetPipelineStats, f.id)
        out.append(
            {
                "id": f.id,
                "name": f.name,
                "classification": f.classification,
                "pii_policy": f.pii_policy,
                "stats": {
                    "events_ingested": stats.events_ingested if stats else 0,
                    "events_cleaned": stats.events_cleaned if stats else 0,
                    "alerts_indexed": stats.alerts_indexed if stats else 0,
                    "embeddings_ready": stats.embeddings_ready if stats else 0,
                    "last_event_at": stats.last_event_at.isoformat()
                    if stats and stats.last_event_at
                    else None,
                },
            }
        )
    return out


@app.post("/v1/fleets/{fleet_id}/vehicles")
async def provision_vehicle(
    fleet_id: str,
    body: VehicleProvision,
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> dict[str, Any]:
    """Onboard a new EV: register identity + issue device token (plugin credentials)."""
    actor = _require_key(request, request.app.state.settings)
    fleet = await session.get(Fleet, fleet_id)
    if fleet is None:
        raise HTTPException(404, "fleet not found")
    existing = await session.get(Vehicle, body.vehicle_id)
    if existing is not None:
        raise HTTPException(409, "vehicle already provisioned")

    token = secrets.token_urlsafe(24)
    vehicle = Vehicle(
        id=body.vehicle_id,
        fleet_id=fleet_id,
        vin=body.vin,
        model=body.model,
        status="provisioned",
        device_token=token,
        plugin_protocol=body.plugin_protocol or "mqtt",
    )
    session.add(vehicle)
    session.add(
        AuditEvent(
            id=uuid4(),
            actor=actor,
            action="vehicle.provision",
            resource_type="vehicle",
            resource_id=body.vehicle_id,
            detail={"fleet_id": fleet_id, "plugin_protocol": vehicle.plugin_protocol},
        )
    )
    await session.flush()
    return {
        "vehicle": _vehicle_out(vehicle),
        "plugin": _plugin_bundle(fleet_id, vehicle),
        "next_step": "Call Activate to start continuous data capture into the platform",
    }


@app.post("/v1/fleets/{fleet_id}/vehicles/{vehicle_id}/activate")
async def activate_vehicle(
    fleet_id: str,
    vehicle_id: str,
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> dict[str, Any]:
    """Backend initiates continuous capture: edge gateway starts publishing this EV."""
    actor = _require_key(request, request.app.state.settings)
    vehicle = await session.get(Vehicle, vehicle_id)
    if vehicle is None or vehicle.fleet_id != fleet_id:
        raise HTTPException(404, "vehicle not found")
    vehicle.status = "streaming"
    vehicle.activated_at = datetime.now(timezone.utc)
    session.add(
        AuditEvent(
            id=uuid4(),
            actor=actor,
            action="vehicle.activate",
            resource_type="vehicle",
            resource_id=vehicle_id,
            detail={"fleet_id": fleet_id},
        )
    )
    await session.flush()

    cmd = EdgeCommand(
        action="activate",
        fleet_id=fleet_id,
        vehicle_id=vehicle_id,
        device_token=vehicle.device_token,
        plugin_protocol=vehicle.plugin_protocol,
        requested_at=datetime.now(timezone.utc),
    )
    await publish(request.app.state.producer, Topics.EDGE_COMMANDS, cmd, key=vehicle_id)
    return {
        "vehicle": _vehicle_out(vehicle),
        "capture": "started",
        "edge_command": cmd.model_dump(mode="json"),
        "message": "Edge gateway instructed to stream telemetry continuously into Kafka",
    }


@app.post("/v1/fleets/{fleet_id}/vehicles/{vehicle_id}/deactivate")
async def deactivate_vehicle(
    fleet_id: str,
    vehicle_id: str,
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> dict[str, Any]:
    actor = _require_key(request, request.app.state.settings)
    vehicle = await session.get(Vehicle, vehicle_id)
    if vehicle is None or vehicle.fleet_id != fleet_id:
        raise HTTPException(404, "vehicle not found")
    vehicle.status = "paused"
    session.add(
        AuditEvent(
            id=uuid4(),
            actor=actor,
            action="vehicle.deactivate",
            resource_type="vehicle",
            resource_id=vehicle_id,
            detail={"fleet_id": fleet_id},
        )
    )
    await session.flush()
    cmd = EdgeCommand(
        action="deactivate",
        fleet_id=fleet_id,
        vehicle_id=vehicle_id,
        device_token=vehicle.device_token,
        plugin_protocol=vehicle.plugin_protocol,
        requested_at=datetime.now(timezone.utc),
    )
    await publish(request.app.state.producer, Topics.EDGE_COMMANDS, cmd, key=vehicle_id)
    return {"vehicle": _vehicle_out(vehicle), "capture": "stopped"}


@app.get("/v1/fleets/{fleet_id}/vehicles")
async def list_vehicles(
    fleet_id: str, request: Request, session: AsyncSession = Depends(get_session)
) -> list[dict[str, Any]]:
    _require_key(request, request.app.state.settings)
    rows = (
        await session.scalars(
            select(Vehicle).where(Vehicle.fleet_id == fleet_id).order_by(Vehicle.id)
        )
    ).all()
    return [_vehicle_out(v) for v in rows]


@app.get("/v1/fleets/{fleet_id}/telemetry")
async def recent_telemetry(
    fleet_id: str,
    request: Request,
    vehicle_id: str | None = None,
    limit: int = Query(default=20, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
) -> list[dict[str, Any]]:
    _require_key(request, request.app.state.settings)
    stmt = select(TelematicsEvent).where(TelematicsEvent.fleet_id == fleet_id)
    if vehicle_id:
        stmt = stmt.where(TelematicsEvent.vehicle_id == vehicle_id)
    stmt = stmt.order_by(TelematicsEvent.event_time.desc()).limit(limit)
    rows = (await session.scalars(stmt)).all()
    return [
        {
            "event_id": str(r.event_id),
            "event_time": r.event_time.isoformat(),
            "vehicle_id": r.vehicle_id,
            "speed_kph": r.speed_kph,
            "soc_percent": r.soc_percent,
            "battery_temp_c": r.battery_temp_c,
            "charging_state": r.charging_state,
            "odometer_km": r.odometer_km,
            "location": {"lat": r.lat, "lon": r.lon} if r.lat is not None else None,
            "alert_code": r.alert_code,
            "alert_severity": r.alert_severity,
            "alert_message": r.alert_message,
            "note": r.note,
            "pii_redacted": r.pii_redacted,
            "pii_categories": r.pii_categories,
        }
        for r in rows
    ]


@app.get("/v1/fleets/{fleet_id}/alerts")
async def list_alerts(
    fleet_id: str,
    request: Request,
    limit: int = Query(default=20, ge=1, le=100),
    session: AsyncSession = Depends(get_session),
) -> list[dict[str, Any]]:
    _require_key(request, request.app.state.settings)
    rows = (
        await session.scalars(
            select(AlertDocument)
            .where(AlertDocument.fleet_id == fleet_id)
            .order_by(AlertDocument.created_at.desc())
            .limit(limit)
        )
    ).all()
    return [
        {
            "id": str(r.id),
            "vehicle_id": r.vehicle_id,
            "severity": r.severity,
            "text": r.text,
            "pii_redacted": r.pii_redacted,
            "embedded": r.embedded,
            "metadata": r.metadata_json,
            "created_at": r.created_at.isoformat() if r.created_at else None,
        }
        for r in rows
    ]


@app.post("/v1/search")
async def search_alerts(
    body: SearchIn, request: Request, session: AsyncSession = Depends(get_session)
) -> dict[str, Any]:
    settings: Settings = request.app.state.settings
    actor = _require_key(request, settings)
    if not body.query.strip():
        raise HTTPException(400, "query must not be empty")

    provider = build_embedding_provider(settings)
    qvec = provider.embed_batch([body.query])[0]
    if body.fleet_id:
        sql = """
            SELECT d.text, d.vehicle_id, d.fleet_id, d.severity, d.pii_redacted, d.metadata_json,
                   1 - (e.embedding <=> CAST(:q AS vector)) AS similarity
            FROM alert_documents d
            JOIN alert_embeddings e
              ON e.content_hash = d.content_hash
             AND e.embedding_model = :model
             AND e.embedding_model_version = :ver
            WHERE d.fleet_id = :fid
            ORDER BY e.embedding <=> CAST(:q AS vector)
            LIMIT :lim
        """
        params = {
            "q": vector_literal(qvec),
            "model": provider.model,
            "ver": provider.model_version,
            "fid": body.fleet_id,
            "lim": body.limit,
        }
    else:
        sql = """
            SELECT d.text, d.vehicle_id, d.fleet_id, d.severity, d.pii_redacted, d.metadata_json,
                   1 - (e.embedding <=> CAST(:q AS vector)) AS similarity
            FROM alert_documents d
            JOIN alert_embeddings e
              ON e.content_hash = d.content_hash
             AND e.embedding_model = :model
             AND e.embedding_model_version = :ver
            ORDER BY e.embedding <=> CAST(:q AS vector)
            LIMIT :lim
        """
        params = {
            "q": vector_literal(qvec),
            "model": provider.model,
            "ver": provider.model_version,
            "lim": body.limit,
        }
    rows = (await session.execute(text(sql), params)).all()
    fleet = await session.get(Fleet, body.fleet_id) if body.fleet_id else None
    session.add(
        AuditEvent(
            id=uuid4(),
            actor=actor,
            action="search.alerts",
            resource_type="fleet",
            resource_id=body.fleet_id,
            detail={"hits": len(rows), "query_len": len(body.query)},
        )
    )
    return {
        "governance": {
            "classification": fleet.classification if fleet else settings.default_classification,
            "pii_policy": fleet.pii_policy if fleet else settings.default_pii_policy,
            "fleet_id": body.fleet_id,
            "domain": "ev-fleet-telematics",
        },
        "hits": [
            {
                "text": r[0],
                "vehicle_id": r[1],
                "fleet_id": r[2],
                "severity": r[3],
                "pii_redacted": bool(r[4]),
                "metadata": r[5] or {},
                "similarity": float(r[6]),
            }
            for r in rows
        ],
    }


@app.get("/v1/pipeline/overview")
async def pipeline_overview(
    request: Request, session: AsyncSession = Depends(get_session)
) -> dict[str, Any]:
    _require_key(request, request.app.state.settings)
    fleets = (await session.scalars(select(Fleet))).all()
    vehicle_count = await session.scalar(text("SELECT COUNT(*) FROM vehicles"))
    streaming = await session.scalar(text("SELECT COUNT(*) FROM vehicles WHERE status = 'streaming'"))
    event_count = await session.scalar(text("SELECT COUNT(*) FROM telematics_events"))
    alert_count = await session.scalar(text("SELECT COUNT(*) FROM alert_documents"))
    embed_count = await session.scalar(text("SELECT COUNT(*) FROM alert_embeddings"))
    return {
        "domain": "ev-fleet-telematics",
        "intake": "continuous Kafka dap.iot.telemetry after vehicle Activate",
        "fleets": len(fleets),
        "vehicles": int(vehicle_count or 0),
        "streaming_vehicles": int(streaming or 0),
        "telematics_events": int(event_count or 0),
        "alerts": int(alert_count or 0),
        "embeddings": int(embed_count or 0),
        "generated_at": datetime.utcnow().isoformat() + "Z",
    }


def main() -> None:
    import uvicorn

    settings = get_settings()
    uvicorn.run("dap_api.main:app", host=settings.api_host, port=settings.api_port, reload=False)


if __name__ == "__main__":
    main()
