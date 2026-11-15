"""Prometheus metrics for DAP services (additive; safe when unused)."""

from __future__ import annotations

import logging
import threading
from time import perf_counter

from dap_common.config import Settings

log = logging.getLogger(__name__)

_started = False
_lock = threading.Lock()

try:
    from prometheus_client import Counter, Histogram, Info, generate_latest, CONTENT_TYPE_LATEST
    from prometheus_client import start_http_server

    MESSAGES_TOTAL = Counter(
        "dap_messages_total",
        "Kafka messages handled",
        ["service", "topic", "outcome"],
    )
    HANDLER_SECONDS = Histogram(
        "dap_handler_seconds",
        "Handler wall time in seconds",
        ["service", "topic"],
        buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
    )
    PUBLISH_TOTAL = Counter(
        "dap_publish_total",
        "Kafka publish attempts",
        ["service", "topic", "outcome"],
    )
    SERVICE_INFO = Info("dap_service", "DAP service identity")
    PROM_AVAILABLE = True
except ImportError:  # pragma: no cover
    PROM_AVAILABLE = False
    CONTENT_TYPE_LATEST = "text/plain; version=0.0.4"
    MESSAGES_TOTAL = None  # type: ignore[assignment]
    HANDLER_SECONDS = None  # type: ignore[assignment]
    PUBLISH_TOTAL = None  # type: ignore[assignment]
    SERVICE_INFO = None  # type: ignore[assignment]
    generate_latest = None  # type: ignore[assignment]
    start_http_server = None  # type: ignore[assignment]


def init_service_info(settings: Settings) -> None:
    if not PROM_AVAILABLE or SERVICE_INFO is None:
        return
    SERVICE_INFO.info(
        {
            "service": settings.service_name,
            "version": settings.version,
        }
    )


def maybe_start_metrics_server(settings: Settings) -> None:
    """Expose /metrics on DAP_METRICS_PORT for worker processes (Compose/Kind scrape)."""
    global _started
    if not settings.metrics_enabled or not PROM_AVAILABLE or start_http_server is None:
        return
    with _lock:
        if _started:
            return
        try:
            start_http_server(settings.metrics_port, addr=settings.metrics_host)
            _started = True
            init_service_info(settings)
            log.info(
                "metrics_server_started host=%s port=%s",
                settings.metrics_host,
                settings.metrics_port,
            )
        except OSError as exc:
            log.warning("metrics_server_failed error=%s", exc)


def record_consume(service: str, topic: str, outcome: str, seconds: float) -> None:
    if not PROM_AVAILABLE:
        return
    MESSAGES_TOTAL.labels(service=service, topic=topic, outcome=outcome).inc()
    HANDLER_SECONDS.labels(service=service, topic=topic).observe(seconds)


def record_publish(service: str, topic: str, outcome: str) -> None:
    if not PROM_AVAILABLE:
        return
    PUBLISH_TOTAL.labels(service=service, topic=topic, outcome=outcome).inc()


def metrics_payload() -> tuple[bytes, str]:
    if not PROM_AVAILABLE or generate_latest is None:
        return b"# prometheus_client not installed\n", "text/plain"
    return generate_latest(), CONTENT_TYPE_LATEST


class Timer:
    def __init__(self) -> None:
        self._t0 = perf_counter()

    def seconds(self) -> float:
        return perf_counter() - self._t0
