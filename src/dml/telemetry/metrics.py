"""Prometheus metrics for the Phase 1 runtime."""

from __future__ import annotations

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
    start_http_server,
)


class Metrics:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        self.tasks_created_total = Counter(
            "tasks_created_total", "Tasks inserted with a job", registry=self.registry
        )
        self.tasks_completed_total = Counter(
            "tasks_completed_total",
            "Tasks that reached succeeded",
            ["task_type"],
            registry=self.registry,
        )
        self.tasks_failed_total = Counter(
            "tasks_failed_total",
            "Failed attempts (retryable or not)",
            ["task_type", "error_type"],
            registry=self.registry,
        )
        self.task_retries_total = Counter(
            "task_retries_total", "Retryable failures that will be requeued", registry=self.registry
        )
        self.task_dead_letter_total = Counter(
            "task_dead_letter_total", "Tasks moved to dead_letter", registry=self.registry
        )
        self.task_duration_seconds = Histogram(
            "task_duration_seconds",
            "Handler wall time",
            ["task_type"],
            registry=self.registry,
        )
        self.queue_depth = Gauge(
            "queue_depth", "Redis stream XLEN", ["stream"], registry=self.registry
        )
        self.workers_registered = Gauge(
            "workers_registered", "Rows in workers table", registry=self.registry
        )
        self.workers_healthy = Gauge(
            "workers_healthy", "Workers with fresh heartbeat", registry=self.registry
        )
        self.worker_task_count = Gauge(
            "worker_task_count", "In-flight tasks on this process", registry=self.registry
        )
        self.api_request_duration_seconds = Histogram(
            "api_request_duration_seconds",
            "HTTP request duration",
            ["method", "path", "status"],
            registry=self.registry,
        )
        self.dummy_executions_total = Counter(
            "dummy_executions_total", "Dummy handler invocations", registry=self.registry
        )

    def render(self) -> tuple[bytes, str]:
        return generate_latest(self.registry), CONTENT_TYPE_LATEST

    def serve(self, port: int) -> None:
        start_http_server(port, registry=self.registry)
