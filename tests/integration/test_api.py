from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from dml.api.main import create_app
from dml.config import Settings

pytestmark = pytest.mark.integration


def test_health_and_create_job(settings: Settings) -> None:
    app = create_app(settings)
    try:
        with TestClient(app) as client:
            live = client.get("/health/live")
            assert live.status_code == 200
            ready = client.get("/health/ready")
            if ready.status_code == 503:
                pytest.skip(ready.text)
            created = client.post(
                "/v1/jobs",
                json={"job_type": "dummy", "task_count": 3, "sleep_ms": 0, "name": "api-test"},
            )
            assert created.status_code == 200, created.text
            job_id = created.json()["id"]
            fetched = client.get(f"/v1/jobs/{job_id}")
            assert fetched.status_code == 200
            tasks = client.get(f"/v1/jobs/{job_id}/tasks")
            assert tasks.status_code == 200
            assert len(tasks.json()) == 3
            workers = client.get("/v1/workers")
            assert workers.status_code == 200
            home = client.get("/")
            assert home.status_code == 200
            assert "Distributed AI" in home.text
            pipes = client.get("/v1/pipelines")
            assert pipes.status_code == 200
            models = client.get("/v1/models")
            assert models.status_code == 200
    except Exception as exc:
        if "connect" in str(exc).lower() or "refused" in str(exc).lower():
            pytest.skip(str(exc))
        raise
