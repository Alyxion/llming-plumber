from __future__ import annotations

from typing import Any
from unittest.mock import patch

import httpx
import pytest
from fastapi import FastAPI
from mongomock_motor import AsyncMongoMockClient

from llming_plumber.blocks.registry import BlockRegistry

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _mock_db() -> Any:
    """Return a mongomock-motor database."""
    client = AsyncMongoMockClient()
    return client["plumber_test"]


class _FakeArqPool:
    """Minimal ARQ pool stub that records enqueued jobs."""

    def __init__(self) -> None:
        self.jobs: list[tuple[str, dict[str, Any]]] = []

    async def enqueue_job(self, func_name: str, **kwargs: Any) -> None:
        self.jobs.append((func_name, kwargs))

    async def aclose(self) -> None:
        pass


@pytest.fixture(autouse=True)
def _reset_block_registry() -> Any:
    """Ensure a clean BlockRegistry for each test."""
    BlockRegistry.reset()
    yield
    BlockRegistry.reset()


@pytest.fixture()
def mock_db() -> Any:
    return _mock_db()


@pytest.fixture()
def arq_pool() -> _FakeArqPool:
    return _FakeArqPool()


@pytest.fixture()
async def client(mock_db: Any, arq_pool: _FakeArqPool) -> Any:
    """Build a test client with mocked DB and ARQ pool.

    httpx.ASGITransport does not trigger ASGI lifespan events, so we
    bypass the lifespan by creating the app without one and manually
    setting up the state that the lifespan would normally configure.
    """
    import fakeredis.aioredis

    from llming_plumber.api import router as api_router
    from llming_plumber.api.deps import get_db as get_db_dep
    from llming_plumber.config import settings

    fake_redis = fakeredis.aioredis.FakeRedis()

    # Build a plain FastAPI app (no lifespan) with the same router
    app = FastAPI()
    app.include_router(api_router, prefix=settings.api_prefix)

    # Set state that the lifespan would normally provide
    app.state.arq_pool = arq_pool

    # Override dependencies
    app.dependency_overrides[get_db_dep] = lambda: mock_db

    with patch("llming_plumber.db.get_redis", return_value=fake_redis):
        BlockRegistry.discover()
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as ac:
            yield ac


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_health_endpoint(client: httpx.AsyncClient) -> None:
    resp = await client.get("/api/health")
    assert resp.status_code == 200
    data = resp.json()
    assert "status" in data
    assert "mongo" in data
    assert "redis" in data


async def test_list_blocks(client: httpx.AsyncClient) -> None:
    resp = await client.get("/api/blocks")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    # After discovery, there should be registered block types
    block_types = {b["block_type"] for b in data}
    # At minimum the built-in blocks should be present
    assert len(block_types) > 0


async def test_pipeline_crud(
    client: httpx.AsyncClient,
    mock_db: Any,
) -> None:
    # Discover blocks so we know a valid block_type
    BlockRegistry.discover()
    catalog = BlockRegistry.catalog()
    # Use the first available block type, or skip if none
    if not catalog:
        pytest.skip("No blocks registered")
    block_type = catalog[0].block_type

    # CREATE
    pipeline_data = {
        "name": "Test Pipeline",
        "description": "A test pipeline",
        "blocks": [
            {
                "uid": "b1",
                "block_type": block_type,
                "label": "Block 1",
                "config": {},
            }
        ],
        "pipes": [],
        "tags": ["test"],
    }
    resp = await client.post("/api/pipelines", json=pipeline_data)
    assert resp.status_code == 201
    created = resp.json()
    assert created["name"] == "Test Pipeline"
    pipeline_id = created["id"]
    assert pipeline_id

    # GET one
    resp = await client.get(f"/api/pipelines/{pipeline_id}")
    assert resp.status_code == 200
    assert resp.json()["name"] == "Test Pipeline"

    # LIST
    resp = await client.get("/api/pipelines")
    assert resp.status_code == 200
    items = resp.json()
    assert len(items) == 1

    # UPDATE
    pipeline_data["name"] = "Updated Pipeline"
    resp = await client.put(f"/api/pipelines/{pipeline_id}", json=pipeline_data)
    assert resp.status_code == 200
    updated = resp.json()
    assert updated["name"] == "Updated Pipeline"
    assert updated["version"] == 2

    # DELETE
    resp = await client.delete(f"/api/pipelines/{pipeline_id}")
    assert resp.status_code == 204

    # Verify deleted
    resp = await client.get(f"/api/pipelines/{pipeline_id}")
    assert resp.status_code == 404


async def test_trigger_run(
    client: httpx.AsyncClient,
    mock_db: Any,
    arq_pool: _FakeArqPool,
) -> None:
    BlockRegistry.discover()
    catalog = BlockRegistry.catalog()
    if not catalog:
        pytest.skip("No blocks registered")
    block_type = catalog[0].block_type

    # Create a pipeline first
    pipeline_data = {
        "name": "Runnable Pipeline",
        "blocks": [
            {"uid": "b1", "block_type": block_type, "label": "Block 1", "config": {}},
        ],
        "pipes": [],
    }
    resp = await client.post("/api/pipelines", json=pipeline_data)
    assert resp.status_code == 201
    pipeline_id = resp.json()["id"]

    # Trigger a run
    resp = await client.post(
        f"/api/pipelines/{pipeline_id}/run",
        json={"key": "value"},
    )
    assert resp.status_code == 201
    run_data = resp.json()
    assert run_data["status"] == "queued"
    assert "run_id" in run_data

    # Verify ARQ was called
    assert len(arq_pool.jobs) == 1
    assert arq_pool.jobs[0][0] == "execute_pipeline"
    assert arq_pool.jobs[0][1]["run_id"] == run_data["run_id"]


async def test_list_runs(
    client: httpx.AsyncClient,
    mock_db: Any,
    arq_pool: _FakeArqPool,
) -> None:
    BlockRegistry.discover()
    catalog = BlockRegistry.catalog()
    if not catalog:
        pytest.skip("No blocks registered")
    block_type = catalog[0].block_type

    # Create a pipeline and trigger a run
    resp = await client.post(
        "/api/pipelines",
        json={
            "name": "P",
            "blocks": [
                {"uid": "b1", "block_type": block_type, "label": "B1", "config": {}},
            ],
            "pipes": [],
        },
    )
    pipeline_id = resp.json()["id"]
    await client.post(f"/api/pipelines/{pipeline_id}/run", json={})

    # List runs
    resp = await client.get("/api/runs")
    assert resp.status_code == 200
    runs = resp.json()
    assert len(runs) == 1
    assert runs[0]["status"] == "queued"


async def test_schedule_crud(
    client: httpx.AsyncClient,
    mock_db: Any,
) -> None:
    # CREATE
    schedule_data = {
        "pipeline_id": "000000000000000000000001",
        "cron_expression": "*/5 * * * *",
        "enabled": True,
        "tags": ["nightly"],
    }
    resp = await client.post("/api/schedules", json=schedule_data)
    assert resp.status_code == 201
    created = resp.json()
    schedule_id = created["id"]
    assert schedule_id
    assert created["next_run_at"] is not None

    # LIST
    resp = await client.get("/api/schedules")
    assert resp.status_code == 200
    assert len(resp.json()) == 1

    # DELETE
    resp = await client.delete(f"/api/schedules/{schedule_id}")
    assert resp.status_code == 204

    # Verify deleted
    resp = await client.get("/api/schedules")
    assert resp.status_code == 200
    assert len(resp.json()) == 0
