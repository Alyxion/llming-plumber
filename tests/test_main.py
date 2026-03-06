from __future__ import annotations

from unittest.mock import AsyncMock, patch

import httpx
import pytest

from llming_plumber import create_app


async def test_create_app_returns_fastapi() -> None:
    """Verify that create_app() returns a FastAPI application."""
    with (
        patch("llming_plumber.db.ensure_indexes", new_callable=AsyncMock),
        patch("arq.connections.create_pool", new_callable=AsyncMock),
        patch("llming_plumber.db.close_connections", new_callable=AsyncMock),
    ):
        app = create_app(mode="all")
        assert app.title == "Plumber"
        # The API router should be mounted under /api
        route_paths = [r.path for r in app.routes]
        assert any("/api" in p for p in route_paths)
