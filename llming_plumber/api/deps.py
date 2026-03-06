from __future__ import annotations

from typing import Any

from fastapi import Request
from motor.motor_asyncio import AsyncIOMotorDatabase

from llming_plumber.db import get_database


async def get_db() -> AsyncIOMotorDatabase:  # type: ignore[type-arg]
    """FastAPI dependency that returns the Motor database."""
    return get_database()


async def get_arq_pool(request: Request) -> Any:
    """FastAPI dependency that returns the ARQ Redis connection pool."""
    return request.app.state.arq_pool
