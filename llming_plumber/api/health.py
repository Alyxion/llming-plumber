from __future__ import annotations

from fastapi import APIRouter, Depends
from motor.motor_asyncio import AsyncIOMotorDatabase

from llming_plumber.api.deps import get_db
from llming_plumber.db import get_redis

router = APIRouter()


@router.get("/health")
async def health(
    db: AsyncIOMotorDatabase = Depends(get_db),  # type: ignore[type-arg]
) -> dict[str, str]:
    """Check MongoDB and Redis connectivity."""
    status: dict[str, str] = {}

    try:
        await db.command("ping")
        status["mongo"] = "ok"
    except Exception as exc:
        status["mongo"] = f"error: {exc}"

    try:
        redis = get_redis()
        await redis.ping()
        status["redis"] = "ok"
    except Exception as exc:
        status["redis"] = f"error: {exc}"

    is_healthy = (
        status.get("mongo") == "ok"
        and status.get("redis") == "ok"
    )
    status["status"] = "healthy" if is_healthy else "degraded"
    return status
