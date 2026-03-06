from __future__ import annotations

from typing import Any

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo import ASCENDING, DESCENDING

from llming_plumber.config import settings

_motor_client: AsyncIOMotorClient | None = None  # type: ignore[type-arg]
_redis_client: Any = None


def get_database() -> AsyncIOMotorDatabase:  # type: ignore[type-arg]
    """Return the Motor database, lazily creating the client on first call."""
    global _motor_client
    if _motor_client is None:
        _motor_client = AsyncIOMotorClient(settings.mongo_uri)
    return _motor_client[settings.mongo_db]


def get_redis() -> Any:
    """Return a cached async Redis connection (single-node or cluster)."""
    global _redis_client
    if _redis_client is None:
        if settings.redis_cluster:
            from redis.asyncio.cluster import RedisCluster

            _redis_client = RedisCluster.from_url(
                settings.redis_url,
                decode_responses=True,
            )
        else:
            from redis.asyncio import from_url as redis_from_url

            _redis_client = redis_from_url(
                settings.redis_url,
                decode_responses=True,
            )
    return _redis_client


async def ensure_indexes(
    db: AsyncIOMotorDatabase,  # type: ignore[type-arg]
) -> None:
    """Create indexes for all collections."""
    # Pipelines
    pipelines = db["pipelines"]
    await pipelines.create_index([("owner_id", ASCENDING)])
    await pipelines.create_index([("tags", ASCENDING)])
    await pipelines.create_index(
        [("name", ASCENDING)],
        unique=True,
    )

    # Runs
    runs = db["runs"]
    await runs.create_index(
        [("status", ASCENDING), ("created_at", DESCENDING)],
    )
    await runs.create_index(
        [("pipeline_id", ASCENDING), ("created_at", DESCENDING)],
    )
    await runs.create_index(
        [("lemming_id", ASCENDING), ("status", ASCENDING)],
    )

    # Run logs
    run_logs = db["run_logs"]
    await run_logs.create_index(
        [("run_id", ASCENDING), ("ts", ASCENDING)],
    )

    # Schedules
    schedules = db["schedules"]
    await schedules.create_index([("enabled", ASCENDING)])
    await schedules.create_index(
        [("enabled", ASCENDING), ("next_run_at", ASCENDING)],
    )


async def close_connections() -> None:
    """Tear down Motor and Redis connections."""
    global _motor_client, _redis_client
    if _motor_client is not None:
        _motor_client.close()
        _motor_client = None
    if _redis_client is not None:
        await _redis_client.aclose()
        _redis_client = None
