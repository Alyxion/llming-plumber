"""ARQ lemming (worker) configuration for Plumber."""

from __future__ import annotations

import os
import socket
from datetime import UTC, datetime
from uuid import uuid4

from arq import cron
from arq.connections import RedisSettings

from llming_plumber.config import settings
from llming_plumber.db import ensure_indexes, get_database
from llming_plumber.worker.executor import execute_pipeline
from llming_plumber.worker.scheduler import check_schedules

LEMMING_ID = f"{socket.gethostname()}:{os.getpid()}:{uuid4().hex[:8]}"


async def startup(ctx: dict) -> None:  # type: ignore[type-arg]
    """Called once when the lemming starts."""
    ctx["lemming_id"] = LEMMING_ID
    db = get_database()
    ctx["db"] = db
    await ensure_indexes(db)
    await db["lemmings"].update_one(
        {"lemming_id": LEMMING_ID},
        {"$set": {"started_at": datetime.now(UTC), "status": "online"}},
        upsert=True,
    )


async def shutdown(ctx: dict) -> None:  # type: ignore[type-arg]
    """Called when the lemming shuts down."""
    db = ctx["db"]
    await db["lemmings"].update_one(
        {"lemming_id": LEMMING_ID},
        {"$set": {"status": "offline", "stopped_at": datetime.now(UTC)}},
    )


class LemmingSettings:
    """ARQ lemming configuration.

    Run with ``arq llming_plumber.worker.LemmingSettings``.
    """

    redis_settings = RedisSettings.from_dsn(settings.redis_url)

    functions = [execute_pipeline]

    cron_jobs = [cron(check_schedules, minute=None)]  # every minute

    max_jobs = settings.lemming_concurrency

    health_check_interval = settings.health_check_interval

    job_timeout = settings.run_timeout

    on_startup = startup
    on_shutdown = shutdown
