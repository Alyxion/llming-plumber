"""ARQ lemming (worker) configuration for Plumber."""

from __future__ import annotations

import asyncio
import logging
import os
import socket
from datetime import UTC, datetime
from uuid import uuid4

from arq.connections import RedisSettings

from llming_plumber.config import settings
from llming_plumber.db import ensure_indexes, get_database
from llming_plumber.worker.executor import execute_pipeline
from llming_plumber.worker.scheduler import check_schedules

logger = logging.getLogger(__name__)

LEMMING_ID = f"{socket.gethostname()}:{os.getpid()}:{uuid4().hex[:8]}"

_scheduler_task: asyncio.Task | None = None  # type: ignore[type-arg]


async def _scheduler_loop(ctx: dict) -> None:  # type: ignore[type-arg]
    """Fast polling loop — checks for due schedules every N seconds."""
    poll = settings.scheduler_poll_seconds
    logger.info("Scheduler loop started (poll every %ds)", poll)
    while True:
        try:
            await check_schedules(ctx)
        except Exception:
            logger.exception("Scheduler loop iteration failed")
        await asyncio.sleep(poll)


async def startup(ctx: dict) -> None:  # type: ignore[type-arg]
    """Called once when the lemming starts."""
    global _scheduler_task
    ctx["lemming_id"] = LEMMING_ID
    db = get_database()
    ctx["db"] = db
    await ensure_indexes(db)
    await db["lemmings"].update_one(
        {"lemming_id": LEMMING_ID},
        {"$set": {"started_at": datetime.now(UTC), "status": "online"}},
        upsert=True,
    )
    # Start the fast scheduler loop
    _scheduler_task = asyncio.create_task(_scheduler_loop(ctx))


async def shutdown(ctx: dict) -> None:  # type: ignore[type-arg]
    """Called when the lemming shuts down."""
    global _scheduler_task
    if _scheduler_task:
        _scheduler_task.cancel()
        _scheduler_task = None
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

    # No more ARQ cron — the scheduler runs as a fast asyncio loop instead
    cron_jobs = []

    max_jobs = settings.lemming_concurrency

    health_check_interval = settings.health_check_interval

    job_timeout = settings.run_timeout

    on_startup = startup
    on_shutdown = shutdown
