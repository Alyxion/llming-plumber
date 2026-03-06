"""ARQ cron job for checking and enqueuing scheduled pipeline runs."""

from __future__ import annotations

from datetime import UTC, datetime

from croniter import croniter


def _utcnow() -> datetime:
    """Return the current UTC time. Extracted for testability."""
    return datetime.now(UTC)


def compute_next_run(cron_expression: str, after: datetime) -> datetime:
    """Use croniter to compute the next run time after the given datetime.

    Returns a timezone-aware UTC datetime.
    """
    cron = croniter(cron_expression, after)
    next_dt: datetime = cron.get_next(datetime)
    if next_dt.tzinfo is None:
        next_dt = next_dt.replace(tzinfo=UTC)
    return next_dt


async def check_schedules(ctx: dict) -> None:  # type: ignore[type-arg]
    """ARQ cron job. Runs every minute. Finds due schedules, enqueues runs."""
    db = ctx["db"]
    now = _utcnow()

    # ARQ provides the redis pool via ctx
    pool = ctx.get("redis") or ctx.get("pool")

    cursor = db["schedules"].find({
        "enabled": True,
        "next_run_at": {"$lte": now},
    })

    async for schedule in cursor:
        # Create run document
        run_doc = {
            "pipeline_id": schedule["pipeline_id"],
            "status": "queued",
            "created_at": now,
            "attempt": 0,
            "tags": schedule.get("tags", []),
        }
        result = await db["runs"].insert_one(run_doc)

        # Enqueue into ARQ
        if pool is not None:
            await pool.enqueue_job(
                "execute_pipeline",
                run_id=str(result.inserted_id),
            )

        # Advance next_run_at
        next_run = compute_next_run(schedule["cron_expression"], now)
        await db["schedules"].update_one(
            {"_id": schedule["_id"]},
            {"$set": {"next_run_at": next_run, "last_run_at": now}},
        )
