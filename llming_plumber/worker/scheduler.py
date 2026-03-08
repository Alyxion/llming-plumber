"""Schedule checker — finds due schedules and enqueues pipeline runs.

Called every few seconds by the fast scheduler loop in the worker.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

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


def _in_time_window(
    now: datetime, windows: list[dict[str, Any]],
) -> bool:
    """Check if the current time falls within any of the time windows."""
    if not windows:
        return True
    weekday = now.weekday()
    t = now.strftime("%H:%M")
    for w in windows:
        if weekday not in w.get("weekdays", [0, 1, 2, 3, 4, 5, 6]):
            continue
        start = w.get("start", "00:00")
        end = w.get("end", "23:59")
        if start <= t <= end:
            return True
    return False


def _compute_next_interval(
    schedule: dict[str, Any], now: datetime,
) -> datetime:
    """Compute the next run time for an interval-based schedule.

    Respects time windows and applies the off-hours multiplier.
    """
    interval = schedule.get("interval_seconds", 60) or 60
    windows = schedule.get("time_windows", [])
    multiplier = schedule.get("interval_multiplier_off_hours", 1.0) or 1.0

    if windows and not _in_time_window(now, windows) and multiplier > 1.0:
        interval = int(interval * multiplier)

    return now + timedelta(seconds=interval)


async def check_schedules(ctx: dict) -> None:  # type: ignore[type-arg]
    """Find due schedules, enqueue runs. Called every few seconds."""
    db = ctx["db"]
    now = _utcnow()

    # ARQ provides the redis pool via ctx
    pool = ctx.get("redis") or ctx.get("pool")

    cursor = db["schedules"].find({
        "enabled": True,
        "next_run_at": {"$lte": now},
    })

    async for schedule in cursor:
        # Skip if outside time windows (unless no windows configured)
        windows = schedule.get("time_windows", [])
        if windows and not _in_time_window(now, windows):
            # Advance next_run_at past the current window gap
            next_run = _compute_next_interval(schedule, now)
            await db["schedules"].update_one(
                {"_id": schedule["_id"]},
                {"$set": {"next_run_at": next_run}},
            )
            continue

        # For timer/interval schedules, skip if a run is already active
        is_timer = bool(
            schedule.get("cron_expression") or schedule.get("interval_seconds")
        )
        if is_timer:
            active = await db["runs"].find_one({
                "pipeline_id": schedule["pipeline_id"],
                "status": {"$in": ["queued", "running"]},
            })
            if active is not None:
                # Don't stack — just push next_run_at forward
                cron_expr = schedule.get("cron_expression")
                interval = schedule.get("interval_seconds")
                if cron_expr:
                    next_run = compute_next_run(cron_expr, now)
                else:
                    next_run = _compute_next_interval(schedule, now)
                await db["schedules"].update_one(
                    {"_id": schedule["_id"]},
                    {"$set": {"next_run_at": next_run}},
                )
                continue

        # Create run document
        run_doc = {
            "pipeline_id": schedule["pipeline_id"],
            "status": "queued",
            "created_at": now,
            "attempt": 0,
            "tags": schedule.get("tags", []),
        }
        result = await db["runs"].insert_one(run_doc)

        # Dispatch the run
        run_id = str(result.inserted_id)
        dispatch = ctx.get("dispatch_run")
        if dispatch is not None:
            await dispatch(run_id)
        elif pool is not None:
            await pool.enqueue_job(
                "execute_pipeline",
                run_id=run_id,
            )

        # Advance next_run_at
        cron_expr = schedule.get("cron_expression")
        interval = schedule.get("interval_seconds")
        if cron_expr:
            next_run = compute_next_run(cron_expr, now)
        elif interval:
            next_run = _compute_next_interval(schedule, now)
        else:
            # One-shot: disable the schedule
            await db["schedules"].update_one(
                {"_id": schedule["_id"]},
                {"$set": {"enabled": False, "last_run_at": now}},
            )
            continue

        await db["schedules"].update_one(
            {"_id": schedule["_id"]},
            {"$set": {"next_run_at": next_run, "last_run_at": now}},
        )
