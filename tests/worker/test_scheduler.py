"""Tests for the scheduler — compute_next_run and check_schedules."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock

from bson import ObjectId
from mongomock_motor import AsyncMongoMockClient

from llming_plumber.worker.scheduler import check_schedules, compute_next_run


class TestComputeNextRun:
    def test_every_minute(self) -> None:
        """'* * * * *' from 12:30:00 -> 12:31:00."""
        after = datetime(2026, 3, 6, 12, 30, 0, tzinfo=UTC)
        result = compute_next_run("* * * * *", after)
        assert result.minute == 31
        assert result.hour == 12
        assert result.tzinfo is not None

    def test_every_hour(self) -> None:
        """'0 * * * *' from 12:30:00 -> 13:00:00."""
        after = datetime(2026, 3, 6, 12, 30, 0, tzinfo=UTC)
        result = compute_next_run("0 * * * *", after)
        assert result.hour == 13
        assert result.minute == 0

    def test_daily_at_midnight(self) -> None:
        """'0 0 * * *' from 2026-03-06 01:00 -> 2026-03-07 00:00."""
        after = datetime(2026, 3, 6, 1, 0, 0, tzinfo=UTC)
        result = compute_next_run("0 0 * * *", after)
        assert result.day == 7
        assert result.hour == 0


class TestCheckSchedules:
    async def test_enqueues_due_schedules(self) -> None:
        """Due schedules get run docs created and jobs enqueued."""
        client = AsyncMongoMockClient()
        db = client["test_plumber"]

        now = datetime(2026, 3, 6, 12, 0, 0, tzinfo=UTC)
        pipeline_oid = ObjectId()

        # Insert a due schedule
        await db["schedules"].insert_one({
            "pipeline_id": pipeline_oid,
            "enabled": True,
            "cron_expression": "*/5 * * * *",
            "next_run_at": datetime(2026, 3, 6, 11, 55, 0, tzinfo=UTC),
            "tags": ["nightly"],
        })

        # Insert a NOT-due schedule
        await db["schedules"].insert_one({
            "pipeline_id": ObjectId(),
            "enabled": True,
            "cron_expression": "0 0 * * *",
            "next_run_at": datetime(2026, 3, 7, 0, 0, 0, tzinfo=UTC),
            "tags": [],
        })

        mock_pool = AsyncMock()

        ctx: dict[str, Any] = {"db": db, "redis": mock_pool}

        from unittest.mock import patch

        with patch("llming_plumber.worker.scheduler._utcnow", return_value=now):
            await check_schedules(ctx)

        # One run doc should have been created
        runs = await db["runs"].find({}).to_list(length=100)
        assert len(runs) == 1
        assert runs[0]["pipeline_id"] == pipeline_oid
        assert runs[0]["status"] == "queued"
        assert runs[0]["tags"] == ["nightly"]

        # Pool should have been called once
        mock_pool.enqueue_job.assert_called_once()
        call_args = mock_pool.enqueue_job.call_args
        assert call_args[0][0] == "execute_pipeline"
        assert "run_id" in call_args[1]

        # Schedule should have been updated with next_run_at
        schedule = await db["schedules"].find_one({"pipeline_id": pipeline_oid})
        assert schedule is not None
        next_run_at = schedule["next_run_at"]
        if next_run_at.tzinfo is None:
            next_run_at = next_run_at.replace(tzinfo=UTC)
        assert next_run_at > now

    async def test_disabled_schedules_ignored(self) -> None:
        """Disabled schedules are not enqueued."""
        client = AsyncMongoMockClient()
        db = client["test_plumber"]

        await db["schedules"].insert_one({
            "pipeline_id": ObjectId(),
            "enabled": False,
            "cron_expression": "* * * * *",
            "next_run_at": datetime(2020, 1, 1, tzinfo=UTC),
        })

        ctx: dict[str, Any] = {"db": db, "redis": AsyncMock()}

        await check_schedules(ctx)

        runs = await db["runs"].find({}).to_list(length=100)
        assert len(runs) == 0
