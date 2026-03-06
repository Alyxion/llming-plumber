from __future__ import annotations

from datetime import UTC, datetime

from pydantic import BaseModel, Field


class Schedule(BaseModel):
    """A cron or interval schedule for automatic pipeline runs."""

    id: str = ""
    pipeline_id: str = ""
    cron_expression: str | None = None
    interval_seconds: int | None = None
    enabled: bool = True
    next_run_at: datetime | None = None
    last_run_at: datetime | None = None
    tags: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
