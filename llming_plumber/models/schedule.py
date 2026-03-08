from __future__ import annotations

from datetime import UTC, datetime

from pydantic import BaseModel, Field


class TimeWindow(BaseModel):
    """A time-of-day window during which a schedule is active."""

    start: str = "00:00"
    end: str = "23:59"
    weekdays: list[int] = Field(
        default_factory=lambda: [0, 1, 2, 3, 4, 5, 6],
        description="Weekdays (0=Mon, 6=Sun)",
    )


class Schedule(BaseModel):
    """A cron or interval schedule for automatic pipeline runs.

    Supports three modes:
    - **cron_expression**: classic cron (``0 8 * * *``).
    - **interval_seconds**: run every N seconds.
    - **Both**: interval within the cron-defined slots.

    Time windows restrict when the schedule is active (e.g. only
    during work hours Mon–Fri). Outside the window the scheduler
    skips the schedule until the next window opens.

    ``interval_multiplier_off_hours`` scales the interval when
    outside the primary time window but still within weekdays.
    E.g. 4.0 means "4x slower at night".
    """

    id: str = ""
    pipeline_id: str = ""
    cron_expression: str | None = None
    interval_seconds: int | None = None
    enabled: bool = True
    next_run_at: datetime | None = None
    last_run_at: datetime | None = None
    time_windows: list[TimeWindow] = Field(default_factory=list)
    interval_multiplier_off_hours: float = Field(
        default=1.0,
        description="Multiply interval by this factor outside time windows",
    )
    tags: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
