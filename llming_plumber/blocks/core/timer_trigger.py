"""Timer trigger block — entry point for scheduled / interval pipelines.

Emits the current timestamp plus scheduling metadata. When
``interval_seconds`` is configured the UI editor auto-repeats the
pipeline on that cadence.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import ClassVar

from pydantic import Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput


class TimerTriggerInput(BlockInput):
    interval_seconds: int = Field(
        default=0,
        title="Interval (seconds)",
        description="Auto-repeat every N seconds (0 = manual only)",
        ge=0,
    )
    cron_expression: str = Field(
        default="",
        title="Cron Expression",
        description="Cron schedule, e.g. '0 8 * * *' for daily at 08:00 (used by the scheduler, not the UI)",
    )
    time_window_start: str = Field(
        default="",
        title="Active Window Start",
        description="Start of active window, e.g. '08:00' (empty = always active)",
    )
    time_window_end: str = Field(
        default="",
        title="Active Window End",
        description="End of active window, e.g. '18:00'",
    )
    weekdays: str = Field(
        default="",
        title="Weekdays",
        description="Active weekdays, e.g. 'Mon,Tue,Wed,Thu,Fri' (empty = every day)",
    )


class TimerTriggerOutput(BlockOutput):
    triggered_at: str
    date: str
    time: str
    weekday: str
    hour: int
    minute: int
    iso: str


class TimerTriggerBlock(BaseBlock[TimerTriggerInput, TimerTriggerOutput]):
    block_type: ClassVar[str] = "timer_trigger"
    icon: ClassVar[str] = "tabler/alarm"
    categories: ClassVar[list[str]] = ["core/trigger"]
    description: ClassVar[str] = (
        "Timer trigger — emits the current timestamp. "
        "Set interval_seconds for auto-repeat in the editor."
    )
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self,
        input: TimerTriggerInput,
        ctx: BlockContext | None = None,
    ) -> TimerTriggerOutput:
        now = datetime.now(UTC)
        return TimerTriggerOutput(
            triggered_at=now.isoformat(),
            date=now.strftime("%Y-%m-%d"),
            time=now.strftime("%H:%M:%S"),
            weekday=now.strftime("%A"),
            hour=now.hour,
            minute=now.minute,
            iso=now.isoformat(),
        )
