"""System clock block — emits the current date/time components.

Useful as a check source for periodic guards, or anywhere the current
time is needed as structured data.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import ClassVar

from llming_plumber.blocks.base import (
    BaseBlock,
    BlockContext,
    BlockInput,
    BlockOutput,
)


class SystemClockInput(BlockInput):
    pass


class SystemClockOutput(BlockOutput):
    iso: str = ""
    date: str = ""
    time: str = ""
    year: int = 0
    month: int = 0
    day: int = 0
    hour: int = 0
    minute: int = 0
    second: int = 0
    weekday: str = ""
    timestamp: int = 0


class SystemClockBlock(BaseBlock[SystemClockInput, SystemClockOutput]):
    block_type: ClassVar[str] = "system_clock"
    icon: ClassVar[str] = "tabler/clock"
    categories: ClassVar[list[str]] = ["core/utility"]
    description: ClassVar[str] = "Emit the current date and time as structured fields"
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self,
        input: SystemClockInput,
        ctx: BlockContext | None = None,
    ) -> SystemClockOutput:
        now = datetime.now(UTC)
        return SystemClockOutput(
            iso=now.isoformat(),
            date=now.strftime("%Y-%m-%d"),
            time=now.strftime("%H:%M:%S"),
            year=now.year,
            month=now.month,
            day=now.day,
            hour=now.hour,
            minute=now.minute,
            second=now.second,
            weekday=now.strftime("%A"),
            timestamp=int(now.timestamp()),
        )
