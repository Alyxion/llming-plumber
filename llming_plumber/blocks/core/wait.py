"""Pause execution for a specified duration."""

from __future__ import annotations

import asyncio
import time
from typing import ClassVar

from pydantic import Field

from llming_plumber.blocks.base import (
    BaseBlock,
    BlockContext,
    BlockInput,
    BlockOutput,
)
from llming_plumber.blocks.limits import MAX_WAIT_SECONDS


class WaitInput(BlockInput):
    seconds: float = Field(
        default=1.0,
        title="Seconds",
        description="How long to wait",
        json_schema_extra={"min": 0, "max": MAX_WAIT_SECONDS},
    )


class WaitOutput(BlockOutput):
    waited_seconds: float


class WaitBlock(BaseBlock[WaitInput, WaitOutput]):
    block_type: ClassVar[str] = "wait"
    icon: ClassVar[str] = "tabler/clock-pause"
    categories: ClassVar[list[str]] = ["core/flow"]
    description: ClassVar[str] = "Pause execution for a specified duration"
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self,
        input: WaitInput,
        ctx: BlockContext | None = None,
    ) -> WaitOutput:
        capped = min(max(input.seconds, 0), MAX_WAIT_SECONDS)
        start = time.monotonic()
        await asyncio.sleep(capped)
        actual = time.monotonic() - start
        return WaitOutput(waited_seconds=round(actual, 3))
