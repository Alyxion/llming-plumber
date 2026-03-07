"""Generate a sequence of numbered items for iteration.

Works like Python's ``range()`` — produces items that the executor
fans out over so downstream blocks run once per index.
"""

from __future__ import annotations

from typing import Any, ClassVar

from pydantic import Field

from llming_plumber.blocks.base import (
    BaseBlock,
    BlockContext,
    BlockInput,
    BlockOutput,
)
from llming_plumber.blocks.limits import MAX_FAN_OUT_ITEMS, check_list_size


class RangeInput(BlockInput):
    start: int = Field(
        default=0,
        title="Start",
        description="First value (inclusive)",
    )
    stop: int = Field(
        title="Stop",
        description="End value (exclusive)",
    )
    step: int = Field(
        default=1,
        title="Step",
        description="Increment per iteration (must not be 0)",
    )


class RangeOutput(BlockOutput):
    items: list[dict[str, Any]]
    total: int


class RangeBlock(BaseBlock[RangeInput, RangeOutput]):
    block_type: ClassVar[str] = "range"
    icon: ClassVar[str] = "tabler/list-numbers"
    categories: ClassVar[list[str]] = ["core/flow"]
    description: ClassVar[str] = (
        "Generate a numbered sequence for iteration (like Python range)"
    )
    cache_ttl: ClassVar[int] = 0
    fan_out_field: ClassVar[str | None] = "items"

    async def execute(
        self,
        input: RangeInput,
        ctx: BlockContext | None = None,
    ) -> RangeOutput:
        if input.step == 0:
            msg = "Step must not be 0"
            raise ValueError(msg)

        # Compute count WITHOUT allocating the list first
        count = len(range(input.start, input.stop, input.step))
        check_list_size(
            count,
            limit=MAX_FAN_OUT_ITEMS,
            label="Range items",
        )
        items = [{"index": i} for i in range(input.start, input.stop, input.step)]
        return RangeOutput(items=items, total=count)
