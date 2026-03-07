"""Fan-out: split a list of items into individual parcels.

The executor runs every downstream block once per item until a
CollectBlock gathers the results back into a single list.
"""

from __future__ import annotations

from typing import Any, ClassVar

from pydantic import Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput
from llming_plumber.blocks.limits import MAX_FAN_OUT_ITEMS, check_list_size


class SplitInput(BlockInput):
    items: list[dict[str, Any]] = Field(
        title="Items",
        description="List of items to fan out over",
    )


class SplitOutput(BlockOutput):
    items: list[dict[str, Any]]
    total: int


class SplitBlock(BaseBlock[SplitInput, SplitOutput]):
    block_type: ClassVar[str] = "split"
    icon: ClassVar[str] = "tabler/arrows-split-2"
    categories: ClassVar[list[str]] = ["core/flow"]
    description: ClassVar[str] = (
        "Fan-out: run the downstream branch once per item"
    )
    cache_ttl: ClassVar[int] = 0
    fan_out_field: ClassVar[str | None] = "items"

    async def execute(
        self, input: SplitInput, ctx: BlockContext | None = None
    ) -> SplitOutput:
        check_list_size(
            input.items, limit=MAX_FAN_OUT_ITEMS, label="Split items",
        )
        return SplitOutput(items=input.items, total=len(input.items))
