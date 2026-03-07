"""Fan-in: gather parcels from a fan-out region back into a single list.

The executor automatically populates ``items`` with the fields from
every upstream parcel produced during the fan-out.
"""

from __future__ import annotations

from typing import Any, ClassVar

from pydantic import Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput
from llming_plumber.blocks.limits import check_list_size


class CollectInput(BlockInput):
    items: list[dict[str, Any]] = Field(
        default_factory=list,
        title="Items",
        description="Collected items (populated by executor during fan-in)",
    )


class CollectOutput(BlockOutput):
    items: list[dict[str, Any]]
    count: int


class CollectBlock(BaseBlock[CollectInput, CollectOutput]):
    block_type: ClassVar[str] = "collect"
    icon: ClassVar[str] = "tabler/arrows-merge"
    categories: ClassVar[list[str]] = ["core/flow"]
    description: ClassVar[str] = (
        "Fan-in: gather fan-out results back into a single list"
    )
    cache_ttl: ClassVar[int] = 0
    fan_in: ClassVar[bool] = True

    async def execute(
        self, input: CollectInput, ctx: BlockContext | None = None
    ) -> CollectOutput:
        check_list_size(input.items, label="Collect items")
        return CollectOutput(items=input.items, count=len(input.items))
