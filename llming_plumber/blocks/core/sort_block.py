"""Sort items by a field."""

from __future__ import annotations

from typing import Any, ClassVar

from pydantic import Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput
from llming_plumber.blocks.limits import check_list_size


class SortInput(BlockInput):
    items: list[dict[str, Any]] = Field(
        title="Items",
        description="List of items to sort",
    )
    field: str = Field(
        title="Field",
        description="The field name to sort by",
        json_schema_extra={"placeholder": "name"},
    )
    descending: bool = Field(
        default=False,
        title="Descending",
        description="Sort in descending order when enabled",
        json_schema_extra={"widget": "toggle"},
    )


class SortOutput(BlockOutput):
    items: list[dict[str, Any]]


class SortBlock(BaseBlock[SortInput, SortOutput]):
    block_type: ClassVar[str] = "sort"
    icon: ClassVar[str] = "tabler/sort-ascending"
    categories: ClassVar[list[str]] = ["core/flow"]
    description: ClassVar[str] = "Sort items by a field value"
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self, input: SortInput, ctx: BlockContext | None = None
    ) -> SortOutput:
        check_list_size(input.items, label="Sort input")
        sorted_items = sorted(
            input.items,
            key=lambda item: item.get(input.field, ""),
            reverse=input.descending,
        )
        return SortOutput(items=sorted_items)
