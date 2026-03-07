"""Remove duplicates by field value."""

from __future__ import annotations

from typing import Any, ClassVar

from pydantic import Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput
from llming_plumber.blocks.limits import check_list_size


class DeduplicatorInput(BlockInput):
    items: list[dict[str, Any]] = Field(
        title="Items",
        description="List of items to deduplicate",
    )
    field: str = Field(
        title="Field",
        description="The field name to check for duplicate values",
        json_schema_extra={"placeholder": "id"},
    )


class DeduplicatorOutput(BlockOutput):
    items: list[dict[str, Any]]
    duplicates_removed: int


class DeduplicatorBlock(BaseBlock[DeduplicatorInput, DeduplicatorOutput]):
    block_type: ClassVar[str] = "deduplicator"
    icon: ClassVar[str] = "tabler/copy-off"
    categories: ClassVar[list[str]] = ["core/flow"]
    description: ClassVar[str] = "Remove duplicate items by field value"
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self, input: DeduplicatorInput, ctx: BlockContext | None = None
    ) -> DeduplicatorOutput:
        check_list_size(input.items, label="Deduplicator input")
        seen: set[Any] = set()
        unique: list[dict[str, Any]] = []
        for item in input.items:
            key = item.get(input.field)
            hashable_key = str(key)
            if hashable_key not in seen:
                seen.add(hashable_key)
                unique.append(item)
        return DeduplicatorOutput(
            items=unique,
            duplicates_removed=len(input.items) - len(unique),
        )
