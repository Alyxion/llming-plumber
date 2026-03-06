"""Merge multiple item lists into one."""

from __future__ import annotations

from typing import Any, ClassVar

from pydantic import Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput


class MergeInput(BlockInput):
    item_lists: list[list[dict[str, Any]]] = Field(
        title="Item Lists",
        description="Multiple lists of items to merge into a single list",
    )


class MergeOutput(BlockOutput):
    items: list[dict[str, Any]]
    source_count: int


class MergeBlock(BaseBlock[MergeInput, MergeOutput]):
    block_type: ClassVar[str] = "merge"
    icon: ClassVar[str] = "tabler/git-merge"
    categories: ClassVar[list[str]] = ["core/flow"]
    description: ClassVar[str] = "Merge multiple item lists into one"
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self, input: MergeInput, ctx: BlockContext | None = None
    ) -> MergeOutput:
        merged: list[dict[str, Any]] = []
        for item_list in input.item_lists:
            merged.extend(item_list)
        return MergeOutput(items=merged, source_count=len(input.item_lists))
