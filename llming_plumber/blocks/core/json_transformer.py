"""Rename, select, or remove fields from a dict."""

from __future__ import annotations

from typing import Any, ClassVar

from pydantic import Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput


class JsonTransformerInput(BlockInput):
    data: dict[str, Any] = Field(
        title="Data",
        description="The input dictionary to transform",
    )
    rename: dict[str, str] = Field(
        default={},
        title="Rename",
        description="Mapping of old field names to new field names",
    )
    keep_only: list[str] | None = Field(
        default=None,
        title="Keep Only",
        description="List of field names to keep; all others are removed",
    )
    remove: list[str] | None = Field(
        default=None,
        title="Remove",
        description="List of field names to remove from the data",
    )


class JsonTransformerOutput(BlockOutput):
    data: dict[str, Any]


class JsonTransformerBlock(BaseBlock[JsonTransformerInput, JsonTransformerOutput]):
    block_type: ClassVar[str] = "json_transformer"
    icon: ClassVar[str] = "tabler/transform"
    categories: ClassVar[list[str]] = ["core/transform"]
    description: ClassVar[str] = "Rename, select, or remove fields from data"
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self, input: JsonTransformerInput, ctx: BlockContext | None = None
    ) -> JsonTransformerOutput:
        result: dict[str, Any] = dict(input.data)

        # Apply rename first
        for old_key, new_key in input.rename.items():
            if old_key in result:
                result[new_key] = result.pop(old_key)

        # Apply keep_only filter
        if input.keep_only is not None:
            result = {k: v for k, v in result.items() if k in input.keep_only}

        # Apply remove filter
        if input.remove is not None:
            for key in input.remove:
                result.pop(key, None)

        return JsonTransformerOutput(data=result)
