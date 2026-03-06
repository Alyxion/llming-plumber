"""Split a text field into multiple items."""

from __future__ import annotations

from typing import ClassVar

from pydantic import Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput


class SplitTextInput(BlockInput):
    text: str = Field(
        title="Text",
        description="The text to split into chunks",
        json_schema_extra={"widget": "textarea"},
    )
    delimiter: str = Field(
        default="\n",
        title="Delimiter",
        description="The string to split on",
        json_schema_extra={"placeholder": "\\n"},
    )
    strip_empty: bool = Field(
        default=True,
        title="Strip Empty",
        description="Remove empty chunks from the result",
        json_schema_extra={"widget": "toggle"},
    )


class SplitTextOutput(BlockOutput):
    chunks: list[str]
    chunk_count: int


class SplitTextBlock(BaseBlock[SplitTextInput, SplitTextOutput]):
    block_type: ClassVar[str] = "split_text"
    icon: ClassVar[str] = "tabler/separator"
    categories: ClassVar[list[str]] = ["core/transform"]
    description: ClassVar[str] = "Split text into multiple chunks"
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self, input: SplitTextInput, ctx: BlockContext | None = None
    ) -> SplitTextOutput:
        parts = input.text.split(input.delimiter)
        if input.strip_empty:
            parts = [p for p in parts if p.strip()]
        return SplitTextOutput(chunks=parts, chunk_count=len(parts))
