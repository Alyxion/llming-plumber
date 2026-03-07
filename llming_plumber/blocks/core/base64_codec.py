"""Encode or decode base64 text."""

from __future__ import annotations

import base64
from typing import ClassVar

from pydantic import Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput
from llming_plumber.blocks.limits import check_base64_size, check_file_size


class Base64CodecInput(BlockInput):
    text: str = Field(
        title="Text",
        description="The text to encode or decode",
        json_schema_extra={"widget": "textarea"},
    )
    mode: str = Field(
        default="encode",
        title="Mode",
        description="Whether to encode or decode the text",
        json_schema_extra={"widget": "select", "options": ["encode", "decode"]},
    )


class Base64CodecOutput(BlockOutput):
    result: str


class Base64CodecBlock(BaseBlock[Base64CodecInput, Base64CodecOutput]):
    block_type: ClassVar[str] = "base64_codec"
    icon: ClassVar[str] = "tabler/binary-tree"
    categories: ClassVar[list[str]] = ["core/transform"]
    description: ClassVar[str] = "Encode or decode Base64"
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self, input: Base64CodecInput, ctx: BlockContext | None = None
    ) -> Base64CodecOutput:
        if input.mode == "encode":
            check_file_size(len(input.text.encode()), label="Base64 encode input")
            result = base64.b64encode(input.text.encode()).decode()
        elif input.mode == "decode":
            check_base64_size(input.text, label="Base64 decode input")
            result = base64.b64decode(input.text.encode()).decode()
        else:
            msg = f"Unsupported mode: {input.mode}. Use 'encode' or 'decode'."
            raise ValueError(msg)
        return Base64CodecOutput(result=result)
