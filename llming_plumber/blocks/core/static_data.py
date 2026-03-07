"""Embed static data directly in a pipeline graph."""

from __future__ import annotations

import base64
from typing import ClassVar

from pydantic import Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput

MAX_CONTENT_BYTES = 256 * 1024  # 256 KB default limit


class StaticDataInput(BlockInput):
    content: str = Field(
        title="Content",
        description="Text content or base64-encoded binary data",
        json_schema_extra={"widget": "textarea", "rows": 10},
    )
    mime_type: str = Field(
        default="text/plain",
        title="MIME Type",
        description="Content type of the embedded data",
        json_schema_extra={
            "widget": "select",
            "options": [
                "text/plain",
                "text/csv",
                "text/markdown",
                "application/json",
                "application/yaml",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                "application/vnd.openxmlformats-officedocument.presentationml.presentation",
                "application/pdf",
                "application/octet-stream",
            ],
        },
    )
    is_base64: bool = Field(
        default=False,
        title="Base64 Encoded",
        description="Whether the content is base64-encoded binary data",
        json_schema_extra={"widget": "toggle"},
    )
    filename: str = Field(
        default="",
        title="Filename",
        description="Optional filename hint for downstream blocks",
        json_schema_extra={"placeholder": "data.xlsx"},
    )
    max_size_kb: int = Field(
        default=256,
        title="Max Size (KB)",
        description="Maximum allowed content size in kilobytes",
        json_schema_extra={"min": 1, "max": 1024},
    )


class StaticDataOutput(BlockOutput):
    content: str
    mime_type: str
    size_bytes: int
    filename: str


class StaticDataBlock(BaseBlock[StaticDataInput, StaticDataOutput]):
    block_type: ClassVar[str] = "static_data"
    icon: ClassVar[str] = "tabler/database-import"
    categories: ClassVar[list[str]] = ["core", "data"]
    description: ClassVar[str] = "Embed small static data directly in the pipeline"
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self, input: StaticDataInput, ctx: BlockContext | None = None
    ) -> StaticDataOutput:
        max_bytes = input.max_size_kb * 1024

        if input.is_base64:
            raw = base64.b64decode(input.content)
            size = len(raw)
        else:
            size = len(input.content.encode())

        if size > max_bytes:
            msg = (
                f"Content size {size} bytes exceeds limit "
                f"of {max_bytes} bytes ({input.max_size_kb} KB)"
            )
            raise ValueError(msg)

        return StaticDataOutput(
            content=input.content,
            mime_type=input.mime_type,
            size_bytes=size,
            filename=input.filename,
        )
