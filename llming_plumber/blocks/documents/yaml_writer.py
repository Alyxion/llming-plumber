"""Write structured data to YAML text."""

from __future__ import annotations

from typing import Any, ClassVar

from pydantic import Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput


class YamlWriterInput(BlockInput):
    data: Any = Field(
        title="Data",
        description="Dictionary or list to serialize as YAML",
    )
    default_flow_style: bool = Field(
        default=False,
        title="Flow Style",
        description="Use flow style (compact inline format) instead of block style",
    )


class YamlWriterOutput(BlockOutput):
    content: str


class YamlWriterBlock(BaseBlock[YamlWriterInput, YamlWriterOutput]):
    block_type: ClassVar[str] = "yaml_writer"
    icon: ClassVar[str] = "tabler/file-code"
    categories: ClassVar[list[str]] = ["documents", "yaml"]
    description: ClassVar[str] = "Write structured data to YAML text"
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self, input: YamlWriterInput, ctx: BlockContext | None = None
    ) -> YamlWriterOutput:
        import yaml

        content = yaml.dump(
            input.data,
            default_flow_style=input.default_flow_style,
            allow_unicode=True,
            sort_keys=False,
        )
        return YamlWriterOutput(content=content)
