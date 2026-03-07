"""Parse YAML text into structured data."""

from __future__ import annotations

from typing import Any, ClassVar

from pydantic import Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput


class YamlParserInput(BlockInput):
    content: str = Field(
        title="Content",
        description="YAML text to parse",
        json_schema_extra={"widget": "textarea"},
    )
    multi_document: bool = Field(
        default=False,
        title="Multi-Document",
        description="Parse multi-document YAML (separated by ---)",
    )


class YamlParserOutput(BlockOutput):
    data: Any
    document_count: int


class YamlParserBlock(BaseBlock[YamlParserInput, YamlParserOutput]):
    block_type: ClassVar[str] = "yaml_parser"
    icon: ClassVar[str] = "tabler/file-code"
    categories: ClassVar[list[str]] = ["documents", "yaml"]
    description: ClassVar[str] = "Parse YAML text into structured data"
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self, input: YamlParserInput, ctx: BlockContext | None = None
    ) -> YamlParserOutput:
        import yaml

        if input.multi_document:
            docs = list(yaml.safe_load_all(input.content))
            return YamlParserOutput(data=docs, document_count=len(docs))

        data = yaml.safe_load(input.content)
        return YamlParserOutput(data=data, document_count=1)
