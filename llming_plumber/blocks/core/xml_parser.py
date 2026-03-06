"""Parse XML text and extract elements by tag."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import ClassVar

from pydantic import Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput


class XmlParserInput(BlockInput):
    xml_text: str = Field(
        title="XML Text",
        description="Raw XML text to parse",
        json_schema_extra={"widget": "textarea"},
    )
    element_tag: str = Field(
        title="Element Tag",
        description="XML tag name to search for",
        json_schema_extra={"placeholder": "item"},
    )
    attributes: list[str] = Field(
        default=[],
        title="Attributes",
        description="List of attribute names to extract from matched elements",
    )


class XmlParserOutput(BlockOutput):
    elements: list[dict[str, str]]
    element_count: int


class XmlParserBlock(BaseBlock[XmlParserInput, XmlParserOutput]):
    block_type: ClassVar[str] = "xml_parser"
    icon: ClassVar[str] = "tabler/file-code"
    categories: ClassVar[list[str]] = ["core/transform", "documents/parsing"]
    description: ClassVar[str] = "Extract elements from XML by tag name"
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self, input: XmlParserInput, ctx: BlockContext | None = None
    ) -> XmlParserOutput:
        root = ET.fromstring(input.xml_text)
        found = root.findall(f".//{input.element_tag}")
        elements: list[dict[str, str]] = []
        for elem in found:
            entry: dict[str, str] = {"text": elem.text or ""}
            for attr in input.attributes:
                entry[attr] = elem.get(attr, "")
            elements.append(entry)
        return XmlParserOutput(elements=elements, element_count=len(elements))
