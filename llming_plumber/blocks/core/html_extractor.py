"""Extract text or HTML content from HTML documents using CSS selectors."""

from __future__ import annotations

from typing import ClassVar

from bs4 import BeautifulSoup
from pydantic import Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput


class HtmlExtractorInput(BlockInput):
    html: str = Field(
        title="HTML",
        description="The HTML document to extract content from",
        json_schema_extra={"widget": "textarea"},
    )
    selector: str = Field(
        default="body",
        title="Selector",
        description="CSS selector to target elements",
        json_schema_extra={"widget": "code", "placeholder": "body"},
    )
    extract_mode: str = Field(
        default="text",
        title="Extract Mode",
        description="Whether to extract text content or raw HTML",
        json_schema_extra={"widget": "select", "options": ["text", "html"]},
    )


class HtmlExtractorOutput(BlockOutput):
    content: str
    element_count: int


class HtmlExtractorBlock(BaseBlock[HtmlExtractorInput, HtmlExtractorOutput]):
    block_type: ClassVar[str] = "html_extractor"
    icon: ClassVar[str] = "tabler/html"
    categories: ClassVar[list[str]] = ["core/transform", "web"]
    description: ClassVar[str] = "Extract content from HTML using CSS selectors"
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self, input: HtmlExtractorInput, ctx: BlockContext | None = None
    ) -> HtmlExtractorOutput:
        soup = BeautifulSoup(input.html, "html.parser")
        elements = soup.select(input.selector)

        parts: list[str] = []
        for el in elements:
            if input.extract_mode == "html":
                parts.append(str(el))
            else:
                parts.append(el.get_text(separator=" ", strip=True))

        content = "\n".join(parts)
        return HtmlExtractorOutput(content=content, element_count=len(elements))
