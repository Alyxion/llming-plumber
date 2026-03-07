"""Render Markdown to HTML."""

from __future__ import annotations

import html.parser
from typing import ClassVar

from pydantic import Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput


class _TextExtractor(html.parser.HTMLParser):
    """Strip HTML tags and extract plain text."""

    def __init__(self) -> None:
        super().__init__()
        self._parts: list[str] = []

    def handle_data(self, data: str) -> None:
        self._parts.append(data)

    def get_text(self) -> str:
        return "".join(self._parts)


class MarkdownRendererInput(BlockInput):
    content: str = Field(
        title="Content",
        description="Markdown text to render",
        json_schema_extra={"widget": "textarea"},
    )
    extensions: list[str] = Field(
        default=["tables", "fenced_code"],
        title="Extensions",
        description="Markdown extensions to enable",
    )


class MarkdownRendererOutput(BlockOutput):
    html: str
    text: str


class MarkdownRendererBlock(BaseBlock[MarkdownRendererInput, MarkdownRendererOutput]):
    block_type: ClassVar[str] = "markdown_renderer"
    icon: ClassVar[str] = "tabler/markdown"
    categories: ClassVar[list[str]] = ["documents", "markdown", "rendering"]
    description: ClassVar[str] = "Render Markdown to HTML"
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self, input: MarkdownRendererInput, ctx: BlockContext | None = None
    ) -> MarkdownRendererOutput:
        import markdown as md

        rendered_html = md.markdown(input.content, extensions=input.extensions)

        extractor = _TextExtractor()
        extractor.feed(rendered_html)
        plain_text = extractor.get_text()

        return MarkdownRendererOutput(html=rendered_html, text=plain_text)
