"""Render text templates with field substitution."""

from __future__ import annotations

from typing import Any, ClassVar

from pydantic import Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput


class TextTemplateInput(BlockInput):
    template: str = Field(
        title="Template",
        description="Text template with {field} placeholders for substitution",
        json_schema_extra={"widget": "textarea", "placeholder": "Hello {name}"},
    )
    values: dict[str, Any] = Field(
        title="Values",
        description="Dictionary of field names and their replacement values",
    )


class TextTemplateOutput(BlockOutput):
    rendered: str


class TextTemplateBlock(BaseBlock[TextTemplateInput, TextTemplateOutput]):
    block_type: ClassVar[str] = "text_template"
    icon: ClassVar[str] = "tabler/template"
    categories: ClassVar[list[str]] = ["core/transform"]
    description: ClassVar[str] = "Render text templates with field substitution"
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self, input: TextTemplateInput, ctx: BlockContext | None = None
    ) -> TextTemplateOutput:
        rendered = input.template.format_map(input.values)
        return TextTemplateOutput(rendered=rendered)
