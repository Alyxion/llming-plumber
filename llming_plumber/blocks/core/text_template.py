"""Render text templates with safe expression interpolation.

Supports ``{expression}`` placeholders evaluated via the safe
expression evaluator.  Variables come from the ``values`` dict
and/or any extra fields piped into the block::

    template = "Hello {name}, you are #{index + 1}!"
    # With name="Alice", index=0 → "Hello Alice, you are #1!"
"""

from __future__ import annotations

from typing import Any, ClassVar

from pydantic import ConfigDict, Field

from llming_plumber.blocks.base import (
    BaseBlock,
    BlockContext,
    BlockInput,
    BlockOutput,
)
from llming_plumber.blocks.core.safe_eval import render_template


class TextTemplateInput(BlockInput):
    model_config = ConfigDict(extra="allow")

    template: str = Field(
        title="Template",
        description=(
            "Text with {expression} placeholders. "
            "Use {{/}} for literal braces."
        ),
        json_schema_extra={
            "widget": "textarea",
            "placeholder": "Hello {name}",
        },
    )
    values: dict[str, Any] = Field(
        default_factory=dict,
        title="Values",
        description="Variables for template interpolation",
    )


class TextTemplateOutput(BlockOutput):
    rendered: str


class TextTemplateBlock(BaseBlock[TextTemplateInput, TextTemplateOutput]):
    block_type: ClassVar[str] = "text_template"
    icon: ClassVar[str] = "tabler/template"
    categories: ClassVar[list[str]] = ["core/transform"]
    description: ClassVar[str] = (
        "Render text templates with safe expression interpolation"
    )
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self,
        input: TextTemplateInput,
        ctx: BlockContext | None = None,
    ) -> TextTemplateOutput:
        # Merge explicit values dict + any extra piped fields
        variables: dict[str, Any] = {**input.values}
        if input.model_extra:
            variables.update(input.model_extra)
        rendered = render_template(input.template, variables)
        return TextTemplateOutput(rendered=rendered)
