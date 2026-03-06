"""LLM Text Rewriter block."""

from __future__ import annotations

from typing import ClassVar

from pydantic import Field

from llming_plumber.blocks.base import (
    BaseBlock,
    BlockContext,
    BlockInput,
    BlockOutput,
)
from llming_plumber.blocks.llm import _client


class RewriterInput(BlockInput):
    provider: str = Field(
        default="openai",
        title="Provider",
        description="LLM provider to use",
        json_schema_extra={
            "widget": "select",
            "options": [
                "openai",
                "azure_openai",
                "anthropic",
                "google",
                "mistral",
            ],
        },
    )
    model: str = Field(
        title="Model",
        description="Model identifier",
        json_schema_extra={"placeholder": "gpt-5-nano"},
    )
    text: str = Field(
        title="Text",
        description="Text to rewrite",
        json_schema_extra={"widget": "textarea"},
    )
    style: str = Field(
        default="formal",
        title="Style",
        description="Rewriting style",
        json_schema_extra={
            "widget": "select",
            "options": [
                "formal",
                "casual",
                "technical",
                "simple",
                "creative",
            ],
        },
    )
    instructions: str = Field(
        default="",
        title="Instructions",
        description="Additional rewriting instructions",
        json_schema_extra={
            "placeholder": (
                "Additional rewriting instructions"
            ),
        },
    )


class RewriterOutput(BlockOutput):
    rewritten_text: str


class RewriterBlock(
    BaseBlock[RewriterInput, RewriterOutput],
):
    block_type: ClassVar[str] = "llm_rewriter"
    icon: ClassVar[str] = "tabler/edit"
    categories: ClassVar[list[str]] = ["llm/text"]
    description: ClassVar[str] = (
        "Rewrite text in a specified style"
    )

    async def execute(
        self,
        input: RewriterInput,
        ctx: BlockContext | None = None,
    ) -> RewriterOutput:
        system = _client.load_prompt("rewriter").format(
            style=input.style,
        )
        if input.instructions:
            system += (
                f"\nAdditional instructions: "
                f"{input.instructions}"
            )
        response = await _client.prompt(
            provider=input.provider,
            model=input.model,
            system=system,
            user=input.text,
        )
        return RewriterOutput(rewritten_text=response)
