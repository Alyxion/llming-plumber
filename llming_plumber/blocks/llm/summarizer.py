"""Text Summarizer block — condense text using an LLM."""

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

_LENGTH_HINTS: dict[str, str] = {
    "brief": "Keep the summary very short (2-3 sentences max).",
    "moderate": "Provide a moderate-length summary.",
    "detailed": (
        "Provide a thorough and detailed summary"
        " covering all key points."
    ),
}

_STYLE_HINTS: dict[str, str] = {
    "bullet_points": "Format the summary as bullet points.",
    "paragraph": "Write the summary as prose paragraphs.",
    "executive_summary": (
        "Write an executive summary with a clear headline,"
        " key findings, and a conclusion."
    ),
}

_TEMPLATE = _client.load_prompt("summarizer")


class SummarizerInput(BlockInput):
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
        description="Text to summarize",
        json_schema_extra={"widget": "textarea"},
    )
    max_length: str = Field(
        default="moderate",
        title="Max Length",
        description="Desired summary length",
        json_schema_extra={
            "widget": "select",
            "options": ["brief", "moderate", "detailed"],
        },
    )
    style: str = Field(
        default="paragraph",
        title="Style",
        description="Output style for the summary",
        json_schema_extra={
            "widget": "select",
            "options": [
                "bullet_points",
                "paragraph",
                "executive_summary",
            ],
        },
    )


class SummarizerOutput(BlockOutput):
    summary: str


class SummarizerBlock(
    BaseBlock[SummarizerInput, SummarizerOutput],
):
    block_type: ClassVar[str] = "llm_summarizer"
    icon: ClassVar[str] = "tabler/file-text"
    categories: ClassVar[list[str]] = ["llm/text"]
    description: ClassVar[str] = (
        "Summarize text using an LLM"
    )
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self,
        input: SummarizerInput,
        ctx: BlockContext | None = None,
    ) -> SummarizerOutput:
        length_hint = _LENGTH_HINTS.get(
            input.max_length, _LENGTH_HINTS["moderate"]
        )
        style_hint = _STYLE_HINTS.get(
            input.style, _STYLE_HINTS["paragraph"]
        )
        system = _TEMPLATE.format(
            length_hint=length_hint,
            style_hint=style_hint,
        )
        response = await _client.prompt(
            provider=input.provider,
            model=input.model,
            system=system,
            user=input.text,
        )
        return SummarizerOutput(summary=response)
