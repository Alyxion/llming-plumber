"""Content Summarizer block — summarize text content using an LLM.

A single-item block that receives text via pipe and produces a summary.
For batch usage, wire inside a ``Split → Content Summarizer → Collect``
fan-out region.

Includes smart truncation (head + tail) for long inputs to stay within
LLM context limits while preserving both opening context and conclusion.
"""

from __future__ import annotations

from typing import ClassVar

from pydantic import Field

from llming_plumber.blocks.base import (
    BaseBlock,
    BlockContext,
    BlockInput,
    BlockOutput,
)
from llming_plumber.blocks.llm import _client, _defaults

_DEFAULT_PROMPT = _client.load_prompt("content_summarizer")


def _truncate(text: str, max_chars: int) -> tuple[str, bool]:
    """Truncate text preserving start and end for context.

    Returns (truncated_text, was_truncated).
    """
    if len(text) <= max_chars:
        return text, False
    head = max_chars * 2 // 3
    tail = max_chars - head
    return text[:head] + "\n\n[...truncated...]\n\n" + text[-tail:], True


class ContentSummarizerInput(BlockInput):
    provider: str = Field(
        default_factory=_defaults.provider_factory("fast"),
        title="Provider",
        description="LLM provider to use",
        json_schema_extra={
            "widget": "combobox",
            "options": _defaults.PROVIDERS,
        },
    )
    model: str = Field(
        default_factory=_defaults.model_factory("fast"),
        title="Model",
        description="Model identifier",
        json_schema_extra={
            "widget": "combobox",
            "options_ref": "llm_models",
        },
    )
    text: str = Field(
        title="Text",
        description="Text content to summarize",
        json_schema_extra={"widget": "textarea"},
    )
    system_prompt: str = Field(
        default="",
        title="System Prompt",
        description="Custom system prompt (leave empty for default)",
        json_schema_extra={"widget": "textarea"},
    )
    max_input_chars: int = Field(
        default=12000,
        title="Max Input Chars",
        description="Truncate input text to this length before sending to LLM",
        json_schema_extra={"min": 1000, "max": 100000},
    )


class ContentSummarizerOutput(BlockOutput):
    summary: str = ""
    source_length: int = 0
    was_truncated: bool = False
    model: str = ""


class ContentSummarizerBlock(
    BaseBlock[ContentSummarizerInput, ContentSummarizerOutput],
):
    llm_tier: ClassVar[str] = "fast"
    block_type: ClassVar[str] = "content_summarizer"
    icon: ClassVar[str] = "tabler/file-text-ai"
    categories: ClassVar[list[str]] = ["llm/text"]
    description: ClassVar[str] = (
        "Summarize text content using an LLM with smart truncation"
    )
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self,
        input: ContentSummarizerInput,
        ctx: BlockContext | None = None,
    ) -> ContentSummarizerOutput:
        source_length = len(input.text)
        truncated_text, was_truncated = _truncate(
            input.text, input.max_input_chars,
        )

        system = input.system_prompt.strip() or _DEFAULT_PROMPT

        if ctx and was_truncated:
            await ctx.log(
                f"Input truncated: {source_length} → {len(truncated_text)} chars"
            )

        summary = await _client.prompt(
            provider=input.provider,
            model=input.model,
            system=system,
            user=truncated_text,
        )

        return ContentSummarizerOutput(
            summary=summary,
            source_length=source_length,
            was_truncated=was_truncated,
            model=input.model,
        )


class SinkScannerBlock(
    BaseBlock[ContentSummarizerInput, ContentSummarizerOutput],
):
    """Backwards-compatible alias — existing pipelines use block_type='sink_scanner'."""

    llm_tier: ClassVar[str] = "fast"
    block_type: ClassVar[str] = "sink_scanner"
    icon: ClassVar[str] = "tabler/file-text-ai"
    categories: ClassVar[list[str]] = ["llm/text"]
    description: ClassVar[str] = (
        "Content Summarizer (legacy name)"
    )
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self,
        input: ContentSummarizerInput,
        ctx: BlockContext | None = None,
    ) -> ContentSummarizerOutput:
        return await ContentSummarizerBlock().execute(input, ctx)
