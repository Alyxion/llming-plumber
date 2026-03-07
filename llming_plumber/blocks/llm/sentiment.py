"""Sentiment Analyzer block — analyze text sentiment with an LLM."""

from __future__ import annotations

import json
from typing import ClassVar

from pydantic import Field

from llming_plumber.blocks.base import (
    BaseBlock,
    BlockContext,
    BlockInput,
    BlockOutput,
)
from llming_plumber.blocks.llm import _client, _defaults


class SentimentInput(BlockInput):
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
        description="Text to analyze for sentiment",
        json_schema_extra={"widget": "textarea"},
    )


class SentimentOutput(BlockOutput):
    sentiment: str
    confidence: float
    explanation: str


class SentimentBlock(
    BaseBlock[SentimentInput, SentimentOutput],
):
    llm_tier: ClassVar[str] = "fast"
    block_type: ClassVar[str] = "llm_sentiment"
    icon: ClassVar[str] = "tabler/mood-smile"
    categories: ClassVar[list[str]] = ["llm/analysis"]
    description: ClassVar[str] = (
        "Analyze text sentiment using an LLM"
    )
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self,
        input: SentimentInput,
        ctx: BlockContext | None = None,
    ) -> SentimentOutput:
        response = await _client.prompt(
            provider=input.provider,
            model=input.model,
            system=_client.load_prompt("sentiment"),
            user=input.text,
        )
        data = json.loads(response)
        return SentimentOutput(
            sentiment=data["sentiment"],
            confidence=float(data["confidence"]),
            explanation=data["explanation"],
        )
