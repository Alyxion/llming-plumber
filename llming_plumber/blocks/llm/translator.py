"""Text Translator block — translate text using an LLM."""

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


class TranslatorInput(BlockInput):
    provider: str = Field(
        default_factory=_defaults.provider_factory("medium"),
        title="Provider",
        description="LLM provider to use",
        json_schema_extra={
            "widget": "combobox",
            "options": _defaults.PROVIDERS,
        },
    )
    model: str = Field(
        default_factory=_defaults.model_factory("medium"),
        title="Model",
        description="Model identifier",
        json_schema_extra={
            "widget": "combobox",
            "options_ref": "llm_models",
        },
    )
    text: str = Field(
        title="Text",
        description="Text to translate",
        json_schema_extra={"widget": "textarea"},
    )
    source_language: str = Field(
        default="",
        title="Source Language",
        description="Source language (leave empty to auto-detect)",
        json_schema_extra={
            "placeholder": "auto-detect",
        },
    )
    target_language: str = Field(
        default="English",
        title="Target Language",
        description="Target language for translation",
        json_schema_extra={"placeholder": "English"},
    )


class TranslatorOutput(BlockOutput):
    translated_text: str
    detected_language: str


class TranslatorBlock(
    BaseBlock[TranslatorInput, TranslatorOutput],
):
    llm_tier: ClassVar[str] = "medium"
    block_type: ClassVar[str] = "llm_translator"
    icon: ClassVar[str] = "tabler/language"
    categories: ClassVar[list[str]] = ["llm/text"]
    description: ClassVar[str] = (
        "Translate text between languages using an LLM"
    )
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self,
        input: TranslatorInput,
        ctx: BlockContext | None = None,
    ) -> TranslatorOutput:
        source = input.source_language or "auto-detect"
        user_msg = (
            f"Source language: {source}\n"
            f"Target language: {input.target_language}\n\n"
            f"{input.text}"
        )
        response = await _client.prompt(
            provider=input.provider,
            model=input.model,
            system=_client.load_prompt("translator"),
            user=user_msg,
        )
        data = json.loads(response)
        return TranslatorOutput(
            translated_text=data["translated_text"],
            detected_language=data["detected_language"],
        )
