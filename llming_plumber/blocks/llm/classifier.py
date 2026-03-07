"""LLM Text Classifier block."""

from __future__ import annotations

import json
from typing import Any, ClassVar

from pydantic import Field

from llming_plumber.blocks.base import (
    BaseBlock,
    BlockContext,
    BlockInput,
    BlockOutput,
)
from llming_plumber.blocks.llm import _client, _defaults


class ClassifierInput(BlockInput):
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
        description="Text to classify",
        json_schema_extra={"widget": "textarea"},
    )
    categories: list[str] = Field(
        title="Categories",
        description="Categories to classify into",
    )
    multi_label: bool = Field(
        default=False,
        title="Multi-Label",
        description="Allow multiple labels",
    )


class ClassifierOutput(BlockOutput):
    labels: list[str]
    confidence_scores: dict[str, float]


class ClassifierBlock(
    BaseBlock[ClassifierInput, ClassifierOutput],
):
    llm_tier: ClassVar[str] = "medium"
    block_type: ClassVar[str] = "llm_classifier"
    icon: ClassVar[str] = "tabler/tags"
    categories: ClassVar[list[str]] = ["llm/analysis"]
    description: ClassVar[str] = (
        "Classify text into predefined categories"
    )

    async def execute(
        self,
        input: ClassifierInput,
        ctx: BlockContext | None = None,
    ) -> ClassifierOutput:
        user_msg = (
            f"Categories: {input.categories}\n"
            f"Multi-label: {input.multi_label}\n\n"
            f"Text:\n{input.text}"
        )
        response = await _client.prompt(
            provider=input.provider,
            model=input.model,
            system=_client.load_prompt("classifier"),
            user=user_msg,
        )
        data: dict[str, Any] = json.loads(response)
        return ClassifierOutput(
            labels=data["labels"],
            confidence_scores=data["confidence_scores"],
        )
