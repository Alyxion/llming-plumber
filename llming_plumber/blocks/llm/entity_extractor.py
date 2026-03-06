"""LLM Named Entity Extractor block."""

from __future__ import annotations

import json
from typing import Any, ClassVar

from pydantic import BaseModel, Field

from llming_plumber.blocks.base import (
    BaseBlock,
    BlockContext,
    BlockInput,
    BlockOutput,
)
from llming_plumber.blocks.llm import _client


class Entity(BaseModel):
    text: str
    type: str


class EntityExtractorInput(BlockInput):
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
        description="Text to extract entities from",
        json_schema_extra={"widget": "textarea"},
    )
    entity_types: list[str] = Field(
        default=[
            "person",
            "organization",
            "location",
            "date",
        ],
        title="Entity Types",
        description="Types of entities to extract",
    )


class EntityExtractorOutput(BlockOutput):
    entities: list[Entity]


class EntityExtractorBlock(
    BaseBlock[EntityExtractorInput, EntityExtractorOutput],
):
    block_type: ClassVar[str] = "llm_entity_extractor"
    icon: ClassVar[str] = "tabler/scan"
    categories: ClassVar[list[str]] = ["llm/analysis"]
    description: ClassVar[str] = (
        "Extract named entities from text"
    )

    async def execute(
        self,
        input: EntityExtractorInput,
        ctx: BlockContext | None = None,
    ) -> EntityExtractorOutput:
        user_msg = (
            f"Entity types: {input.entity_types}\n\n"
            f"Text:\n{input.text}"
        )
        response = await _client.prompt(
            provider=input.provider,
            model=input.model,
            system=_client.load_prompt("entity_extractor"),
            user=user_msg,
        )
        data: dict[str, Any] = json.loads(response)
        entities = [
            Entity(**e) for e in data["entities"]
        ]
        return EntityExtractorOutput(entities=entities)
