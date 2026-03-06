"""LLM Structured Data Extractor block."""

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
from llming_plumber.blocks.llm import _client


class DataExtractorInput(BlockInput):
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
        description="Text to extract data from",
        json_schema_extra={"widget": "textarea"},
    )
    schema_description: str = Field(
        title="Schema Description",
        description=(
            "Describe the data structure to extract, "
            "e.g. 'Extract product name, price, "
            "and currency'"
        ),
        json_schema_extra={"widget": "textarea"},
    )
    output_format: str = Field(
        default="json",
        title="Output Format",
        description="Format for extracted data",
        json_schema_extra={
            "widget": "select",
            "options": ["json", "key_value"],
        },
    )


class DataExtractorOutput(BlockOutput):
    extracted_data: dict[str, Any]


class DataExtractorBlock(
    BaseBlock[DataExtractorInput, DataExtractorOutput],
):
    block_type: ClassVar[str] = "llm_data_extractor"
    icon: ClassVar[str] = "tabler/database-import"
    categories: ClassVar[list[str]] = ["llm/transform"]
    description: ClassVar[str] = (
        "Extract structured data from text"
    )

    async def execute(
        self,
        input: DataExtractorInput,
        ctx: BlockContext | None = None,
    ) -> DataExtractorOutput:
        user_msg = (
            f"Schema: {input.schema_description}\n"
            f"Output format: {input.output_format}\n\n"
            f"Text:\n{input.text}"
        )
        response = await _client.prompt(
            provider=input.provider,
            model=input.model,
            system=_client.load_prompt("data_extractor"),
            user=user_msg,
        )
        data: dict[str, Any] = json.loads(response)
        return DataExtractorOutput(extracted_data=data)
