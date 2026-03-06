"""LLM Question Answerer block (RAG-style)."""

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


class QuestionAnswererInput(BlockInput):
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
    context: str = Field(
        title="Context",
        description="Reference text or documents",
        json_schema_extra={"widget": "textarea"},
    )
    question: str = Field(
        title="Question",
        description="Question to answer",
    )
    answer_style: str = Field(
        default="concise",
        title="Answer Style",
        description="Style of the answer",
        json_schema_extra={
            "widget": "select",
            "options": [
                "concise",
                "detailed",
                "step_by_step",
            ],
        },
    )


class QuestionAnswererOutput(BlockOutput):
    answer: str
    confidence: str


class QuestionAnswererBlock(
    BaseBlock[
        QuestionAnswererInput,
        QuestionAnswererOutput,
    ],
):
    block_type: ClassVar[str] = "llm_question_answerer"
    icon: ClassVar[str] = "tabler/help"
    categories: ClassVar[list[str]] = ["llm/chat"]
    description: ClassVar[str] = (
        "Answer questions based on provided context"
    )

    async def execute(
        self,
        input: QuestionAnswererInput,
        ctx: BlockContext | None = None,
    ) -> QuestionAnswererOutput:
        user_msg = (
            f"Answer style: {input.answer_style}\n\n"
            f"Context:\n{input.context}\n\n"
            f"Question: {input.question}"
        )
        response = await _client.prompt(
            provider=input.provider,
            model=input.model,
            system=_client.load_prompt("question_answerer"),
            user=user_msg,
        )
        data: dict[str, Any] = json.loads(response)
        return QuestionAnswererOutput(
            answer=data["answer"],
            confidence=data["confidence"],
        )
