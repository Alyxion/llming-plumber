"""LLM Chat block — send a message and get a response."""

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


class ChatInput(BlockInput):
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
    system_prompt: str = Field(
        default="",
        title="System Prompt",
        description="System prompt for the conversation",
        json_schema_extra={"widget": "textarea"},
    )
    user_message: str = Field(
        title="User Message",
        description="Message to send to the model",
        json_schema_extra={"widget": "textarea"},
    )
    temperature: float = Field(
        default=0.3,
        title="Temperature",
        description="Sampling temperature",
        json_schema_extra={"min": 0.0, "max": 2.0},
    )
    max_tokens: int = Field(
        default=4096,
        title="Max Tokens",
        description="Maximum number of tokens to generate",
    )


class ChatOutput(BlockOutput):
    response: str
    model: str
    provider: str


class ChatBlock(BaseBlock[ChatInput, ChatOutput]):
    block_type: ClassVar[str] = "llm_chat"
    icon: ClassVar[str] = "tabler/message-chatbot"
    categories: ClassVar[list[str]] = ["llm/chat"]
    description: ClassVar[str] = (
        "Send a message to an LLM and get a response"
    )
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self,
        input: ChatInput,
        ctx: BlockContext | None = None,
    ) -> ChatOutput:
        system = input.system_prompt or _client.load_prompt("chat")
        response = await _client.prompt(
            provider=input.provider,
            model=input.model,
            system=system,
            user=input.user_message,
            temperature=input.temperature,
            max_tokens=input.max_tokens,
        )
        return ChatOutput(
            response=response,
            model=input.model,
            provider=input.provider,
        )
