"""Integration tests for LLM providers — calls real APIs.

Run with:
    pytest tests/llm/test_providers_integration.py -m integration -v

Requires a .env file with valid API keys (see .env for required vars).
These tests are NOT run in CI — they are for local verification only.
"""

from __future__ import annotations

import os

import pytest

from llming_plumber.llm.llm_base_client import LlmClient
from llming_plumber.llm.messages import (
    LlmAIMessage,
    LlmHumanMessage,
    LlmMessageChunk,
    LlmSystemMessage,
)

SIMPLE_PROMPT = "Reply with exactly one word: hello"
SYSTEM_PROMPT = "You are a helpful assistant. Be concise."


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _create_client(provider_name: str) -> LlmClient:
    """Create a client for the cheapest/fastest model of a provider."""
    from llming_plumber.llm.providers import get_provider

    cls = get_provider(provider_name)
    provider = cls()
    if not provider.is_available:
        pytest.skip(f"{provider_name} not available (missing API key)")

    # Pick the smallest model
    models = provider.get_models()
    from llming_plumber.llm.providers.llm_provider_models import ModelSize
    small = [m for m in models if m.size == ModelSize.SMALL]
    model = small[0] if small else models[-1]

    # Reasoning models need enforced_temperature and enough output tokens
    temp = model.enforced_temperature if model.enforced_temperature is not None else 0.0

    return provider.create_client(
        model=model.model,
        temperature=temp,
        max_tokens=256,
    )


def _messages() -> list[LlmSystemMessage | LlmHumanMessage]:
    return [
        LlmSystemMessage(content=SYSTEM_PROMPT),
        LlmHumanMessage(content=SIMPLE_PROMPT),
    ]


# ---------------------------------------------------------------------------
# OpenAI
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestOpenAIIntegration:
    def test_invoke(self) -> None:
        client = _create_client("openai")
        result = client.invoke(_messages())
        assert isinstance(result, LlmAIMessage)
        assert len(result.content) > 0

    @pytest.mark.asyncio
    async def test_ainvoke(self) -> None:
        client = _create_client("openai")
        result = await client.ainvoke(_messages())
        assert isinstance(result, LlmAIMessage)
        assert len(result.content) > 0

    @pytest.mark.asyncio
    async def test_astream(self) -> None:
        client = _create_client("openai")
        chunks: list[LlmMessageChunk] = []
        async for chunk in client.astream(_messages()):
            chunks.append(chunk)
        assert len(chunks) > 0
        assert chunks[-1].is_final

    def test_stream(self) -> None:
        client = _create_client("openai")
        chunks = list(client.stream(_messages()))
        assert len(chunks) > 0
        assert chunks[-1].is_final


# ---------------------------------------------------------------------------
# Azure OpenAI
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestAzureOpenAIIntegration:
    def test_invoke(self) -> None:
        client = _create_client("azure_openai")
        result = client.invoke(_messages())
        assert isinstance(result, LlmAIMessage)
        assert len(result.content) > 0

    @pytest.mark.asyncio
    async def test_ainvoke(self) -> None:
        client = _create_client("azure_openai")
        result = await client.ainvoke(_messages())
        assert isinstance(result, LlmAIMessage)
        assert len(result.content) > 0

    @pytest.mark.asyncio
    async def test_astream(self) -> None:
        client = _create_client("azure_openai")
        chunks: list[LlmMessageChunk] = []
        async for chunk in client.astream(_messages()):
            chunks.append(chunk)
        assert len(chunks) > 0
        assert chunks[-1].is_final

    def test_stream(self) -> None:
        client = _create_client("azure_openai")
        chunks = list(client.stream(_messages()))
        assert len(chunks) > 0
        assert chunks[-1].is_final


# ---------------------------------------------------------------------------
# Anthropic
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestAnthropicIntegration:
    def test_invoke(self) -> None:
        client = _create_client("anthropic")
        result = client.invoke(_messages())
        assert isinstance(result, LlmAIMessage)
        assert len(result.content) > 0

    @pytest.mark.asyncio
    async def test_ainvoke(self) -> None:
        client = _create_client("anthropic")
        result = await client.ainvoke(_messages())
        assert isinstance(result, LlmAIMessage)
        assert len(result.content) > 0

    @pytest.mark.asyncio
    async def test_astream(self) -> None:
        client = _create_client("anthropic")
        chunks: list[LlmMessageChunk] = []
        async for chunk in client.astream(_messages()):
            chunks.append(chunk)
        assert len(chunks) > 0
        assert chunks[-1].is_final

    def test_stream(self) -> None:
        client = _create_client("anthropic")
        chunks = list(client.stream(_messages()))
        assert len(chunks) > 0
        assert chunks[-1].is_final


# ---------------------------------------------------------------------------
# Google Gemini
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestGoogleIntegration:
    def test_invoke(self) -> None:
        client = _create_client("google")
        result = client.invoke(_messages())
        assert isinstance(result, LlmAIMessage)
        assert len(result.content) > 0

    @pytest.mark.asyncio
    async def test_ainvoke(self) -> None:
        client = _create_client("google")
        result = await client.ainvoke(_messages())
        assert isinstance(result, LlmAIMessage)
        assert len(result.content) > 0

    @pytest.mark.asyncio
    async def test_astream(self) -> None:
        client = _create_client("google")
        chunks: list[LlmMessageChunk] = []
        async for chunk in client.astream(_messages()):
            chunks.append(chunk)
        assert len(chunks) > 0
        assert chunks[-1].is_final

    def test_stream(self) -> None:
        client = _create_client("google")
        chunks = list(client.stream(_messages()))
        assert len(chunks) > 0
        assert chunks[-1].is_final


# ---------------------------------------------------------------------------
# Mistral
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestMistralIntegration:
    def test_invoke(self) -> None:
        client = _create_client("mistral")
        result = client.invoke(_messages())
        assert isinstance(result, LlmAIMessage)
        assert len(result.content) > 0

    @pytest.mark.asyncio
    async def test_ainvoke(self) -> None:
        client = _create_client("mistral")
        result = await client.ainvoke(_messages())
        assert isinstance(result, LlmAIMessage)
        assert len(result.content) > 0

    @pytest.mark.asyncio
    async def test_astream(self) -> None:
        client = _create_client("mistral")
        chunks: list[LlmMessageChunk] = []
        async for chunk in client.astream(_messages()):
            chunks.append(chunk)
        assert len(chunks) > 0
        assert chunks[-1].is_final

    def test_stream(self) -> None:
        client = _create_client("mistral")
        chunks = list(client.stream(_messages()))
        assert len(chunks) > 0
        assert chunks[-1].is_final
