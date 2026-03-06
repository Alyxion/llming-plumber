"""Tests for LLM provider layer (synced from llming-lodge).

These are offline unit tests — they do NOT call real APIs.
They verify provider registration, model catalogues, and client creation.
"""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from llming_plumber.llm.providers import (
    PROVIDERS,
    BaseProvider,
    LLMInfo,
    get_provider,
    register_provider,
)
from llming_plumber.llm.providers.llm_provider_models import (
    ModelSize,
    ReasoningEffort,
)
from llming_plumber.llm.llm_base_client import LlmClient
from llming_plumber.llm.llm_base_models import ChatMessage, Role
from llming_plumber.llm.messages import (
    LlmAIMessage,
    LlmHumanMessage,
    LlmSystemMessage,
    LlmMessageChunk,
)
from llming_plumber.llm.tools.tool_call import ToolCallInfo, ToolCallStatus
from llming_plumber.llm.tools.llm_tool import LlmTool
from llming_plumber.llm.tools.llm_toolbox import LlmToolbox


# ---------------------------------------------------------------------------
# Provider registry
# ---------------------------------------------------------------------------

EXPECTED_PROVIDERS = ["openai", "anthropic", "mistral", "google", "azure_openai"]


def test_all_providers_registered() -> None:
    for name in EXPECTED_PROVIDERS:
        assert name in PROVIDERS, f"Provider {name} not registered"


def test_get_provider_returns_class() -> None:
    for name in EXPECTED_PROVIDERS:
        cls = get_provider(name)
        assert issubclass(cls, BaseProvider)


def test_get_provider_unknown_raises() -> None:
    with pytest.raises(ValueError, match="not registered"):
        get_provider("nonexistent_provider")


# ---------------------------------------------------------------------------
# Provider instantiation + model catalogues
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("provider_name", EXPECTED_PROVIDERS)
def test_provider_has_models(provider_name: str) -> None:
    cls = get_provider(provider_name)
    provider = cls()
    models = provider.get_models()
    assert len(models) > 0
    for m in models:
        assert isinstance(m, LLMInfo)
        assert m.provider != ""
        assert m.model != ""
        assert m.label != ""


@pytest.mark.parametrize("provider_name", EXPECTED_PROVIDERS)
def test_provider_label_and_name(provider_name: str) -> None:
    cls = get_provider(provider_name)
    provider = cls()
    assert provider.name == provider_name
    assert provider.label != ""


# ---------------------------------------------------------------------------
# Provider availability (env-based)
# ---------------------------------------------------------------------------

def test_openai_not_available_without_key() -> None:
    with patch.dict(os.environ, {}, clear=True):
        from llming_plumber.llm.providers.openai.openai_provider import (
            OpenAIProvider,
        )
        p = OpenAIProvider()
        assert not p.is_available


def test_anthropic_not_available_without_key() -> None:
    with patch.dict(os.environ, {}, clear=True):
        from llming_plumber.llm.providers.anthropic.anthropic_provider import (
            AnthropicProvider,
        )
        p = AnthropicProvider()
        assert not p.is_available


def test_google_not_available_without_key() -> None:
    with patch.dict(os.environ, {}, clear=True):
        from llming_plumber.llm.providers.google.google_provider import (
            GoogleProvider,
        )
        p = GoogleProvider()
        assert not p.is_available


def test_mistral_not_available_without_key() -> None:
    with patch.dict(os.environ, {}, clear=True):
        from llming_plumber.llm.providers.mistral.mistral_provider import (
            MistralProvider,
        )
        p = MistralProvider()
        assert not p.is_available


def test_azure_not_available_without_key() -> None:
    with patch.dict(os.environ, {}, clear=True):
        from llming_plumber.llm.providers.azure_openai.azure_openai_provider import (
            AzureOpenAIProvider,
        )
        p = AzureOpenAIProvider()
        assert not p.is_available


# ---------------------------------------------------------------------------
# Client creation raises when key is missing
# ---------------------------------------------------------------------------

def test_openai_create_client_raises_without_key() -> None:
    with patch.dict(os.environ, {}, clear=True):
        from llming_plumber.llm.providers.openai.openai_provider import (
            OpenAIProvider,
        )
        p = OpenAIProvider()
        with pytest.raises(ValueError, match="OPENAI_API_KEY"):
            p.create_client(model="gpt-5-nano")


def test_anthropic_create_client_raises_without_key() -> None:
    with patch.dict(os.environ, {}, clear=True):
        from llming_plumber.llm.providers.anthropic.anthropic_provider import (
            AnthropicProvider,
        )
        p = AnthropicProvider()
        with pytest.raises(ValueError, match="ANTHROPIC_API_KEY"):
            p.create_client(model="claude-haiku-4-5-20251001")


def test_google_create_client_raises_without_key() -> None:
    with patch.dict(os.environ, {}, clear=True):
        from llming_plumber.llm.providers.google.google_provider import (
            GoogleProvider,
        )
        p = GoogleProvider()
        with pytest.raises(ValueError, match="GEMINI_KEY"):
            p.create_client(model="gemini-3-flash-preview")


def test_mistral_create_client_raises_without_key() -> None:
    with patch.dict(os.environ, {}, clear=True):
        from llming_plumber.llm.providers.mistral.mistral_provider import (
            MistralProvider,
        )
        p = MistralProvider()
        with pytest.raises(ValueError, match="MISTRAL_API_KEY"):
            p.create_client(model="mistral-small-latest")


def test_azure_create_client_raises_without_key() -> None:
    with patch.dict(os.environ, {}, clear=True):
        from llming_plumber.llm.providers.azure_openai.azure_openai_provider import (
            AzureOpenAIProvider,
        )
        p = AzureOpenAIProvider()
        with pytest.raises(ValueError):
            p.create_client(model="gpt-5-nano")


# ---------------------------------------------------------------------------
# LLMInfo model
# ---------------------------------------------------------------------------

def test_llm_info_fields() -> None:
    info = LLMInfo(
        provider="test",
        name="test_model",
        label="Test Model",
        model="test-model-v1",
        description="A test model",
        input_token_price=1.0,
    )
    assert info.provider == "test"
    assert info.size == ModelSize.MEDIUM  # default
    assert info.max_input_tokens == 64000
    assert info.supports_image_input is False


def test_llm_info_reasoning_effort() -> None:
    info = LLMInfo(
        provider="test",
        name="r",
        label="R",
        model="r",
        description="r",
        input_token_price=0,
        reasoning=True,
        default_reasoning_effort=ReasoningEffort.HIGH,
    )
    assert info.reasoning is True
    assert info.default_reasoning_effort == ReasoningEffort.HIGH


# ---------------------------------------------------------------------------
# Message types
# ---------------------------------------------------------------------------

def test_message_types() -> None:
    sys_msg = LlmSystemMessage(content="You are helpful.")
    assert sys_msg.role == Role.SYSTEM

    user_msg = LlmHumanMessage(content="Hello")
    assert user_msg.role == Role.USER

    ai_msg = LlmAIMessage(content="Hi there!")
    assert ai_msg.role == Role.ASSISTANT


def test_message_chunk() -> None:
    chunk = LlmMessageChunk(
        content="partial",
        role=Role.ASSISTANT,
        index=0,
        is_final=False,
    )
    assert chunk.content == "partial"
    assert chunk.is_final is False
    assert chunk.tool_call is None


# ---------------------------------------------------------------------------
# Tool types
# ---------------------------------------------------------------------------

def test_tool_call_info() -> None:
    info = ToolCallInfo(
        name="search",
        call_id="abc123",
        status=ToolCallStatus.PENDING,
    )
    assert info.display_name == "Search"
    assert not info.is_image_generation


def test_llm_tool() -> None:
    tool = LlmTool(
        name="my_tool",
        description="Does things",
        func=lambda: None,
        parameters={"type": "object", "properties": {}},
    )
    assert tool.name == "my_tool"
    assert callable(tool.func)


def test_llm_toolbox() -> None:
    tool = LlmTool(name="t", description="d", func=lambda: None)
    box = LlmToolbox(name="box", description="tools", tools=[tool])
    assert len(box.tools) == 1


# ---------------------------------------------------------------------------
# Client creation with key (mocked env)
# ---------------------------------------------------------------------------

def test_openai_create_client_with_key() -> None:
    with patch.dict(os.environ, {"OPENAI_API_KEY": "sk-test"}):
        from llming_plumber.llm.providers.openai.openai_provider import (
            OpenAIProvider,
        )
        p = OpenAIProvider()
        client = p.create_client(model="gpt-5-nano")
        assert isinstance(client, LlmClient)
        assert client.model == "gpt-5-nano"


def test_mistral_create_client_with_key() -> None:
    with patch.dict(os.environ, {"MISTRAL_API_KEY": "test-key"}):
        from llming_plumber.llm.providers.mistral.mistral_provider import (
            MistralProvider,
        )
        p = MistralProvider()
        client = p.create_client(model="mistral-small-latest")
        assert isinstance(client, LlmClient)
        assert client.model == "mistral-small-latest"


def test_google_create_client_with_key() -> None:
    with patch.dict(os.environ, {"GEMINI_KEY": "test-key"}):
        from llming_plumber.llm.providers.google.google_provider import (
            GoogleProvider,
        )
        p = GoogleProvider()
        client = p.create_client(model="gemini-3-flash-preview")
        assert isinstance(client, LlmClient)


def test_anthropic_create_client_with_key() -> None:
    with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "sk-ant-test"}):
        from llming_plumber.llm.providers.anthropic.anthropic_provider import (
            AnthropicProvider,
        )
        p = AnthropicProvider()
        client = p.create_client(model="claude-haiku-4-5-20251001")
        assert isinstance(client, LlmClient)


def test_azure_create_client_with_key() -> None:
    env = {
        "AZURE_OPENAI_API_KEY": "test-key",
        "AZURE_OPENAI_ENDPOINT": "https://test.openai.azure.com",
    }
    with patch.dict(os.environ, env):
        from llming_plumber.llm.providers.azure_openai.azure_openai_provider import (
            AzureOpenAIProvider,
        )
        p = AzureOpenAIProvider()
        client = p.create_client(model="gpt-5-nano")
        assert isinstance(client, LlmClient)
