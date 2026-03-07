"""Global LLM tier defaults for provider and model fields.

Blocks declare an ``llm_tier`` ClassVar (``"complex"``, ``"medium"``, or
``"fast"``) and use the factories here as ``default_factory`` for their
``provider`` and ``model`` fields.  This way the *same* global config
(``settings.llm``) drives all blocks, and users can override via env vars
(``PLUMBER_LLM_COMPLEX_PROVIDER``, etc.) without touching block code.
"""

from __future__ import annotations

from typing import Literal

from llming_plumber.config import settings

LlmTier = Literal["complex", "medium", "fast"]

# Providers available for selection in the UI
PROVIDERS = ["anthropic", "openai", "azure_openai", "google", "mistral"]

# Well-known models per provider for UI suggestions
MODEL_OPTIONS: dict[str, list[str]] = {
    "anthropic": ["claude-opus-4-6", "claude-sonnet-4-6", "claude-haiku-4-5-20251001"],
    "openai": ["gpt-5.2", "gpt-5-mini", "gpt-5-nano"],
    "azure_openai": ["gpt-5.2", "gpt-5-mini", "gpt-5-nano"],
    "google": ["gemini-3-pro-preview", "gemini-3-flash-preview"],
    "mistral": ["mistral-large-latest", "mistral-medium-latest", "mistral-small-latest"],
}


def _tier_config(tier: LlmTier):  # noqa: ANN202
    return getattr(settings.llm, tier)


def provider_factory(tier: LlmTier):  # noqa: ANN202
    """Return a ``default_factory`` callable for the provider field."""
    def _factory() -> str:
        return _tier_config(tier).provider
    return _factory


def model_factory(tier: LlmTier):  # noqa: ANN202
    """Return a ``default_factory`` callable for the model field."""
    def _factory() -> str:
        return _tier_config(tier).model
    return _factory
