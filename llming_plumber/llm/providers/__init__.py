"""Provider management for LLM integrations."""
from typing import Dict, Type

from .llm_provider_base import BaseProvider
from .llm_provider_models import LLMInfo

PROVIDERS: Dict[str, Type[BaseProvider]] = {}


def register_provider(provider_name: str):
    """Decorator to register provider implementations."""
    def decorator(provider_class: Type[BaseProvider]):
        PROVIDERS[provider_name] = provider_class
        return provider_class
    return decorator


def get_provider(provider: str) -> Type[BaseProvider]:
    """Get provider implementation class."""
    if provider not in PROVIDERS:
        raise ValueError(f"Provider {provider} not registered")
    return PROVIDERS[provider]


# Import provider implementations to register them
from .openai.openai_provider import OpenAIProvider  # noqa: E402
from .anthropic.anthropic_provider import AnthropicProvider  # noqa: E402
from .mistral.mistral_provider import MistralProvider  # noqa: E402
from .google.google_provider import GoogleProvider  # noqa: E402
from .azure_openai.azure_openai_provider import AzureOpenAIProvider  # noqa: E402


__all__ = [
    'BaseProvider',
    'LLMInfo',
    'register_provider',
    'get_provider',
    'OpenAIProvider',
    'AnthropicProvider',
    'MistralProvider',
    'GoogleProvider',
    'AzureOpenAIProvider',
]
