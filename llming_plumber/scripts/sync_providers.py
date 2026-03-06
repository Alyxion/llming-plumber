#!/usr/bin/env python3
"""Sync LLM provider files from llming-lodge into llming-plumber.

Copies only the provider layer (clients, models, base classes) and rewrites
imports from ``llming_lodge`` to ``llming_plumber.llm``.

Usage:
    python scripts/sync_providers.py
"""

from __future__ import annotations

import re
import shutil
from pathlib import Path

LODGE_ROOT = Path.home() / "projects/SalesBot/dependencies/llming-lodge/llming_lodge"
DEST_ROOT = Path(__file__).resolve().parent.parent / "llming_plumber" / "llm"

# Files to sync, relative to the llming_lodge package root.
FILES: list[str] = [
    # Base / shared
    "llm_base_client.py",
    "llm_base_models.py",
    "messages.py",
    # Tools (minimal surface needed by clients)
    "tools/tool_call.py",
    "tools/llm_tool.py",
    "tools/llm_toolbox.py",
    # Provider base + models
    "providers/llm_provider_base.py",
    "providers/llm_provider_models.py",
    # OpenAI-compat client (used by Mistral etc.)
    "providers/openai_compat_client.py",
    # OpenAI
    "providers/openai/__init__.py",
    "providers/openai/openai_provider.py",
    "providers/openai/openai_client.py",
    "providers/openai/openai_models.py",
    # Azure OpenAI
    "providers/azure_openai/__init__.py",
    "providers/azure_openai/azure_openai_provider.py",
    "providers/azure_openai/azure_openai_models.py",
    # Anthropic
    "providers/anthropic/__init__.py",
    "providers/anthropic/anthropic_provider.py",
    "providers/anthropic/anthropic_client.py",
    "providers/anthropic/anthropic_models.py",
    # Google
    "providers/google/__init__.py",
    "providers/google/google_provider.py",
    "providers/google/google_client.py",
    "providers/google/google_models.py",
    # Mistral
    "providers/mistral/__init__.py",
    "providers/mistral/mistral_provider.py",
    "providers/mistral/mistral_models.py",
    # Budget / cost tracking
    "budget/__init__.py",
    "budget/budget_types.py",
    "budget/budget_limit.py",
    "budget/budget_manager.py",
    "budget/memory_budget_limit.py",
    "budget/mongodb_budget_limit.py",
    "budget/time_intervals.py",
]

# Hand-written files that replace synced originals.
# These are written AFTER the sync to override lodge-specific code.
OVERRIDES: dict[str, str] = {
    "__init__.py": '"""LLM provider layer — synced from llming-lodge."""\n',
    "tools/__init__.py": (
        '"""LLM Tools — minimal surface for provider clients."""\n\n'
        "from .tool_call import ToolCallInfo, ToolCallStatus\n"
        "from .llm_tool import LlmTool\n"
        "from .llm_toolbox import LlmToolbox\n\n"
        "__all__ = [\n"
        "    'ToolCallInfo',\n"
        "    'ToolCallStatus',\n"
        "    'LlmTool',\n"
        "    'LlmToolbox',\n"
        "]\n"
    ),
    "budget/__init__.py": (
        '"""LLM Budget management — cost tracking and limits."""\n\n'
        "from .budget_types import LimitPeriod, InsufficientBudgetError, TokenUsage, BudgetInfo, BudgetHandler\n"
        "from .budget_limit import BudgetLimit\n"
        "from .memory_budget_limit import MemoryBudgetLimit\n"
        "from .budget_manager import LLMBudgetManager\n\n\n"
        "def __getattr__(name):\n"
        '    """Lazy imports for optional dependencies."""\n'
        '    if name == "MongoDBBudgetLimit":\n'
        "        from .mongodb_budget_limit import MongoDBBudgetLimit\n"
        "        return MongoDBBudgetLimit\n"
        '    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")\n\n\n'
        "__all__ = [\n"
        "    'LimitPeriod',\n"
        "    'InsufficientBudgetError',\n"
        "    'TokenUsage',\n"
        "    'BudgetLimit',\n"
        "    'MemoryBudgetLimit',\n"
        "    'MongoDBBudgetLimit',\n"
        "    'LLMBudgetManager',\n"
        "    'BudgetInfo',\n"
        "    'BudgetHandler',\n"
        "]\n"
    ),
    "providers/__init__.py": (
        '"""Provider management for LLM integrations."""\n'
        "from typing import Dict, Type\n\n"
        "from .llm_provider_base import BaseProvider\n"
        "from .llm_provider_models import LLMInfo\n\n"
        "PROVIDERS: Dict[str, Type[BaseProvider]] = {}\n\n\n"
        "def register_provider(provider_name: str):\n"
        '    """Decorator to register provider implementations."""\n'
        "    def decorator(provider_class: Type[BaseProvider]):\n"
        "        PROVIDERS[provider_name] = provider_class\n"
        "        return provider_class\n"
        "    return decorator\n\n\n"
        "def get_provider(provider: str) -> Type[BaseProvider]:\n"
        '    """Get provider implementation class."""\n'
        "    if provider not in PROVIDERS:\n"
        '        raise ValueError(f"Provider {provider} not registered")\n'
        "    return PROVIDERS[provider]\n\n\n"
        "# Import provider implementations to register them\n"
        "from .openai.openai_provider import OpenAIProvider  # noqa: E402\n"
        "from .anthropic.anthropic_provider import AnthropicProvider  # noqa: E402\n"
        "from .mistral.mistral_provider import MistralProvider  # noqa: E402\n"
        "from .google.google_provider import GoogleProvider  # noqa: E402\n"
        "from .azure_openai.azure_openai_provider import AzureOpenAIProvider  # noqa: E402\n\n\n"
        "__all__ = [\n"
        "    'BaseProvider',\n"
        "    'LLMInfo',\n"
        "    'register_provider',\n"
        "    'get_provider',\n"
        "    'OpenAIProvider',\n"
        "    'AnthropicProvider',\n"
        "    'MistralProvider',\n"
        "    'GoogleProvider',\n"
        "    'AzureOpenAIProvider',\n"
        "]\n"
    ),
}


def rewrite_imports(text: str) -> str:
    """Replace llming_lodge references with llming_plumber.llm."""
    return re.sub(
        r"(from|import)\s+llming_lodge\b",
        r"\1 llming_plumber.llm",
        text,
    )


def sync() -> None:
    # Clean destination
    if DEST_ROOT.exists():
        shutil.rmtree(DEST_ROOT)

    copied = 0
    for rel in FILES:
        src = LODGE_ROOT / rel
        dst = DEST_ROOT / rel
        if not src.exists():
            print(f"  SKIP (missing): {rel}")
            continue

        dst.parent.mkdir(parents=True, exist_ok=True)
        content = src.read_text()
        content = rewrite_imports(content)
        dst.write_text(content)
        copied += 1

    # Write override files (custom __init__.py files)
    for rel, content in OVERRIDES.items():
        dst = DEST_ROOT / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_text(content)

    print(f"Synced {copied} files to {DEST_ROOT}")


if __name__ == "__main__":
    sync()
