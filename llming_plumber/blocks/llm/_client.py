"""Shared helper for LLM blocks — creates a client from provider+model."""

from __future__ import annotations

from pathlib import Path

from llming_plumber.llm.llm_base_client import LlmClient
from llming_plumber.llm.messages import (
    LlmHumanMessage,
    LlmSystemMessage,
)
from llming_plumber.llm.providers import get_provider

_PROMPTS_DIR = Path(__file__).parent / "prompts"


def load_prompt(name: str) -> str:
    """Load a prompt template from the prompts/ directory."""
    return (_PROMPTS_DIR / f"{name}.txt").read_text().strip()


def create_client(
    provider: str,
    model: str,
    temperature: float = 0.3,
    max_tokens: int = 4096,
) -> LlmClient:
    """Create an LLM client from provider name and model."""
    cls = get_provider(provider)
    p = cls()
    if not p.is_available:
        msg = f"Provider {provider} is not available (missing API key)"
        raise ValueError(msg)
    info = next(
        (m for m in p.get_models() if m.model == model),
        None,
    )
    temp = temperature
    if info and info.enforced_temperature is not None:
        temp = info.enforced_temperature
    return p.create_client(
        model=model, temperature=temp, max_tokens=max_tokens
    )


async def prompt(
    provider: str,
    model: str,
    system: str,
    user: str,
    temperature: float = 0.3,
    max_tokens: int = 4096,
) -> str:
    """One-shot prompt → response text."""
    client = create_client(provider, model, temperature, max_tokens)
    result = await client.ainvoke([
        LlmSystemMessage(content=system),
        LlmHumanMessage(content=user),
    ])
    return result.content
