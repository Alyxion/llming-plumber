"""API endpoint exposing global LLM tier defaults and model options."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter

from llming_plumber.blocks.llm._defaults import MODEL_OPTIONS, PROVIDERS
from llming_plumber.config import settings

router = APIRouter()


@router.get("/llm-defaults")
async def get_llm_defaults() -> dict[str, Any]:
    """Return global LLM tier config and model options for the UI."""
    return {
        "tiers": {
            "complex": settings.llm.complex.model_dump(),
            "medium": settings.llm.medium.model_dump(),
            "fast": settings.llm.fast.model_dump(),
        },
        "providers": PROVIDERS,
        "models": MODEL_OPTIONS,
    }
