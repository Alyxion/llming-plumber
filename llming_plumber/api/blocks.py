from __future__ import annotations

from typing import Any

from fastapi import APIRouter

from llming_plumber.blocks.registry import BlockRegistry
from llming_plumber.worker.executor import GLOBAL_VARIABLES

router = APIRouter()


@router.get("")
async def list_blocks() -> list[dict[str, Any]]:
    """Return the catalog of all registered block types."""
    catalog = BlockRegistry.catalog()
    return [item.model_dump() for item in catalog]


@router.get("/variables")
async def list_global_variables() -> list[dict[str, str]]:
    """Return global template variables available in every block config."""
    return GLOBAL_VARIABLES
