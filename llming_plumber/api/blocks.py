from __future__ import annotations

from typing import Any

from fastapi import APIRouter

from llming_plumber.blocks.registry import BlockRegistry

router = APIRouter()


@router.get("")
async def list_blocks() -> list[dict[str, Any]]:
    """Return the catalog of all registered block types."""
    catalog = BlockRegistry.catalog()
    return [item.model_dump() for item in catalog]
