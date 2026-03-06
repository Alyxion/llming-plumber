from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends
from motor.motor_asyncio import AsyncIOMotorDatabase

from llming_plumber.api.deps import get_db

router = APIRouter()


@router.get("")
async def list_lemmings(
    db: AsyncIOMotorDatabase = Depends(get_db),  # type: ignore[type-arg]
) -> list[dict[str, Any]]:
    """List all registered lemmings from the lemmings collection."""
    cursor = db["lemmings"].find()
    results: list[dict[str, Any]] = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        results.append(doc)
    return results
