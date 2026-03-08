from __future__ import annotations

from typing import Any

from fastapi import HTTPException, Request
from motor.motor_asyncio import AsyncIOMotorDatabase

from llming_plumber.db import get_database


async def get_db() -> AsyncIOMotorDatabase:  # type: ignore[type-arg]
    """FastAPI dependency that returns the Motor database."""
    return get_database()


async def get_arq_pool(request: Request) -> Any:
    """FastAPI dependency that returns the ARQ Redis connection pool."""
    return request.app.state.arq_pool


async def get_current_user(request: Request) -> str:
    """Extract current user handle from signed session cookie.

    Returns the handle string. Raises 401 if no valid session exists.
    """
    from llming_plumber.api.session import get_user_handle

    handle = get_user_handle(request)
    if not handle:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return handle


async def require_pipeline_access(
    pipeline_id: str,
    db: AsyncIOMotorDatabase,  # type: ignore[type-arg]
    user: str,
) -> dict[str, Any]:
    """Load a pipeline and verify the user has access.

    Currently checks owner_id match. Pipelines with no owner
    (empty owner_id) are accessible to everyone (backwards compat).
    """
    from bson import ObjectId

    doc = await db["pipelines"].find_one({"_id": ObjectId(pipeline_id)})
    if doc is None:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    from llming_plumber.config import settings

    owner = doc.get("owner_id", "")
    # In dev mode (no secret_key configured), skip ownership checks
    if not settings.secret_key:
        return doc
    # Allow access when: no owner set, owner matches, or owner is "sample" (demo pipelines)
    if owner and owner != user and owner != "sample":
        raise HTTPException(status_code=403, detail="Not authorized")
    return doc
