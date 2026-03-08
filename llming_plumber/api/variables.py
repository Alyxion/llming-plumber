"""Admin endpoints for global variable access grants."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from llming_plumber.api.deps import get_current_user

router = APIRouter()


class GrantRequest(BaseModel):
    var_name: str
    pipeline_id: str


@router.post("/grants")
async def grant_access(
    body: GrantRequest,
    user: str = Depends(get_current_user),
) -> dict[str, Any]:
    """Grant a pipeline access to a global variable."""
    from llming_plumber.blocks.core.variable_store import VariableStore
    from llming_plumber.db import get_redis

    redis = get_redis()
    await VariableStore.grant_global_access(redis, body.var_name, body.pipeline_id)
    return {"granted": True, "var_name": body.var_name, "pipeline_id": body.pipeline_id}


@router.delete("/grants")
async def revoke_access(
    body: GrantRequest,
    user: str = Depends(get_current_user),
) -> dict[str, Any]:
    """Revoke a pipeline's access to a global variable."""
    from llming_plumber.blocks.core.variable_store import VariableStore
    from llming_plumber.db import get_redis

    redis = get_redis()
    await VariableStore.revoke_global_access(redis, body.var_name, body.pipeline_id)
    return {"revoked": True, "var_name": body.var_name, "pipeline_id": body.pipeline_id}
