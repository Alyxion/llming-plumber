"""Cache blocks — store, read, and delete cached data in Redis.

Cache keys are scoped by name and optionally by pipeline:
    - ``pipeline`` scope: key = ``plumber:cache:pl:{pipeline_id}:{name}``
    - ``global`` scope: key = ``plumber:cache:gl:{name}``

The ReadCache block has a special ``cache_hit`` output field. When used
with conditional routing (future), the downstream path depends on
whether data was found in the cache.

Usage pattern:
    ReadCache → (hit) → next step
    ReadCache → (miss) → compute → StoreCache → next step
"""

from __future__ import annotations

import json
import logging
from typing import Any, ClassVar, Literal

from pydantic import Field

from llming_plumber.blocks.base import (
    BaseBlock,
    BlockContext,
    BlockInput,
    BlockOutput,
    FittingDescriptor,
)

logger = logging.getLogger(__name__)

_CACHE_PREFIX_PL = "plumber:cache:pl:"
_CACHE_PREFIX_GL = "plumber:cache:gl:"


def _cache_key(
    name: str,
    scope: str,
    pipeline_id: str = "",
) -> str:
    """Build the Redis key for a cache entry."""
    if scope == "global":
        return f"{_CACHE_PREFIX_GL}{name}"
    return f"{_CACHE_PREFIX_PL}{pipeline_id}:{name}"


def _get_redis() -> Any:
    """Get Redis connection, returns None on failure."""
    try:
        from llming_plumber.db import get_redis
        return get_redis()
    except Exception:
        return None


# ------------------------------------------------------------------
# StoreCache
# ------------------------------------------------------------------


class StoreCacheInput(BlockInput):
    cache_name: str = Field(
        title="Cache Name",
        description="Unique name for this cache entry",
    )
    scope: Literal["pipeline", "global"] = Field(
        default="pipeline",
        title="Scope",
        description="Pipeline-scoped or globally shared",
        json_schema_extra={"widget": "select", "options": ["pipeline", "global"]},
    )
    ttl_seconds: int = Field(
        default=3600,
        title="TTL (seconds)",
        description="Time to live. 0 = no expiry.",
        json_schema_extra={"min": 0},
    )
    data: str = Field(
        default="",
        title="Data",
        description="Data to cache (string or JSON)",
        json_schema_extra={"widget": "textarea", "rows": 4},
    )


class StoreCacheOutput(BlockOutput):
    stored: bool = False
    cache_name: str = ""
    ttl_seconds: int = 0


class StoreCacheBlock(BaseBlock[StoreCacheInput, StoreCacheOutput]):
    block_type: ClassVar[str] = "store_cache"
    icon: ClassVar[str] = "tabler/database-export"
    categories: ClassVar[list[str]] = ["core/cache"]
    description: ClassVar[str] = "Store data in a named cache with TTL"

    async def execute(
        self,
        input: StoreCacheInput,
        ctx: BlockContext | None = None,
    ) -> StoreCacheOutput:
        redis = _get_redis()
        if redis is None:
            return StoreCacheOutput(
                stored=False,
                cache_name=input.cache_name,
            )

        pipeline_id = ctx.pipeline_id if ctx else ""
        key = _cache_key(input.cache_name, input.scope, pipeline_id)

        if input.ttl_seconds > 0:
            await redis.setex(key, input.ttl_seconds, input.data)
        else:
            await redis.set(key, input.data)

        return StoreCacheOutput(
            stored=True,
            cache_name=input.cache_name,
            ttl_seconds=input.ttl_seconds,
        )


# ------------------------------------------------------------------
# ReadCache
# ------------------------------------------------------------------


class ReadCacheInput(BlockInput):
    cache_name: str = Field(
        title="Cache Name",
        description="Name of the cache entry to read",
    )
    scope: Literal["pipeline", "global"] = Field(
        default="pipeline",
        title="Scope",
        json_schema_extra={"widget": "select", "options": ["pipeline", "global"]},
    )


class ReadCacheOutput(BlockOutput):
    cache_hit: bool = False
    data: str = ""
    cache_name: str = ""


class ReadCacheBlock(BaseBlock[ReadCacheInput, ReadCacheOutput]):
    block_type: ClassVar[str] = "read_cache"
    icon: ClassVar[str] = "tabler/database-import"
    categories: ClassVar[list[str]] = ["core/cache"]
    description: ClassVar[str] = "Read data from a named cache"
    output_fittings: ClassVar[list[FittingDescriptor]] = [
        FittingDescriptor(uid="hit", label="Cache Hit", color="#4caf50", description="Data found in cache"),
        FittingDescriptor(uid="miss", label="Cache Miss", color="#ff9800", description="Data not in cache"),
    ]

    async def execute(
        self,
        input: ReadCacheInput,
        ctx: BlockContext | None = None,
    ) -> ReadCacheOutput:
        redis = _get_redis()
        if redis is None:
            return ReadCacheOutput(cache_name=input.cache_name)

        pipeline_id = ctx.pipeline_id if ctx else ""
        key = _cache_key(input.cache_name, input.scope, pipeline_id)

        raw = await redis.get(key)
        if raw is None:
            return ReadCacheOutput(
                cache_hit=False,
                cache_name=input.cache_name,
            )

        return ReadCacheOutput(
            cache_hit=True,
            data=raw,
            cache_name=input.cache_name,
        )


# ------------------------------------------------------------------
# DeleteCache
# ------------------------------------------------------------------


class DeleteCacheInput(BlockInput):
    cache_name: str = Field(
        title="Cache Name",
        description="Name of the cache entry to delete",
    )
    scope: Literal["pipeline", "global"] = Field(
        default="pipeline",
        title="Scope",
        json_schema_extra={"widget": "select", "options": ["pipeline", "global"]},
    )


class DeleteCacheOutput(BlockOutput):
    deleted: bool = False
    cache_name: str = ""


class DeleteCacheBlock(BaseBlock[DeleteCacheInput, DeleteCacheOutput]):
    block_type: ClassVar[str] = "delete_cache"
    icon: ClassVar[str] = "tabler/database-x"
    categories: ClassVar[list[str]] = ["core/cache"]
    description: ClassVar[str] = "Delete a named cache entry"

    async def execute(
        self,
        input: DeleteCacheInput,
        ctx: BlockContext | None = None,
    ) -> DeleteCacheOutput:
        redis = _get_redis()
        if redis is None:
            return DeleteCacheOutput(cache_name=input.cache_name)

        pipeline_id = ctx.pipeline_id if ctx else ""
        key = _cache_key(input.cache_name, input.scope, pipeline_id)

        count = await redis.delete(key)
        return DeleteCacheOutput(
            deleted=count > 0,
            cache_name=input.cache_name,
        )
