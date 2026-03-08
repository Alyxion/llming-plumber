"""Integration tests for cache blocks — real Redis.

Run with: pytest -m integration tests/blocks/core/test_cache_integration.py -v
"""

from __future__ import annotations

import asyncio
import uuid

import pytest

from llming_plumber.blocks.base import BlockContext
from llming_plumber.blocks.core.cache import (
    DeleteCacheBlock,
    DeleteCacheInput,
    ReadCacheBlock,
    ReadCacheInput,
    StoreCacheBlock,
    StoreCacheInput,
)

pytestmark = pytest.mark.integration


def _unique(prefix: str = "") -> str:
    return f"test_{prefix}{uuid.uuid4().hex[:8]}"


@pytest.fixture
async def redis(monkeypatch: pytest.MonkeyPatch):
    """Create a fresh Redis connection and patch _get_redis."""
    import redis.asyncio as aioredis
    try:
        r = aioredis.from_url(
            "redis://localhost:6379",
            decode_responses=True,
            single_connection_client=True,
        )
        await r.ping()
    except Exception as exc:
        pytest.skip(f"Redis not available: {exc}")
    monkeypatch.setattr("llming_plumber.blocks.core.cache._get_redis", lambda: r)
    return r


@pytest.fixture
def pipeline_id() -> str:
    return _unique("pl_")


def _ctx(pipeline_id: str = "testpl") -> BlockContext:
    return BlockContext(pipeline_id=pipeline_id, run_id="testrun")


# ── Store + Read cycle ──


async def test_store_and_read(redis, pipeline_id: str) -> None:
    ctx = _ctx(pipeline_id)
    name = _unique("cache_")

    store_block = StoreCacheBlock()
    store_result = await store_block.execute(
        StoreCacheInput(cache_name=name, data='{"temp": 20}', ttl_seconds=60),
        ctx=ctx,
    )
    assert store_result.stored is True

    read_block = ReadCacheBlock()
    read_result = await read_block.execute(
        ReadCacheInput(cache_name=name),
        ctx=ctx,
    )
    assert read_result.cache_hit is True
    assert read_result.data == '{"temp": 20}'

    del_block = DeleteCacheBlock()
    await del_block.execute(DeleteCacheInput(cache_name=name), ctx=ctx)


async def test_read_miss(redis, pipeline_id: str) -> None:
    ctx = _ctx(pipeline_id)
    name = _unique("nonexistent_")

    read_block = ReadCacheBlock()
    result = await read_block.execute(
        ReadCacheInput(cache_name=name),
        ctx=ctx,
    )
    assert result.cache_hit is False
    assert result.data == ""


# ── Delete ──


async def test_delete_existing(redis, pipeline_id: str) -> None:
    ctx = _ctx(pipeline_id)
    name = _unique("del_")

    store_block = StoreCacheBlock()
    await store_block.execute(
        StoreCacheInput(cache_name=name, data="val", ttl_seconds=60),
        ctx=ctx,
    )

    del_block = DeleteCacheBlock()
    result = await del_block.execute(
        DeleteCacheInput(cache_name=name),
        ctx=ctx,
    )
    assert result.deleted is True

    read_block = ReadCacheBlock()
    read_result = await read_block.execute(
        ReadCacheInput(cache_name=name),
        ctx=ctx,
    )
    assert read_result.cache_hit is False


async def test_delete_nonexistent(redis, pipeline_id: str) -> None:
    ctx = _ctx(pipeline_id)
    name = _unique("ghost_")

    del_block = DeleteCacheBlock()
    result = await del_block.execute(
        DeleteCacheInput(cache_name=name),
        ctx=ctx,
    )
    assert result.deleted is False


# ── TTL expiry ──


async def test_ttl_expiry(redis, pipeline_id: str) -> None:
    ctx = _ctx(pipeline_id)
    name = _unique("ttl_")

    store_block = StoreCacheBlock()
    await store_block.execute(
        StoreCacheInput(cache_name=name, data="ephemeral", ttl_seconds=1),
        ctx=ctx,
    )

    read_block = ReadCacheBlock()
    result = await read_block.execute(ReadCacheInput(cache_name=name), ctx=ctx)
    assert result.cache_hit is True

    await asyncio.sleep(1.5)

    result = await read_block.execute(ReadCacheInput(cache_name=name), ctx=ctx)
    assert result.cache_hit is False


# ── No TTL (persistent) ──


async def test_no_ttl(redis, pipeline_id: str) -> None:
    ctx = _ctx(pipeline_id)
    name = _unique("perm_")

    store_block = StoreCacheBlock()
    await store_block.execute(
        StoreCacheInput(cache_name=name, data="persistent", ttl_seconds=0),
        ctx=ctx,
    )

    read_block = ReadCacheBlock()
    result = await read_block.execute(ReadCacheInput(cache_name=name), ctx=ctx)
    assert result.cache_hit is True
    assert result.data == "persistent"

    del_block = DeleteCacheBlock()
    await del_block.execute(DeleteCacheInput(cache_name=name), ctx=ctx)


# ── Global scope ──


async def test_global_scope_shared(redis) -> None:
    name = _unique("global_")
    ctx1 = _ctx("pipeline_A")
    ctx2 = _ctx("pipeline_B")

    store_block = StoreCacheBlock()
    await store_block.execute(
        StoreCacheInput(cache_name=name, data="shared_data", scope="global", ttl_seconds=60),
        ctx=ctx1,
    )

    read_block = ReadCacheBlock()
    result = await read_block.execute(
        ReadCacheInput(cache_name=name, scope="global"),
        ctx=ctx2,
    )
    assert result.cache_hit is True
    assert result.data == "shared_data"

    del_block = DeleteCacheBlock()
    await del_block.execute(
        DeleteCacheInput(cache_name=name, scope="global"),
        ctx=ctx1,
    )


# ── Pipeline isolation ──


async def test_pipeline_scope_isolated(redis) -> None:
    name = _unique("iso_")
    ctx_a = _ctx("pipeline_A")
    ctx_b = _ctx("pipeline_B")

    store_block = StoreCacheBlock()
    await store_block.execute(
        StoreCacheInput(cache_name=name, data="A_data", ttl_seconds=60),
        ctx=ctx_a,
    )

    read_block = ReadCacheBlock()
    result = await read_block.execute(
        ReadCacheInput(cache_name=name),
        ctx=ctx_b,
    )
    assert result.cache_hit is False

    result = await read_block.execute(
        ReadCacheInput(cache_name=name),
        ctx=ctx_a,
    )
    assert result.cache_hit is True
    assert result.data == "A_data"

    del_block = DeleteCacheBlock()
    await del_block.execute(DeleteCacheInput(cache_name=name), ctx=ctx_a)


# ── Overwrite ──


async def test_overwrite_cache(redis, pipeline_id: str) -> None:
    ctx = _ctx(pipeline_id)
    name = _unique("overwrite_")

    store_block = StoreCacheBlock()
    await store_block.execute(
        StoreCacheInput(cache_name=name, data="v1", ttl_seconds=60),
        ctx=ctx,
    )

    await store_block.execute(
        StoreCacheInput(cache_name=name, data="v2", ttl_seconds=60),
        ctx=ctx,
    )

    read_block = ReadCacheBlock()
    result = await read_block.execute(ReadCacheInput(cache_name=name), ctx=ctx)
    assert result.cache_hit is True
    assert result.data == "v2"

    del_block = DeleteCacheBlock()
    await del_block.execute(DeleteCacheInput(cache_name=name), ctx=ctx)


# ── Large data ──


async def test_large_data(redis, pipeline_id: str) -> None:
    ctx = _ctx(pipeline_id)
    name = _unique("large_")
    big_data = "x" * 10_000

    store_block = StoreCacheBlock()
    await store_block.execute(
        StoreCacheInput(cache_name=name, data=big_data, ttl_seconds=60),
        ctx=ctx,
    )

    read_block = ReadCacheBlock()
    result = await read_block.execute(ReadCacheInput(cache_name=name), ctx=ctx)
    assert result.cache_hit is True
    assert len(result.data) == 10_000

    del_block = DeleteCacheBlock()
    await del_block.execute(DeleteCacheInput(cache_name=name), ctx=ctx)
