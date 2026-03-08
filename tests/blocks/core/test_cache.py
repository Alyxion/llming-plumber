"""Unit tests for cache blocks — mocked Redis."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from llming_plumber.blocks.base import BlockContext
from llming_plumber.blocks.core.cache import (
    DeleteCacheBlock,
    DeleteCacheInput,
    ReadCacheBlock,
    ReadCacheInput,
    ReadCacheOutput,
    StoreCacheBlock,
    StoreCacheInput,
    StoreCacheOutput,
    DeleteCacheOutput,
    _cache_key,
)


def _mock_redis() -> AsyncMock:
    r = AsyncMock()
    r.get = AsyncMock(return_value=None)
    r.set = AsyncMock()
    r.setex = AsyncMock()
    r.delete = AsyncMock(return_value=1)
    return r


def _ctx(pipeline_id: str = "pl1") -> BlockContext:
    return BlockContext(pipeline_id=pipeline_id, run_id="run1")


# ── _cache_key ──


def test_cache_key_pipeline() -> None:
    key = _cache_key("mydata", "pipeline", "pl1")
    assert key == "plumber:cache:pl:pl1:mydata"


def test_cache_key_global() -> None:
    key = _cache_key("mydata", "global")
    assert key == "plumber:cache:gl:mydata"


# ── StoreCacheBlock ──


async def test_store_with_ttl(monkeypatch: pytest.MonkeyPatch) -> None:
    redis = _mock_redis()
    monkeypatch.setattr("llming_plumber.blocks.core.cache._get_redis", lambda: redis)

    block = StoreCacheBlock()
    result = await block.execute(
        StoreCacheInput(cache_name="weather", data='{"temp": 20}', ttl_seconds=300),
        ctx=_ctx(),
    )
    assert isinstance(result, StoreCacheOutput)
    assert result.stored is True
    assert result.cache_name == "weather"
    assert result.ttl_seconds == 300
    redis.setex.assert_called_once()


async def test_store_no_ttl(monkeypatch: pytest.MonkeyPatch) -> None:
    redis = _mock_redis()
    monkeypatch.setattr("llming_plumber.blocks.core.cache._get_redis", lambda: redis)

    block = StoreCacheBlock()
    result = await block.execute(
        StoreCacheInput(cache_name="config", data="val", ttl_seconds=0),
        ctx=_ctx(),
    )
    assert result.stored is True
    redis.set.assert_called_once()
    redis.setex.assert_not_called()


async def test_store_no_redis(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("llming_plumber.blocks.core.cache._get_redis", lambda: None)

    block = StoreCacheBlock()
    result = await block.execute(
        StoreCacheInput(cache_name="x", data="y"),
        ctx=_ctx(),
    )
    assert result.stored is False


async def test_store_global_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    redis = _mock_redis()
    monkeypatch.setattr("llming_plumber.blocks.core.cache._get_redis", lambda: redis)

    block = StoreCacheBlock()
    result = await block.execute(
        StoreCacheInput(cache_name="shared", data="data", scope="global", ttl_seconds=60),
        ctx=_ctx(),
    )
    assert result.stored is True
    key = redis.setex.call_args[0][0]
    assert "gl:" in key
    assert "shared" in key


# ── ReadCacheBlock ──


async def test_read_cache_hit(monkeypatch: pytest.MonkeyPatch) -> None:
    redis = _mock_redis()
    redis.get = AsyncMock(return_value='{"temp": 20}')
    monkeypatch.setattr("llming_plumber.blocks.core.cache._get_redis", lambda: redis)

    block = ReadCacheBlock()
    result = await block.execute(
        ReadCacheInput(cache_name="weather"),
        ctx=_ctx(),
    )
    assert isinstance(result, ReadCacheOutput)
    assert result.cache_hit is True
    assert result.data == '{"temp": 20}'
    assert result.cache_name == "weather"


async def test_read_cache_miss(monkeypatch: pytest.MonkeyPatch) -> None:
    redis = _mock_redis()
    redis.get = AsyncMock(return_value=None)
    monkeypatch.setattr("llming_plumber.blocks.core.cache._get_redis", lambda: redis)

    block = ReadCacheBlock()
    result = await block.execute(
        ReadCacheInput(cache_name="weather"),
        ctx=_ctx(),
    )
    assert result.cache_hit is False
    assert result.data == ""


async def test_read_cache_no_redis(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("llming_plumber.blocks.core.cache._get_redis", lambda: None)

    block = ReadCacheBlock()
    result = await block.execute(
        ReadCacheInput(cache_name="x"),
        ctx=_ctx(),
    )
    assert result.cache_hit is False
    assert result.cache_name == "x"


async def test_read_global_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    redis = _mock_redis()
    redis.get = AsyncMock(return_value="cached")
    monkeypatch.setattr("llming_plumber.blocks.core.cache._get_redis", lambda: redis)

    block = ReadCacheBlock()
    result = await block.execute(
        ReadCacheInput(cache_name="shared", scope="global"),
        ctx=_ctx(),
    )
    assert result.cache_hit is True
    assert result.data == "cached"


# ── DeleteCacheBlock ──


async def test_delete_cache_exists(monkeypatch: pytest.MonkeyPatch) -> None:
    redis = _mock_redis()
    redis.delete = AsyncMock(return_value=1)
    monkeypatch.setattr("llming_plumber.blocks.core.cache._get_redis", lambda: redis)

    block = DeleteCacheBlock()
    result = await block.execute(
        DeleteCacheInput(cache_name="weather"),
        ctx=_ctx(),
    )
    assert isinstance(result, DeleteCacheOutput)
    assert result.deleted is True


async def test_delete_cache_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    redis = _mock_redis()
    redis.delete = AsyncMock(return_value=0)
    monkeypatch.setattr("llming_plumber.blocks.core.cache._get_redis", lambda: redis)

    block = DeleteCacheBlock()
    result = await block.execute(
        DeleteCacheInput(cache_name="missing"),
        ctx=_ctx(),
    )
    assert result.deleted is False


async def test_delete_cache_no_redis(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("llming_plumber.blocks.core.cache._get_redis", lambda: None)

    block = DeleteCacheBlock()
    result = await block.execute(
        DeleteCacheInput(cache_name="x"),
        ctx=_ctx(),
    )
    assert result.deleted is False


# ── Block metadata ──


def test_store_block_type() -> None:
    assert StoreCacheBlock.block_type == "store_cache"


def test_read_block_type() -> None:
    assert ReadCacheBlock.block_type == "read_cache"


def test_delete_block_type() -> None:
    assert DeleteCacheBlock.block_type == "delete_cache"


def test_categories() -> None:
    assert "core/cache" in StoreCacheBlock.categories
    assert "core/cache" in ReadCacheBlock.categories
    assert "core/cache" in DeleteCacheBlock.categories


# ── No context (standalone) ──


async def test_store_no_context(monkeypatch: pytest.MonkeyPatch) -> None:
    redis = _mock_redis()
    monkeypatch.setattr("llming_plumber.blocks.core.cache._get_redis", lambda: redis)

    block = StoreCacheBlock()
    result = await block.execute(
        StoreCacheInput(cache_name="test", data="val", ttl_seconds=60),
    )
    assert result.stored is True
    key = redis.setex.call_args[0][0]
    # Pipeline key with empty pipeline_id
    assert "pl:" in key


async def test_read_no_context(monkeypatch: pytest.MonkeyPatch) -> None:
    redis = _mock_redis()
    redis.get = AsyncMock(return_value="val")
    monkeypatch.setattr("llming_plumber.blocks.core.cache._get_redis", lambda: redis)

    block = ReadCacheBlock()
    result = await block.execute(ReadCacheInput(cache_name="test"))
    assert result.cache_hit is True


async def test_delete_no_context(monkeypatch: pytest.MonkeyPatch) -> None:
    redis = _mock_redis()
    redis.delete = AsyncMock(return_value=1)
    monkeypatch.setattr("llming_plumber.blocks.core.cache._get_redis", lambda: redis)

    block = DeleteCacheBlock()
    result = await block.execute(DeleteCacheInput(cache_name="test"))
    assert result.deleted is True
