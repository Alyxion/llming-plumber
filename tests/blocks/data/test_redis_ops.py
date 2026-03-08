"""Unit tests for Redis operation blocks — mocked Redis."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from llming_plumber.blocks.base import BlockContext


def _mock_redis() -> AsyncMock:
    r = AsyncMock()
    r.get = AsyncMock(return_value=None)
    r.set = AsyncMock()
    r.setex = AsyncMock()
    r.delete = AsyncMock(return_value=1)
    r.lpush = AsyncMock(return_value=1)
    r.rpush = AsyncMock(return_value=1)
    r.lpop = AsyncMock(return_value=None)
    r.rpop = AsyncMock(return_value=None)
    r.blpop = AsyncMock(return_value=None)
    r.brpop = AsyncMock(return_value=None)
    r.lrange = AsyncMock(return_value=[])
    r.publish = AsyncMock(return_value=0)
    r.hget = AsyncMock(return_value=None)
    r.hgetall = AsyncMock(return_value={})
    r.hmget = AsyncMock(return_value=[])
    r.hset = AsyncMock(return_value=0)
    r.keys = AsyncMock(return_value=[])
    r.incrby = AsyncMock(return_value=0)
    r.pubsub = MagicMock()
    return r


# ── RedisGetBlock ──


@pytest.mark.asyncio
async def test_get_found(monkeypatch: pytest.MonkeyPatch) -> None:
    from llming_plumber.blocks.data.redis_ops import RedisGetBlock, RedisGetInput

    redis = _mock_redis()
    redis.get = AsyncMock(return_value="hello")
    monkeypatch.setattr("llming_plumber.blocks.data.redis_ops._get_redis", lambda: redis)

    block = RedisGetBlock()
    result = await block.execute(RedisGetInput(key="mykey"))
    assert result.found is True
    assert result.value == "hello"


@pytest.mark.asyncio
async def test_get_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    from llming_plumber.blocks.data.redis_ops import RedisGetBlock, RedisGetInput

    redis = _mock_redis()
    monkeypatch.setattr("llming_plumber.blocks.data.redis_ops._get_redis", lambda: redis)

    block = RedisGetBlock()
    result = await block.execute(RedisGetInput(key="missing"))
    assert result.found is False
    assert result.value == ""


# ── RedisSetBlock ──


@pytest.mark.asyncio
async def test_set_without_ttl(monkeypatch: pytest.MonkeyPatch) -> None:
    from llming_plumber.blocks.data.redis_ops import RedisSetBlock, RedisSetInput

    redis = _mock_redis()
    monkeypatch.setattr("llming_plumber.blocks.data.redis_ops._get_redis", lambda: redis)

    block = RedisSetBlock()
    result = await block.execute(RedisSetInput(key="foo", value="bar"))
    assert result.success is True
    redis.set.assert_called_once_with("foo", "bar")


@pytest.mark.asyncio
async def test_set_with_ttl(monkeypatch: pytest.MonkeyPatch) -> None:
    from llming_plumber.blocks.data.redis_ops import RedisSetBlock, RedisSetInput

    redis = _mock_redis()
    monkeypatch.setattr("llming_plumber.blocks.data.redis_ops._get_redis", lambda: redis)

    block = RedisSetBlock()
    result = await block.execute(RedisSetInput(key="foo", value="bar", ttl_seconds=60))
    assert result.success is True
    redis.setex.assert_called_once_with("foo", 60, "bar")


# ── RedisDeleteBlock ──


@pytest.mark.asyncio
async def test_delete_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    from llming_plumber.blocks.data.redis_ops import RedisDeleteBlock, RedisDeleteInput

    redis = _mock_redis()
    redis.delete = AsyncMock(return_value=2)
    monkeypatch.setattr("llming_plumber.blocks.data.redis_ops._get_redis", lambda: redis)

    block = RedisDeleteBlock()
    result = await block.execute(RedisDeleteInput(keys="key1,key2"))
    assert result.deleted_count == 2


# ── RedisListPushBlock ──


@pytest.mark.asyncio
async def test_list_push_right(monkeypatch: pytest.MonkeyPatch) -> None:
    from llming_plumber.blocks.data.redis_ops import RedisListPushBlock, RedisListPushInput

    redis = _mock_redis()
    redis.rpush = AsyncMock(return_value=3)
    monkeypatch.setattr("llming_plumber.blocks.data.redis_ops._get_redis", lambda: redis)

    block = RedisListPushBlock()
    result = await block.execute(
        RedisListPushInput(key="mylist", values="a\nb\nc", direction="right"),
    )
    assert result.list_length == 3


@pytest.mark.asyncio
async def test_list_push_left(monkeypatch: pytest.MonkeyPatch) -> None:
    from llming_plumber.blocks.data.redis_ops import RedisListPushBlock, RedisListPushInput

    redis = _mock_redis()
    redis.lpush = AsyncMock(return_value=2)
    monkeypatch.setattr("llming_plumber.blocks.data.redis_ops._get_redis", lambda: redis)

    block = RedisListPushBlock()
    result = await block.execute(
        RedisListPushInput(key="mylist", values='["x", "y"]', direction="left"),
    )
    assert result.list_length == 2


# ── RedisListPopBlock ──


@pytest.mark.asyncio
async def test_list_pop(monkeypatch: pytest.MonkeyPatch) -> None:
    from llming_plumber.blocks.data.redis_ops import RedisListPopBlock, RedisListPopInput

    redis = _mock_redis()
    redis.lpop = AsyncMock(return_value="item1")
    monkeypatch.setattr("llming_plumber.blocks.data.redis_ops._get_redis", lambda: redis)

    block = RedisListPopBlock()
    result = await block.execute(RedisListPopInput(key="mylist"))
    assert len(result.values) == 1
    assert result.values[0] == "item1"


# ── RedisListRangeBlock ──


@pytest.mark.asyncio
async def test_list_range(monkeypatch: pytest.MonkeyPatch) -> None:
    from llming_plumber.blocks.data.redis_ops import RedisListRangeBlock, RedisListRangeInput

    redis = _mock_redis()
    redis.lrange = AsyncMock(return_value=["a", "b", "c"])
    monkeypatch.setattr("llming_plumber.blocks.data.redis_ops._get_redis", lambda: redis)

    block = RedisListRangeBlock()
    result = await block.execute(RedisListRangeInput(key="mylist"))
    assert result.count == 3
    assert result.values == ["a", "b", "c"]


# ── RedisPublishBlock ──


@pytest.mark.asyncio
async def test_publish(monkeypatch: pytest.MonkeyPatch) -> None:
    from llming_plumber.blocks.data.redis_ops import RedisPublishBlock, RedisPublishInput

    redis = _mock_redis()
    redis.publish = AsyncMock(return_value=2)
    monkeypatch.setattr("llming_plumber.blocks.data.redis_ops._get_redis", lambda: redis)

    block = RedisPublishBlock()
    result = await block.execute(RedisPublishInput(channel="events", message="hello"))
    assert result.receivers == 2


# ── RedisHashGetBlock ──


@pytest.mark.asyncio
async def test_hash_get_all(monkeypatch: pytest.MonkeyPatch) -> None:
    from llming_plumber.blocks.data.redis_ops import RedisHashGetBlock, RedisHashGetInput

    redis = _mock_redis()
    redis.hgetall = AsyncMock(return_value={"name": "Alice", "age": "30"})
    monkeypatch.setattr("llming_plumber.blocks.data.redis_ops._get_redis", lambda: redis)

    block = RedisHashGetBlock()
    result = await block.execute(RedisHashGetInput(key="user:1"))
    assert result.found is True
    assert result.data["name"] == "Alice"


@pytest.mark.asyncio
async def test_hash_get_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    from llming_plumber.blocks.data.redis_ops import RedisHashGetBlock, RedisHashGetInput

    redis = _mock_redis()
    redis.hgetall = AsyncMock(return_value={})
    monkeypatch.setattr("llming_plumber.blocks.data.redis_ops._get_redis", lambda: redis)

    block = RedisHashGetBlock()
    result = await block.execute(RedisHashGetInput(key="user:999"))
    assert result.found is False


# ── RedisHashSetBlock ──


@pytest.mark.asyncio
async def test_hash_set(monkeypatch: pytest.MonkeyPatch) -> None:
    from llming_plumber.blocks.data.redis_ops import RedisHashSetBlock, RedisHashSetInput

    redis = _mock_redis()
    redis.hset = AsyncMock(return_value=2)
    monkeypatch.setattr("llming_plumber.blocks.data.redis_ops._get_redis", lambda: redis)

    block = RedisHashSetBlock()
    result = await block.execute(
        RedisHashSetInput(key="user:1", data='{"name": "Bob", "age": "25"}'),
    )
    assert result.fields_set == 2


# ── RedisKeysBlock ──


@pytest.mark.asyncio
async def test_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    from llming_plumber.blocks.data.redis_ops import RedisKeysBlock, RedisKeysInput

    redis = _mock_redis()
    redis.keys = AsyncMock(return_value=["user:1", "user:2"])
    monkeypatch.setattr("llming_plumber.blocks.data.redis_ops._get_redis", lambda: redis)

    block = RedisKeysBlock()
    result = await block.execute(RedisKeysInput(pattern="user:*"))
    assert result.count == 2
    assert "user:1" in result.keys


# ── RedisIncrBlock ──


@pytest.mark.asyncio
async def test_incr(monkeypatch: pytest.MonkeyPatch) -> None:
    from llming_plumber.blocks.data.redis_ops import RedisIncrBlock, RedisIncrInput

    redis = _mock_redis()
    redis.incrby = AsyncMock(return_value=5)
    monkeypatch.setattr("llming_plumber.blocks.data.redis_ops._get_redis", lambda: redis)

    block = RedisIncrBlock()
    result = await block.execute(RedisIncrInput(key="counter", amount=5))
    assert result.value == 5


# ── No redis available ──


@pytest.mark.asyncio
async def test_get_no_redis(monkeypatch: pytest.MonkeyPatch) -> None:
    from llming_plumber.blocks.data.redis_ops import RedisGetBlock, RedisGetInput

    monkeypatch.setattr("llming_plumber.blocks.data.redis_ops._get_redis", lambda: None)

    block = RedisGetBlock()
    result = await block.execute(RedisGetInput(key="foo"))
    assert result.found is False


# ── Registry ──


def test_redis_blocks_in_registry() -> None:
    from llming_plumber.blocks.registry import BlockRegistry

    BlockRegistry.reset()
    BlockRegistry.discover()
    for bt in [
        "redis_get", "redis_set", "redis_delete", "redis_list_push",
        "redis_list_pop", "redis_list_range", "redis_publish",
        "redis_hash_get", "redis_hash_set", "redis_keys", "redis_incr",
    ]:
        assert bt in BlockRegistry._registry, f"{bt} not registered"
