"""Integration tests for Redis operation blocks — requires a running Redis server."""

from __future__ import annotations

import pytest


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.fixture
async def redis(monkeypatch: pytest.MonkeyPatch):
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

    monkeypatch.setattr("llming_plumber.blocks.data.redis_ops._get_redis", lambda: r)

    # Clean up test keys after each test
    yield r
    for key in await r.keys("plumber:test:*"):
        await r.delete(key)


async def test_get_set_roundtrip(redis) -> None:
    from llming_plumber.blocks.data.redis_ops import (
        RedisGetBlock,
        RedisGetInput,
        RedisSetBlock,
        RedisSetInput,
    )

    # Set
    set_block = RedisSetBlock()
    result = await set_block.execute(
        RedisSetInput(key="plumber:test:hello", value="world", ttl_seconds=60),
    )
    assert result.success is True

    # Get
    get_block = RedisGetBlock()
    result = await get_block.execute(RedisGetInput(key="plumber:test:hello"))
    assert result.found is True
    assert result.value == "world"


async def test_delete(redis) -> None:
    from llming_plumber.blocks.data.redis_ops import (
        RedisDeleteBlock,
        RedisDeleteInput,
        RedisSetBlock,
        RedisSetInput,
    )

    await RedisSetBlock().execute(RedisSetInput(key="plumber:test:del1", value="x"))
    await RedisSetBlock().execute(RedisSetInput(key="plumber:test:del2", value="y"))

    result = await RedisDeleteBlock().execute(
        RedisDeleteInput(keys="plumber:test:del1,plumber:test:del2"),
    )
    assert result.deleted_count == 2


async def test_list_push_pop(redis) -> None:
    from llming_plumber.blocks.data.redis_ops import (
        RedisListPopBlock,
        RedisListPopInput,
        RedisListPushBlock,
        RedisListPushInput,
    )

    push = await RedisListPushBlock().execute(
        RedisListPushInput(key="plumber:test:list", values="a\nb\nc", direction="right"),
    )
    assert push.list_length == 3

    pop = await RedisListPopBlock().execute(
        RedisListPopInput(key="plumber:test:list", direction="left"),
    )
    assert pop.values == ["a"]


async def test_list_range(redis) -> None:
    from llming_plumber.blocks.data.redis_ops import (
        RedisListPushBlock,
        RedisListPushInput,
        RedisListRangeBlock,
        RedisListRangeInput,
    )

    await RedisListPushBlock().execute(
        RedisListPushInput(key="plumber:test:range", values="x\ny\nz"),
    )

    result = await RedisListRangeBlock().execute(
        RedisListRangeInput(key="plumber:test:range"),
    )
    assert result.count == 3
    assert result.values == ["x", "y", "z"]


async def test_hash_set_get(redis) -> None:
    from llming_plumber.blocks.data.redis_ops import (
        RedisHashGetBlock,
        RedisHashGetInput,
        RedisHashSetBlock,
        RedisHashSetInput,
    )

    await RedisHashSetBlock().execute(
        RedisHashSetInput(key="plumber:test:hash", data='{"name": "Alice", "age": "30"}'),
    )

    result = await RedisHashGetBlock().execute(
        RedisHashGetInput(key="plumber:test:hash"),
    )
    assert result.found is True
    assert result.data["name"] == "Alice"


async def test_keys_pattern(redis) -> None:
    from llming_plumber.blocks.data.redis_ops import (
        RedisKeysBlock,
        RedisKeysInput,
        RedisSetBlock,
        RedisSetInput,
    )

    await RedisSetBlock().execute(RedisSetInput(key="plumber:test:k1", value="1"))
    await RedisSetBlock().execute(RedisSetInput(key="plumber:test:k2", value="2"))

    result = await RedisKeysBlock().execute(
        RedisKeysInput(pattern="plumber:test:k*"),
    )
    assert result.count == 2


async def test_incr(redis) -> None:
    from llming_plumber.blocks.data.redis_ops import RedisIncrBlock, RedisIncrInput

    block = RedisIncrBlock()
    r1 = await block.execute(RedisIncrInput(key="plumber:test:counter", amount=5))
    assert r1.value == 5

    r2 = await block.execute(RedisIncrInput(key="plumber:test:counter", amount=3))
    assert r2.value == 8


async def test_publish(redis) -> None:
    from llming_plumber.blocks.data.redis_ops import RedisPublishBlock, RedisPublishInput

    # No subscribers, but should succeed
    result = await RedisPublishBlock().execute(
        RedisPublishInput(channel="plumber:test:chan", message="hello"),
    )
    assert result.receivers == 0
