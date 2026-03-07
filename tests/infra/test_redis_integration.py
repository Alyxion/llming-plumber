"""Integration tests for Redis — real local instance.

Covers: basic ops, pub/sub broadcasts, key expiry, waiting for
keys (BLPOP/streams), pipeline batching, and Lua scripting.

Requires a running Redis on localhost:6379.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from redis.asyncio import Redis

pytestmark = pytest.mark.integration

TEST_PREFIX = "plumber:test:"


@pytest.fixture()
async def r():
    """Provide a Redis connection and clean up test keys after."""
    client = Redis(
        host="localhost",
        port=6379,
        db=1,  # use db 1 to avoid stomping real data
        decode_responses=True,
    )
    yield client
    # Clean up all test keys
    keys = [k async for k in client.scan_iter(f"{TEST_PREFIX}*")]
    if keys:
        await client.delete(*keys)
    await client.aclose()


# -------------------------------------------------------------------
# Basic connectivity
# -------------------------------------------------------------------


async def test_ping(r: Redis) -> None:
    assert await r.ping() is True


# -------------------------------------------------------------------
# Key/value basics
# -------------------------------------------------------------------


async def test_set_get_delete(r: Redis) -> None:
    key = f"{TEST_PREFIX}hello"
    await r.set(key, "world")
    assert await r.get(key) == "world"
    await r.delete(key)
    assert await r.get(key) is None


async def test_json_value(r: Redis) -> None:
    """Store and retrieve a JSON-serialized run status."""
    key = f"{TEST_PREFIX}run:status"
    payload = {
        "run_id": "abc-123",
        "status": "running",
        "lemming_id": "lem-1",
    }
    await r.set(key, json.dumps(payload))
    stored = json.loads(await r.get(key))  # type: ignore[arg-type]
    assert stored == payload


async def test_hash_operations(r: Redis) -> None:
    """Use a hash to store block states."""
    key = f"{TEST_PREFIX}run:blocks"
    await r.hset(key, mapping={
        "fetch": "completed",
        "transform": "running",
        "store": "pending",
    })

    assert await r.hget(key, "fetch") == "completed"
    assert await r.hlen(key) == 3

    all_states = await r.hgetall(key)
    assert all_states == {
        "fetch": "completed",
        "transform": "running",
        "store": "pending",
    }


# -------------------------------------------------------------------
# Key expiry / TTL
# -------------------------------------------------------------------


async def test_key_expiry(r: Redis) -> None:
    """Keys with TTL expire automatically."""
    key = f"{TEST_PREFIX}ephemeral"
    await r.set(key, "temp", ex=1)
    assert await r.get(key) == "temp"

    ttl = await r.ttl(key)
    assert 0 < ttl <= 1

    await asyncio.sleep(1.1)
    assert await r.get(key) is None


# -------------------------------------------------------------------
# Pub/Sub broadcasts
# -------------------------------------------------------------------


async def test_pubsub_broadcast(r: Redis) -> None:
    """Publish a run status update and receive it via subscription."""
    channel = f"{TEST_PREFIX}run_updates"
    received: list[dict[str, Any]] = []

    async def subscriber() -> None:
        pubsub = r.pubsub()
        await pubsub.subscribe(channel)
        async for message in pubsub.listen():
            if message["type"] == "message":
                received.append(json.loads(message["data"]))
                break
        await pubsub.unsubscribe(channel)
        await pubsub.aclose()

    # Start subscriber, give it a moment to connect
    sub_task = asyncio.create_task(subscriber())
    await asyncio.sleep(0.1)

    # Publish
    payload = {
        "run_id": "run-42",
        "status": "completed",
        "lemming_id": "lem-1",
    }
    listeners = await r.publish(channel, json.dumps(payload))
    assert listeners >= 1

    await asyncio.wait_for(sub_task, timeout=3.0)
    assert len(received) == 1
    assert received[0] == payload


async def test_pubsub_multiple_subscribers(r: Redis) -> None:
    """Multiple subscribers all receive the broadcast."""
    channel = f"{TEST_PREFIX}multi_sub"
    results: dict[str, list[str]] = {"a": [], "b": []}

    async def sub(name: str) -> None:
        pubsub = r.pubsub()
        await pubsub.subscribe(channel)
        async for msg in pubsub.listen():
            if msg["type"] == "message":
                results[name].append(msg["data"])
                break
        await pubsub.unsubscribe(channel)
        await pubsub.aclose()

    tasks = [
        asyncio.create_task(sub("a")),
        asyncio.create_task(sub("b")),
    ]
    await asyncio.sleep(0.1)

    await r.publish(channel, "hello")

    await asyncio.wait_for(
        asyncio.gather(*tasks), timeout=3.0,
    )
    assert results["a"] == ["hello"]
    assert results["b"] == ["hello"]


# -------------------------------------------------------------------
# Waiting for keys (BLPOP — blocking list pop)
# -------------------------------------------------------------------


async def test_blpop_wait_for_key(r: Redis) -> None:
    """BLPOP blocks until a value is pushed to the list."""
    key = f"{TEST_PREFIX}job_queue"
    received: list[str] = []

    async def waiter() -> None:
        result = await r.blpop(key, timeout=3)
        if result:
            received.append(result[1])

    task = asyncio.create_task(waiter())
    await asyncio.sleep(0.1)

    # Push a job
    await r.rpush(key, "job-123")

    await asyncio.wait_for(task, timeout=3.0)
    assert received == ["job-123"]


async def test_blpop_fifo_order(r: Redis) -> None:
    """BLPOP respects FIFO ordering."""
    key = f"{TEST_PREFIX}fifo"
    await r.rpush(key, "first", "second", "third")

    results = []
    for _ in range(3):
        result = await r.blpop(key, timeout=1)
        assert result is not None
        results.append(result[1])

    assert results == ["first", "second", "third"]


# -------------------------------------------------------------------
# Redis Streams (event log)
# -------------------------------------------------------------------


async def test_stream_add_and_read(r: Redis) -> None:
    """Add entries to a stream and read them back."""
    stream = f"{TEST_PREFIX}events"

    # Add events
    for i in range(5):
        await r.xadd(stream, {
            "run_id": f"run-{i}",
            "event": "block_completed",
            "block_id": f"b{i}",
        })

    # Read all entries
    entries = await r.xrange(stream)
    assert len(entries) == 5
    assert entries[0][1]["run_id"] == "run-0"
    assert entries[4][1]["block_id"] == "b4"


async def test_stream_consumer_group(r: Redis) -> None:
    """Consumer groups allow multiple workers to share a stream."""
    stream = f"{TEST_PREFIX}work_stream"
    group = "lemmings"

    # Add work items
    for i in range(4):
        await r.xadd(stream, {"task": f"task-{i}"})

    # Create consumer group
    await r.xgroup_create(stream, group, id="0")

    # Two consumers each claim work
    c1 = await r.xreadgroup(
        group, "lem-1", {stream: ">"}, count=2,
    )
    c2 = await r.xreadgroup(
        group, "lem-2", {stream: ">"}, count=2,
    )

    # Between them they should have all 4
    c1_entries = c1[0][1] if c1 else []
    c2_entries = c2[0][1] if c2 else []
    assert len(c1_entries) + len(c2_entries) == 4

    # Acknowledge
    for entry_id, _ in c1_entries:
        await r.xack(stream, group, entry_id)
    for entry_id, _ in c2_entries:
        await r.xack(stream, group, entry_id)


# -------------------------------------------------------------------
# Pipeline batching (Redis pipeline, not Plumber pipeline)
# -------------------------------------------------------------------


async def test_redis_pipeline_batch(r: Redis) -> None:
    """Batch multiple commands in a Redis pipeline for atomicity."""
    async with r.pipeline(transaction=True) as pipe:
        for i in range(10):
            pipe.set(f"{TEST_PREFIX}batch:{i}", str(i))
        results = await pipe.execute()

    assert all(results)

    # Verify all keys were set
    for i in range(10):
        val = await r.get(f"{TEST_PREFIX}batch:{i}")
        assert val == str(i)


# -------------------------------------------------------------------
# Atomic increment (rate limiting / counters)
# -------------------------------------------------------------------


async def test_atomic_increment(r: Redis) -> None:
    """INCR is atomic — safe for concurrent counters."""
    key = f"{TEST_PREFIX}counter"

    async def bump(n: int) -> None:
        for _ in range(n):
            await r.incr(key)

    await asyncio.gather(bump(50), bump(50))
    assert int(await r.get(key)) == 100  # type: ignore[arg-type]


# -------------------------------------------------------------------
# Lua scripting (conditional set)
# -------------------------------------------------------------------


async def test_lua_conditional_set(r: Redis) -> None:
    """Lua script for atomic compare-and-swap."""
    script = """
    local current = redis.call('GET', KEYS[1])
    if current == ARGV[1] then
        redis.call('SET', KEYS[1], ARGV[2])
        return 1
    end
    return 0
    """
    key = f"{TEST_PREFIX}cas"
    await r.set(key, "queued")

    # CAS: queued -> running (should succeed)
    result = await r.eval(script, 1, key, "queued", "running")
    assert result == 1
    assert await r.get(key) == "running"

    # CAS: queued -> completed (should fail — current is running)
    result = await r.eval(script, 1, key, "queued", "completed")
    assert result == 0
    assert await r.get(key) == "running"


# -------------------------------------------------------------------
# Set operations (tracking active lemmings)
# -------------------------------------------------------------------


async def test_set_operations(r: Redis) -> None:
    """Use sets to track online lemmings."""
    key = f"{TEST_PREFIX}lemmings:online"

    await r.sadd(key, "lem-1", "lem-2", "lem-3")
    assert await r.scard(key) == 3
    assert await r.sismember(key, "lem-2")

    await r.srem(key, "lem-2")
    assert await r.scard(key) == 2
    members = await r.smembers(key)
    assert members == {"lem-1", "lem-3"}
