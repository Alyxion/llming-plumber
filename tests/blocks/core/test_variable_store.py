"""Unit tests for VariableStore — mocked Redis, no real connections."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from llming_plumber.blocks.core.variable_store import VariableStore


def _mock_redis() -> AsyncMock:
    """Create a mock Redis client with common async methods."""
    r = AsyncMock()
    r.get = AsyncMock(return_value=None)
    r.set = AsyncMock()
    r.delete = AsyncMock(return_value=1)
    r.incrbyfloat = AsyncMock(return_value="5")
    r.append = AsyncMock()
    r.sismember = AsyncMock(return_value=True)
    r.sadd = AsyncMock()
    r.srem = AsyncMock()

    # scan_iter returns async iterator
    async def _empty_scan(**kw: Any) -> Any:
        return
        yield  # make it an async generator

    r.scan_iter = _empty_scan
    return r


# ── Local scope ──


async def test_local_set_and_get() -> None:
    store = VariableStore(None, "pl1", "run1")
    await store.set("x", 42)
    assert await store.get("x") == 42


async def test_local_delete() -> None:
    store = VariableStore(None, "pl1", "run1")
    await store.set("x", 10)
    await store.delete("x")
    assert await store.get("x") is None


async def test_local_incr() -> None:
    store = VariableStore(None, "pl1", "run1")
    result = await store.incr("counter", 5)
    assert result == 5.0
    result = await store.incr("counter", 3)
    assert result == 8.0


async def test_local_decr() -> None:
    store = VariableStore(None, "pl1", "run1")
    await store.set("counter", 10)
    result = await store.decr("counter", 3)
    assert result == 7.0


async def test_local_append() -> None:
    store = VariableStore(None, "pl1", "run1")
    result = await store.append("msg", "hello")
    assert result == "hello"
    result = await store.append("msg", " world")
    assert result == "hello world"


async def test_get_local_vars() -> None:
    store = VariableStore(None, "pl1", "run1")
    await store.set("a", 1)
    await store.set("b", "two")
    assert store.get_local_vars() == {"a": 1, "b": "two"}


async def test_fetch_alias() -> None:
    store = VariableStore(None, "pl1", "run1")
    await store.set("x", 99)
    assert await store.fetch("x") == 99


# ── Pipeline scope ──


async def test_pipeline_set_and_get() -> None:
    redis = _mock_redis()
    redis.get = AsyncMock(return_value="42")
    store = VariableStore(redis, "pl1", "run1")
    await store.set("pl_count", 42)
    redis.set.assert_called_once()
    key = redis.set.call_args[0][0]
    assert "pl1" in key
    assert "count" in key

    result = await store.get("pl_count")
    assert result == 42


async def test_pipeline_incr() -> None:
    redis = _mock_redis()
    redis.incrbyfloat = AsyncMock(return_value="10")
    store = VariableStore(redis, "pl1", "run1")
    result = await store.incr("pl_count", 10)
    assert result == 10.0


async def test_pipeline_delete() -> None:
    redis = _mock_redis()
    store = VariableStore(redis, "pl1", "run1")
    await store.delete("pl_count")
    redis.delete.assert_called_once()


# ── Job scope ──


async def test_job_set_and_get() -> None:
    redis = _mock_redis()
    redis.get = AsyncMock(return_value="processing")
    store = VariableStore(redis, "pl1", "run1")
    await store.set("job_status", "processing")
    key = redis.set.call_args[0][0]
    assert "run1" in key
    assert "status" in key


async def test_job_get_vars() -> None:
    redis = _mock_redis()

    async def scan_with_results(**kw: Any) -> Any:
        yield "plumber:var:job:run1:status"

    redis.scan_iter = scan_with_results
    redis.get = AsyncMock(return_value="done")

    store = VariableStore(redis, "pl1", "run1")
    result = await store.get_job_vars()
    assert "job_status" in result
    assert result["job_status"] == "done"


# ── Global scope (access control) ──


async def test_global_requires_grant() -> None:
    redis = _mock_redis()
    redis.sismember = AsyncMock(return_value=False)
    store = VariableStore(redis, "pl1", "run1")

    with pytest.raises(PermissionError, match="not granted"):
        await store.get("gl_total")

    with pytest.raises(PermissionError, match="not granted"):
        await store.set("gl_total", 100)

    with pytest.raises(PermissionError, match="not granted"):
        await store.incr("gl_total", 1)

    with pytest.raises(PermissionError, match="not granted"):
        await store.append("gl_label", "x")


async def test_global_allowed_when_granted() -> None:
    redis = _mock_redis()
    redis.sismember = AsyncMock(return_value=True)
    redis.get = AsyncMock(return_value="100")
    store = VariableStore(redis, "pl1", "run1")

    result = await store.get("gl_total")
    assert result == 100


async def test_grant_and_revoke() -> None:
    redis = _mock_redis()
    await VariableStore.grant_global_access(redis, "total", "pl1")
    redis.sadd.assert_called_once()

    await VariableStore.revoke_global_access(redis, "total", "pl1")
    redis.srem.assert_called_once()


async def test_check_global_access_no_redis() -> None:
    store = VariableStore(None, "pl1", "run1")
    assert await store.check_global_access("total") is False


# ── No-redis fallbacks ──


async def test_no_redis_pipeline_get_returns_none() -> None:
    store = VariableStore(None, "pl1", "run1")
    assert await store.get("pl_count") is None


async def test_no_redis_pipeline_set_noop() -> None:
    store = VariableStore(None, "pl1", "run1")
    await store.set("pl_count", 10)  # should not raise


async def test_no_redis_pipeline_delete_noop() -> None:
    store = VariableStore(None, "pl1", "run1")
    await store.delete("pl_count")  # should not raise


async def test_no_redis_incr_returns_amount() -> None:
    store = VariableStore(None, "pl1", "run1")
    result = await store.incr("pl_count", 5)
    assert result == 5.0


async def test_no_redis_append_returns_suffix() -> None:
    store = VariableStore(None, "pl1", "run1")
    result = await store.append("pl_label", "hello")
    assert result == "hello"


async def test_no_redis_job_vars_empty() -> None:
    store = VariableStore(None, "pl1", "run1")
    assert await store.get_job_vars() == {}


# ── Pipeline append ──


async def test_pipeline_append() -> None:
    redis = _mock_redis()
    redis.sismember = AsyncMock(return_value=True)
    redis.get = AsyncMock(return_value="hello world")
    store = VariableStore(redis, "pl1", "run1")
    result = await store.append("pl_msg", " world")
    assert result == "hello world"
    redis.append.assert_called_once()


# ── Float incr ──


async def test_incr_float_amount() -> None:
    redis = _mock_redis()
    redis.incrbyfloat = AsyncMock(return_value="3.14")
    store = VariableStore(redis, "pl1", "run1")
    result = await store.incr("pl_score", 3.14)
    assert result == 3.14
