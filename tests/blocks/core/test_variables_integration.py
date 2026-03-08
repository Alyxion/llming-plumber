"""Integration tests for variable store & set_variables block — real Redis.

Run with: pytest -m integration tests/blocks/core/test_variables_integration.py -v
"""

from __future__ import annotations

import uuid

import pytest

from llming_plumber.blocks.base import BlockContext
from llming_plumber.blocks.core.set_variables import SetVariablesBlock, SetVariablesInput
from llming_plumber.blocks.core.variable_store import VariableStore

pytestmark = pytest.mark.integration


def _unique(prefix: str = "") -> str:
    return f"test_{prefix}{uuid.uuid4().hex[:8]}"


@pytest.fixture
async def redis(monkeypatch: pytest.MonkeyPatch):
    """Create a fresh Redis connection and patch get_redis."""
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
    monkeypatch.setattr("llming_plumber.db.get_redis", lambda: r)
    return r


@pytest.fixture
def pipeline_id() -> str:
    return _unique("pl_")


@pytest.fixture
def run_id() -> str:
    return _unique("run_")


# ── VariableStore: pipeline scope ──


async def test_pipeline_set_get_delete(redis, pipeline_id: str, run_id: str) -> None:
    store = VariableStore(redis, pipeline_id, run_id)
    name = f"pl_{_unique()}"

    await store.set(name, 42)
    assert await store.get(name) == 42

    await store.delete(name)
    assert await store.get(name) is None


async def test_pipeline_incr_decr(redis, pipeline_id: str, run_id: str) -> None:
    store = VariableStore(redis, pipeline_id, run_id)
    name = f"pl_{_unique()}"

    result = await store.incr(name, 10)
    assert result == 10.0

    result = await store.incr(name, 5)
    assert result == 15.0

    result = await store.decr(name, 3)
    assert result == 12.0

    await store.delete(name)


async def test_pipeline_incr_float(redis, pipeline_id: str, run_id: str) -> None:
    store = VariableStore(redis, pipeline_id, run_id)
    name = f"pl_{_unique()}"

    result = await store.incr(name, 3.14)
    assert abs(result - 3.14) < 0.001

    result = await store.incr(name, 2.86)
    assert abs(result - 6.0) < 0.001

    await store.delete(name)


async def test_pipeline_append(redis, pipeline_id: str, run_id: str) -> None:
    store = VariableStore(redis, pipeline_id, run_id)
    name = f"pl_{_unique()}"

    result = await store.append(name, "hello")
    assert result == "hello"

    result = await store.append(name, " world")
    assert result == "hello world"

    await store.delete(name)


async def test_pipeline_string_set_get(redis, pipeline_id: str, run_id: str) -> None:
    store = VariableStore(redis, pipeline_id, run_id)
    name = f"pl_{_unique()}"

    await store.set(name, "processing")
    val = await store.get(name)
    assert val == "processing"

    await store.delete(name)


# ── VariableStore: job scope ──


async def test_job_set_get(redis, pipeline_id: str, run_id: str) -> None:
    store = VariableStore(redis, pipeline_id, run_id)
    name = f"job_{_unique()}"

    await store.set(name, "active")
    val = await store.get(name)
    assert val == "active"

    await store.delete(name)


async def test_job_get_vars(redis, pipeline_id: str, run_id: str) -> None:
    store = VariableStore(redis, pipeline_id, run_id)
    short = _unique()
    name = f"job_{short}"

    await store.set(name, "done")
    job_vars = await store.get_job_vars()
    found = any(short in k for k in job_vars)
    assert found, f"Expected {short} in {job_vars}"

    await store.delete(name)


# ── VariableStore: global scope with grants ──


async def test_global_access_denied(redis, pipeline_id: str, run_id: str) -> None:
    store = VariableStore(redis, pipeline_id, run_id)
    var = _unique()

    with pytest.raises(PermissionError, match="not granted"):
        await store.get(f"gl_{var}")


async def test_global_grant_use_revoke(redis, pipeline_id: str, run_id: str) -> None:
    store = VariableStore(redis, pipeline_id, run_id)
    var = _unique()
    gl_name = f"gl_{var}"

    await VariableStore.grant_global_access(redis, var, pipeline_id)

    await store.set(gl_name, 100)
    assert await store.get(gl_name) == 100

    await store.incr(gl_name, 50)
    assert await store.get(gl_name) == 150.0

    await VariableStore.revoke_global_access(redis, var, pipeline_id)

    with pytest.raises(PermissionError):
        await store.get(gl_name)

    # Cleanup
    await VariableStore.grant_global_access(redis, var, pipeline_id)
    await store.delete(gl_name)
    await VariableStore.revoke_global_access(redis, var, pipeline_id)


async def test_global_atomic_incr(redis, pipeline_id: str, run_id: str) -> None:
    store = VariableStore(redis, pipeline_id, run_id)
    var = _unique()
    gl_name = f"gl_{var}"

    await VariableStore.grant_global_access(redis, var, pipeline_id)

    result = await store.incr(gl_name, 1)
    assert result == 1.0
    result = await store.incr(gl_name, 1)
    assert result == 2.0
    result = await store.decr(gl_name, 1)
    assert result == 1.0

    await store.delete(gl_name)
    await VariableStore.revoke_global_access(redis, var, pipeline_id)


async def test_global_append(redis, pipeline_id: str, run_id: str) -> None:
    store = VariableStore(redis, pipeline_id, run_id)
    var = _unique()
    gl_name = f"gl_{var}"

    await VariableStore.grant_global_access(redis, var, pipeline_id)

    result = await store.append(gl_name, "a")
    assert result == "a"
    result = await store.append(gl_name, "b")
    assert result == "ab"

    await store.delete(gl_name)
    await VariableStore.revoke_global_access(redis, var, pipeline_id)


# ── Local scope ──


async def test_local_isolation(redis, pipeline_id: str, run_id: str) -> None:
    store = VariableStore(redis, pipeline_id, run_id)
    await store.set("x", 42)
    assert await store.get("x") == 42
    assert store.get_local_vars() == {"x": 42}


# ── SetVariablesBlock integration ──


async def test_block_local_script(redis) -> None:
    block = SetVariablesBlock()
    result = await block.execute(
        SetVariablesInput(script="a = 1\nb = 2\nc = a + b\nc += 10"),
    )
    assert result.variables["a"] == 1
    assert result.variables["b"] == 2
    assert result.variables["c"] == 13.0
    assert result.operations_run == 4


async def test_block_pipeline_scoped(redis, pipeline_id: str, run_id: str) -> None:
    block = SetVariablesBlock()
    var = _unique()
    ctx = BlockContext(pipeline_id=pipeline_id, run_id=run_id)

    result = await block.execute(
        SetVariablesInput(script=f"pl_{var} = 100\npl_{var} += 50"),
        ctx=ctx,
    )
    assert result.variables[f"pl_{var}"] == 150.0
    assert result.operations_run == 2

    store = VariableStore(redis, pipeline_id, run_id)
    await store.delete(f"pl_{var}")


async def test_block_job_scoped(redis, pipeline_id: str, run_id: str) -> None:
    block = SetVariablesBlock()
    var = _unique()
    ctx = BlockContext(pipeline_id=pipeline_id, run_id=run_id)

    result = await block.execute(
        SetVariablesInput(script=f'job_{var} = "started"'),
        ctx=ctx,
    )
    assert result.variables[f"job_{var}"] == "started"

    store = VariableStore(redis, pipeline_id, run_id)
    await store.delete(f"job_{var}")


async def test_block_mixed_scopes(redis, pipeline_id: str, run_id: str) -> None:
    block = SetVariablesBlock()
    var = _unique()
    ctx = BlockContext(pipeline_id=pipeline_id, run_id=run_id)

    script = f"""local_x = 10
pl_{var} = 20
job_{var} = 30
result = local_x + 1"""
    result = await block.execute(SetVariablesInput(script=script), ctx=ctx)

    assert result.variables["local_x"] == 10
    assert result.variables[f"pl_{var}"] == 20
    assert result.variables[f"job_{var}"] == 30
    assert result.variables["result"] == 11
    assert result.operations_run == 4

    store = VariableStore(redis, pipeline_id, run_id)
    await store.delete(f"pl_{var}")
    await store.delete(f"job_{var}")


async def test_block_string_operations(redis) -> None:
    block = SetVariablesBlock()
    result = await block.execute(
        SetVariablesInput(
            script='name = "Alice"\ngreeting = "Hello, " + name + "!"\nlength = len(name)'
        ),
    )
    assert result.variables["name"] == "Alice"
    assert result.variables["greeting"] == "Hello, Alice!"
    assert result.variables["length"] == 5


async def test_block_arithmetic(redis) -> None:
    block = SetVariablesBlock()
    result = await block.execute(
        SetVariablesInput(
            script="x = 10\ny = 3\nsum_ = x + y\ndiff = x - y\nprod = x * y\nquot = x / y"
        ),
    )
    assert result.variables["sum_"] == 13
    assert result.variables["diff"] == 7
    assert result.variables["prod"] == 30
    assert abs(result.variables["quot"] - 3.333) < 0.01
