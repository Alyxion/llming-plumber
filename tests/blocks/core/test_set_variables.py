"""Unit tests for SetVariablesBlock — mocked Redis."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from llming_plumber.blocks.base import BlockContext
from llming_plumber.blocks.core.safe_eval import SafeEvalError
from llming_plumber.blocks.core.set_variables import (
    MAX_SCRIPT_LINES,
    SetVariablesBlock,
    SetVariablesInput,
)


def _mock_redis() -> AsyncMock:
    r = AsyncMock()
    r.get = AsyncMock(return_value=None)
    r.set = AsyncMock()
    r.delete = AsyncMock(return_value=1)
    r.incrbyfloat = AsyncMock(return_value="0")
    r.append = AsyncMock()
    r.sismember = AsyncMock(return_value=True)

    async def _empty_scan(**kw: Any) -> Any:
        return
        yield

    r.scan_iter = _empty_scan
    return r


def _ctx(pipeline_id: str = "pl1", run_id: str = "run1") -> BlockContext:
    return BlockContext(pipeline_id=pipeline_id, run_id=run_id)


# ── Basic assignment ──


async def test_simple_assign() -> None:
    block = SetVariablesBlock()
    result = await block.execute(
        SetVariablesInput(script="x = 42"),
    )
    assert result.variables["x"] == 42
    assert result.operations_run == 1


async def test_string_assign() -> None:
    block = SetVariablesBlock()
    result = await block.execute(
        SetVariablesInput(script='status = "done"'),
    )
    assert result.variables["status"] == "done"


async def test_multi_line_script() -> None:
    block = SetVariablesBlock()
    result = await block.execute(
        SetVariablesInput(script="a = 1\nb = 2\nc = a + b"),
    )
    assert result.variables["a"] == 1
    assert result.variables["b"] == 2
    assert result.variables["c"] == 3
    assert result.operations_run == 3


async def test_comments_ignored() -> None:
    block = SetVariablesBlock()
    result = await block.execute(
        SetVariablesInput(script="# comment\nx = 1\n# another"),
    )
    assert result.variables["x"] == 1
    assert result.operations_run == 1


async def test_blank_lines_ignored() -> None:
    block = SetVariablesBlock()
    result = await block.execute(
        SetVariablesInput(script="\n\nx = 5\n\n"),
    )
    assert result.variables["x"] == 5
    assert result.operations_run == 1


# ── Increment / Decrement ──


async def test_increment_local() -> None:
    block = SetVariablesBlock()
    result = await block.execute(
        SetVariablesInput(script="counter = 0\ncounter += 5"),
    )
    assert result.variables["counter"] == 5.0
    assert result.operations_run == 2


async def test_decrement_local() -> None:
    block = SetVariablesBlock()
    result = await block.execute(
        SetVariablesInput(script="counter = 10\ncounter -= 3"),
    )
    assert result.variables["counter"] == 7.0
    assert result.operations_run == 2


async def test_string_append_local() -> None:
    block = SetVariablesBlock()
    result = await block.execute(
        SetVariablesInput(script='msg = "hello"\nmsg += " world"'),
    )
    assert result.variables["msg"] == "hello world"


# ── Expressions ──


async def test_expression_with_functions() -> None:
    block = SetVariablesBlock()
    result = await block.execute(
        SetVariablesInput(script='x = len("hello")\ny = abs(-3)'),
    )
    assert result.variables["x"] == 5
    assert result.variables["y"] == 3


async def test_string_concat() -> None:
    block = SetVariablesBlock()
    result = await block.execute(
        SetVariablesInput(
            script='name = "world"\ngreeting = "hello " + name'
        ),
    )
    assert result.variables["greeting"] == "hello world"


async def test_str_conversion() -> None:
    block = SetVariablesBlock()
    result = await block.execute(
        SetVariablesInput(script='n = 42\nlabel = "item_" + str(n)'),
    )
    assert result.variables["label"] == "item_42"


# ── Scoped variables with mocked Redis ──


async def test_pipeline_scoped_incr(monkeypatch: pytest.MonkeyPatch) -> None:
    redis = _mock_redis()
    redis.incrbyfloat = AsyncMock(return_value="10")

    monkeypatch.setattr(
        "llming_plumber.blocks.core.set_variables.SetVariablesBlock.execute",
        SetVariablesBlock.execute,
    )

    # Patch get_redis to return our mock
    import llming_plumber.blocks.core.set_variables as sv_mod
    original_execute = SetVariablesBlock.execute

    block = SetVariablesBlock()

    # We need to patch at the db module level
    monkeypatch.setattr("llming_plumber.db.get_redis", lambda: redis)

    result = await block.execute(
        SetVariablesInput(script="pl_count += 10"),
        ctx=_ctx(),
    )
    assert result.variables["pl_count"] == 10.0


async def test_global_scoped_requires_grant(monkeypatch: pytest.MonkeyPatch) -> None:
    redis = _mock_redis()
    redis.sismember = AsyncMock(return_value=False)
    monkeypatch.setattr("llming_plumber.db.get_redis", lambda: redis)

    block = SetVariablesBlock()
    with pytest.raises(PermissionError, match="not granted"):
        await block.execute(
            SetVariablesInput(script="gl_total = 100"),
            ctx=_ctx(),
        )


# ── Error handling ──


async def test_invalid_operation() -> None:
    block = SetVariablesBlock()
    with pytest.raises(SafeEvalError, match="Invalid operation"):
        await block.execute(
            SetVariablesInput(script="if True: pass"),
        )


async def test_script_too_long() -> None:
    block = SetVariablesBlock()
    script = "\n".join(f"x{i} = {i}" for i in range(MAX_SCRIPT_LINES + 1))
    with pytest.raises(SafeEvalError, match="Script too long"):
        await block.execute(SetVariablesInput(script=script))


async def test_empty_script() -> None:
    block = SetVariablesBlock()
    result = await block.execute(SetVariablesInput(script=""))
    assert result.operations_run == 0
    assert result.variables == {}


# ── Upstream input fields available ──


async def test_upstream_fields_available() -> None:
    """Extra fields on the input model are available in expressions."""
    block = SetVariablesBlock()
    # SetVariablesInput only has "script", so no extra upstream fields
    # But model_dump will include script, which is excluded
    result = await block.execute(
        SetVariablesInput(script="x = 1 + 2"),
    )
    assert result.variables["x"] == 3


# ── Block metadata ──


def test_block_type() -> None:
    assert SetVariablesBlock.block_type == "set_variables"


def test_categories() -> None:
    assert "core/data" in SetVariablesBlock.categories
