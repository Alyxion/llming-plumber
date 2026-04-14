"""Unit tests for the guard block — mocked check blocks."""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from llming_plumber.blocks.base import BlockContext
from llming_plumber.blocks.core.guard import (
    GuardAbortError,
    GuardBlock,
    GuardInput,
    GuardOutput,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Mock output from ip_geolocation block
MOCK_IP_OUTPUT = {
    "results": [
        {
            "query": "203.0.113.50",
            "status": "success",
            "country": "Germany",
            "country_code": "DE",
            "city": "Stuttgart",
            "lat": 48.78,
            "lon": 9.18,
            "isp": "Example ISP",
            "hosting": False,
            "proxy": False,
            "mobile": False,
        },
    ],
    "total": 1,
}

MOCK_OFFICE_IP_OUTPUT = {
    "results": [
        {
            "query": "88.79.198.243",
            "status": "success",
            "country": "Germany",
            "country_code": "DE",
            "city": "Metzingen",
            "lat": 48.53,
            "lon": 9.28,
            "isp": "Office ISP",
            "hosting": False,
            "proxy": False,
            "mobile": False,
        },
    ],
    "total": 1,
}

_PATCH_CREATE = "llming_plumber.blocks.core.guard.BlockRegistry.create"
_PATCH_DISCOVER = "llming_plumber.blocks.core.guard.BlockRegistry.discover"
_PATCH_IO_TYPES = (
    "llming_plumber.worker.executor._get_input_output_types"
)


def _mock_execute(output: dict[str, Any]) -> AsyncMock:
    """Create a mock block.execute that returns a mock output model."""
    mock_output = MagicMock()
    mock_output.model_dump.return_value = output
    return AsyncMock(return_value=mock_output)


def _input(
    *,
    condition: str = 'results[0]["query"] != "88.79.198.243"',
    check_config: str = "{}",
    abort_message: str = "Guard check failed — pipeline aborted.",
) -> GuardInput:
    return GuardInput(
        check_block_type="ip_geolocation",
        check_config=check_config,
        condition=condition,
        abort_message=abort_message,
    )


def _ctx() -> BlockContext:
    mock_console = AsyncMock()
    mock_console.write = AsyncMock()
    return BlockContext(
        pipeline_id="pl1", run_id="run1", block_id="guard1",
        console=mock_console,
    )


def _guard_patches(mock_block: AsyncMock):
    """Context manager stacking all three patches needed for guard tests."""
    return (
        patch(_PATCH_CREATE, return_value=mock_block),
        patch(_PATCH_DISCOVER),
        patch(_PATCH_IO_TYPES, return_value=(dict, type(mock_block))),
    )


# ---------------------------------------------------------------------------
# Pass scenarios
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_guard_passes_when_ip_differs() -> None:
    """Guard passes when current IP is NOT the office IP."""
    mock_block = AsyncMock()
    mock_block.execute = _mock_execute(MOCK_IP_OUTPUT)

    p_create, p_discover, p_io = _guard_patches(mock_block)
    with p_create, p_discover, p_io:
        block = GuardBlock()
        result = await block.execute(_input(), ctx=_ctx())

    assert result.passed is True
    assert result.check_output == MOCK_IP_OUTPUT


@pytest.mark.asyncio
async def test_guard_passes_simple_condition() -> None:
    """Guard with a simple numeric condition."""
    mock_block = AsyncMock()
    mock_block.execute = _mock_execute({"value": 42, "status": "ok"})

    p_create, p_discover, p_io = _guard_patches(mock_block)
    with p_create, p_discover, p_io:
        block = GuardBlock()
        result = await block.execute(
            GuardInput(
                check_block_type="some_block",
                check_config="{}",
                condition="value > 10",
                abort_message="Value too low",
            ),
            ctx=_ctx(),
        )

    assert result.passed is True


# ---------------------------------------------------------------------------
# Abort scenarios
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_guard_aborts_when_ip_matches() -> None:
    """Guard aborts when current IP matches the forbidden office IP."""
    mock_block = AsyncMock()
    mock_block.execute = _mock_execute(MOCK_OFFICE_IP_OUTPUT)

    p_create, p_discover, p_io = _guard_patches(mock_block)
    with p_create, p_discover, p_io:
        block = GuardBlock()
        with pytest.raises(GuardAbortError, match="Guard check failed"):
            await block.execute(_input(), ctx=_ctx())


@pytest.mark.asyncio
async def test_guard_aborts_with_custom_message() -> None:
    """Abort message can include placeholders from check output."""
    mock_block = AsyncMock()
    mock_block.execute = _mock_execute({"total": 5, "status": "bad"})

    p_create, p_discover, p_io = _guard_patches(mock_block)
    with p_create, p_discover, p_io:
        block = GuardBlock()
        with pytest.raises(GuardAbortError, match="Total was 5"):
            await block.execute(
                GuardInput(
                    check_block_type="x",
                    check_config="{}",
                    condition="status == 'good'",
                    abort_message="Total was {total}",
                ),
                ctx=_ctx(),
            )


@pytest.mark.asyncio
async def test_guard_abort_with_falsy_condition() -> None:
    """Condition returning 0 / empty string / empty list triggers abort."""
    mock_block = AsyncMock()
    mock_block.execute = _mock_execute({"count": 0})

    p_create, p_discover, p_io = _guard_patches(mock_block)
    with p_create, p_discover, p_io:
        block = GuardBlock()
        with pytest.raises(GuardAbortError):
            await block.execute(
                GuardInput(
                    check_block_type="x",
                    check_config="{}",
                    condition="count",
                    abort_message="No items",
                ),
                ctx=_ctx(),
            )


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_invalid_check_config_json() -> None:
    """Bad JSON in check_config raises ValueError."""
    block = GuardBlock()
    with pytest.raises(ValueError, match="Invalid check_config JSON"):
        await block.execute(
            GuardInput(
                check_block_type="ip_geolocation",
                check_config="not json",
                condition="True",
                abort_message="x",
            )
        )


@pytest.mark.asyncio
async def test_invalid_condition_expression() -> None:
    """Syntactically invalid condition raises ValueError."""
    mock_block = AsyncMock()
    mock_block.execute = _mock_execute({"x": 1})

    p_create, p_discover, p_io = _guard_patches(mock_block)
    with p_create, p_discover, p_io:
        block = GuardBlock()
        with pytest.raises(ValueError, match="Guard condition error"):
            await block.execute(
                GuardInput(
                    check_block_type="x",
                    check_config="{}",
                    condition="import os",
                    abort_message="x",
                ),
                ctx=_ctx(),
            )


@pytest.mark.asyncio
async def test_unknown_variable_in_condition() -> None:
    """Referencing a field that doesn't exist in check output."""
    mock_block = AsyncMock()
    mock_block.execute = _mock_execute({"x": 1})

    p_create, p_discover, p_io = _guard_patches(mock_block)
    with p_create, p_discover, p_io:
        block = GuardBlock()
        with pytest.raises(ValueError, match="Guard condition error"):
            await block.execute(
                GuardInput(
                    check_block_type="x",
                    check_config="{}",
                    condition="nonexistent_field == 1",
                    abort_message="x",
                ),
                ctx=_ctx(),
            )


# ---------------------------------------------------------------------------
# Check config passing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_check_config_passed_to_block() -> None:
    """JSON check_config is parsed and passed to the check block."""
    mock_block = AsyncMock()
    mock_block.execute = _mock_execute({"ok": True})
    captured_input: list[Any] = []

    original_execute = mock_block.execute

    async def _capture_execute(inp: Any, **kwargs: Any) -> Any:
        captured_input.append(inp)
        return await original_execute(inp, **kwargs)

    mock_block.execute = _capture_execute

    p_create, p_discover, p_io = _guard_patches(mock_block)
    with p_create, p_discover, p_io:
        block = GuardBlock()
        await block.execute(
            GuardInput(
                check_block_type="ip_geolocation",
                check_config='{"ip_addresses": ["8.8.8.8"]}',
                condition="ok",
                abort_message="failed",
            ),
            ctx=_ctx(),
        )

    # dict was called with the parsed config
    assert len(captured_input) == 1
    assert captured_input[0] == {"ip_addresses": ["8.8.8.8"]}


# ---------------------------------------------------------------------------
# Context logging
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_guard_logs_on_pass() -> None:
    mock_block = AsyncMock()
    mock_block.execute = _mock_execute({"x": 1})

    p_create, p_discover, p_io = _guard_patches(mock_block)
    with p_create, p_discover, p_io:
        ctx = _ctx()
        block = GuardBlock()
        await block.execute(
            GuardInput(
                check_block_type="x",
                check_config="{}",
                condition="x == 1",
                abort_message="",
            ),
            ctx=ctx,
        )

    messages = [str(c.args[1]) for c in ctx.console.write.call_args_list]
    assert any("passed" in m.lower() for m in messages)


@pytest.mark.asyncio
async def test_guard_logs_on_abort() -> None:
    mock_block = AsyncMock()
    mock_block.execute = _mock_execute({"x": 1})

    p_create, p_discover, p_io = _guard_patches(mock_block)
    with p_create, p_discover, p_io:
        ctx = _ctx()
        block = GuardBlock()
        with pytest.raises(GuardAbortError):
            await block.execute(
                GuardInput(
                    check_block_type="x",
                    check_config="{}",
                    condition="x == 999",
                    abort_message="Nope",
                ),
                ctx=ctx,
            )

    messages = [str(c.args[1]) for c in ctx.console.write.call_args_list]
    assert any("Nope" in m for m in messages)


# ---------------------------------------------------------------------------
# Standalone (ctx=None)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_guard_works_without_ctx() -> None:
    mock_block = AsyncMock()
    mock_block.execute = _mock_execute({"x": 42})

    p_create, p_discover, p_io = _guard_patches(mock_block)
    with p_create, p_discover, p_io:
        block = GuardBlock()
        result = await block.execute(
            GuardInput(
                check_block_type="x",
                check_config="{}",
                condition="x > 10",
                abort_message="",
            ),
            ctx=None,
        )
    assert result.passed is True


# ---------------------------------------------------------------------------
# Block metadata
# ---------------------------------------------------------------------------


def test_block_metadata() -> None:
    assert GuardBlock.block_type == "guard"
    assert "core/flow" in GuardBlock.categories
    assert GuardBlock.cache_ttl == 0
    assert GuardBlock.icon == "tabler/shield-check"


def test_guard_abort_error_is_runtime_error() -> None:
    """GuardAbortError should be catchable as RuntimeError."""
    err = GuardAbortError("test")
    assert isinstance(err, RuntimeError)
    assert str(err) == "test"
