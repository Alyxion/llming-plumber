"""Unit tests for the periodic guard block — mocked check blocks."""

from __future__ import annotations

import asyncio
import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from llming_plumber.blocks.base import BlockContext
from llming_plumber.blocks.core.guard import GuardAbortError
from llming_plumber.blocks.core.periodic_guard import (
    PeriodicGuardBlock,
    PeriodicGuardInput,
    PeriodicGuardOutput,
    run_guard_loop,
)
from llming_plumber.worker.pause import PauseController


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_PATCH_CREATE = "llming_plumber.blocks.core.periodic_guard.BlockRegistry.create"
_PATCH_DISCOVER = "llming_plumber.blocks.core.periodic_guard.BlockRegistry.discover"
_PATCH_IO_TYPES = "llming_plumber.worker.executor._get_input_output_types"


def _mock_execute(output: dict[str, Any]) -> AsyncMock:
    mock_output = MagicMock()
    mock_output.model_dump.return_value = output
    return AsyncMock(return_value=mock_output)


def _ctx() -> BlockContext:
    mock_console = AsyncMock()
    mock_console.write = AsyncMock()
    return BlockContext(
        pipeline_id="pl1", run_id="run1", block_id="pg1",
        console=mock_console,
    )


def _patches(mock_block: AsyncMock) -> tuple:
    return (
        patch(_PATCH_CREATE, return_value=mock_block),
        patch(_PATCH_DISCOVER),
        patch(_PATCH_IO_TYPES, return_value=(dict, type(mock_block))),
    )


# ---------------------------------------------------------------------------
# Initial check — pass
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_initial_check_passes() -> None:
    mock_block = AsyncMock()
    mock_block.execute = _mock_execute({"value": 42})

    p1, p2, p3 = _patches(mock_block)
    with p1, p2, p3:
        block = PeriodicGuardBlock()
        result = await block.execute(
            PeriodicGuardInput(
                check_block_type="some_block",
                check_config="{}",
                condition="value > 10",
                interval_seconds=60,
            ),
            ctx=_ctx(),
        )

    assert result.passed is True
    assert result.check_output == {"value": 42}


# ---------------------------------------------------------------------------
# Initial check — abort
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_initial_check_aborts() -> None:
    mock_block = AsyncMock()
    mock_block.execute = _mock_execute({"value": 5})

    p1, p2, p3 = _patches(mock_block)
    with p1, p2, p3:
        block = PeriodicGuardBlock()
        with pytest.raises(GuardAbortError, match="Guard condition failed"):
            await block.execute(
                PeriodicGuardInput(
                    check_block_type="some_block",
                    check_config="{}",
                    condition="value > 10",
                    pause_message="Guard condition failed",
                ),
                ctx=_ctx(),
            )


@pytest.mark.asyncio
async def test_invalid_json_config() -> None:
    block = PeriodicGuardBlock()
    with pytest.raises(ValueError, match="Invalid check_config JSON"):
        await block.execute(
            PeriodicGuardInput(
                check_block_type="x",
                check_config="bad json",
                condition="True",
            ),
        )


@pytest.mark.asyncio
async def test_invalid_condition() -> None:
    mock_block = AsyncMock()
    mock_block.execute = _mock_execute({"x": 1})

    p1, p2, p3 = _patches(mock_block)
    with p1, p2, p3:
        block = PeriodicGuardBlock()
        with pytest.raises(ValueError, match="Guard condition error"):
            await block.execute(
                PeriodicGuardInput(
                    check_block_type="x",
                    check_config="{}",
                    condition="import os",
                ),
                ctx=_ctx(),
            )


# ---------------------------------------------------------------------------
# Block metadata
# ---------------------------------------------------------------------------


def test_metadata() -> None:
    assert PeriodicGuardBlock.block_type == "periodic_guard"
    assert "core/flow" in PeriodicGuardBlock.categories
    assert PeriodicGuardBlock.cache_ttl == 0


# ---------------------------------------------------------------------------
# Guard loop — pause and resume
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_guard_loop_pauses_on_failure() -> None:
    """Guard loop sets pause when check fails."""
    call_count = 0
    pause_ctl = PauseController()
    check_called = asyncio.Event()
    resume_called = asyncio.Event()

    # First call: fails (pause), second call: passes (resume)
    async def _mock_check():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            check_called.set()
            return False, {"status": "bad"}
        resume_called.set()
        return True, {"status": "ok"}

    mock_runs = AsyncMock()
    mock_db = MagicMock()
    mock_db.__getitem__ = MagicMock(return_value=mock_runs)

    with (
        patch(
            "llming_plumber.blocks.core.periodic_guard._run_check",
            return_value=_mock_check,
        ),
        patch(
            "llming_plumber.blocks.core.periodic_guard.GUARD_MIN_INTERVAL_SECONDS",
            0,
        ),
    ):
        task = asyncio.create_task(
            run_guard_loop(
                check_block_type="x",
                check_config={},
                condition="True",
                interval_seconds=0.02,
                pause_message="Paused!",
                max_pause_seconds=9999,
                pause_ctl=pause_ctl,
                guard_block_uid="g1",
                run_id="000000000000000000000001",
                db=mock_db,
            ),
        )
        # Wait for first check to fire (should pause)
        await asyncio.wait_for(check_called.wait(), timeout=1.0)
        await asyncio.sleep(0.01)  # let the loop process the result
        assert pause_ctl.is_paused

        # Wait for second check (should resume)
        await asyncio.wait_for(resume_called.wait(), timeout=1.0)
        await asyncio.sleep(0.01)
        assert not pause_ctl.is_paused

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_guard_loop_aborts_on_max_pause() -> None:
    """Guard loop raises GuardAbortError after max pause time."""
    pause_ctl = PauseController()

    async def _always_fail():
        return False, {}

    mock_db = AsyncMock()
    mock_db.__getitem__ = MagicMock(return_value=mock_db)
    mock_db.update_one = AsyncMock()

    with (
        patch(
            "llming_plumber.blocks.core.periodic_guard._run_check",
            return_value=_always_fail,
        ),
        patch(
            "llming_plumber.blocks.core.periodic_guard.GUARD_MIN_INTERVAL_SECONDS",
            0,
        ),
        patch(
            "llming_plumber.blocks.core.periodic_guard.MAX_PAUSE_SECONDS",
            999999,
        ),
    ):
        task = asyncio.create_task(
            run_guard_loop(
                check_block_type="x",
                check_config={},
                condition="True",
                interval_seconds=0.02,
                pause_message="Paused",
                max_pause_seconds=0,  # immediate abort on second check
                pause_ctl=pause_ctl,
                guard_block_uid="g1",
                run_id="000000000000000000000001",
                db=mock_db,
            ),
        )

        with pytest.raises(GuardAbortError, match="aborting"):
            await asyncio.wait_for(task, timeout=2.0)


# ---------------------------------------------------------------------------
# ctx.check_pause()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ctx_check_pause_no_op_without_controller() -> None:
    ctx = BlockContext(
        pipeline_id="pl1", run_id="run1", block_id="b1",
    )
    # Should return immediately
    await asyncio.wait_for(ctx.check_pause(), timeout=0.1)


@pytest.mark.asyncio
async def test_ctx_check_pause_blocks_when_paused() -> None:
    pc = PauseController()
    pc.pause()
    ctx = BlockContext(
        pipeline_id="pl1", run_id="run1", block_id="b1",
        pause_ctl=pc,
    )
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(ctx.check_pause(), timeout=0.05)


@pytest.mark.asyncio
async def test_ctx_check_pause_unblocks_on_resume() -> None:
    pc = PauseController()
    pc.pause()
    ctx = BlockContext(
        pipeline_id="pl1", run_id="run1", block_id="b1",
        pause_ctl=pc,
    )

    done = False

    async def _wait() -> None:
        nonlocal done
        await ctx.check_pause()
        done = True

    task = asyncio.create_task(_wait())
    await asyncio.sleep(0.02)
    assert not done

    pc.resume()
    await asyncio.wait_for(task, timeout=0.2)
    assert done
