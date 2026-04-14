"""Unit tests for PauseController."""

from __future__ import annotations

import asyncio

import pytest

from llming_plumber.worker.pause import PauseController


def test_starts_not_paused() -> None:
    pc = PauseController()
    assert pc.is_paused is False


def test_pause_and_resume() -> None:
    pc = PauseController()
    pc.pause()
    assert pc.is_paused is True
    pc.resume()
    assert pc.is_paused is False


@pytest.mark.asyncio
async def test_wait_if_paused_returns_immediately_when_running() -> None:
    pc = PauseController()
    # Should return instantly
    await asyncio.wait_for(pc.wait_if_paused(), timeout=0.1)


@pytest.mark.asyncio
async def test_wait_if_paused_blocks_when_paused() -> None:
    pc = PauseController()
    pc.pause()

    # Confirm it blocks
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(pc.wait_if_paused(), timeout=0.05)


@pytest.mark.asyncio
async def test_wait_if_paused_unblocks_on_resume() -> None:
    pc = PauseController()
    pc.pause()

    resumed = False

    async def _wait() -> None:
        nonlocal resumed
        await pc.wait_if_paused()
        resumed = True

    task = asyncio.create_task(_wait())
    await asyncio.sleep(0.02)
    assert not resumed

    pc.resume()
    await asyncio.wait_for(task, timeout=0.1)
    assert resumed


@pytest.mark.asyncio
async def test_multiple_waiters_all_unblocked() -> None:
    pc = PauseController()
    pc.pause()

    count = 0

    async def _wait() -> None:
        nonlocal count
        await pc.wait_if_paused()
        count += 1

    tasks = [asyncio.create_task(_wait()) for _ in range(5)]
    await asyncio.sleep(0.02)
    assert count == 0

    pc.resume()
    await asyncio.wait_for(asyncio.gather(*tasks), timeout=0.2)
    assert count == 5


def test_double_pause_idempotent() -> None:
    pc = PauseController()
    pc.pause()
    pc.pause()
    assert pc.is_paused is True


def test_double_resume_idempotent() -> None:
    pc = PauseController()
    pc.pause()
    pc.resume()
    pc.resume()
    assert pc.is_paused is False
