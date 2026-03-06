"""Tests for LLM budget / cost tracking (synced from llming-lodge)."""

from __future__ import annotations

import pytest

from llming_plumber.llm.budget import (
    InsufficientBudgetError,
    LLMBudgetManager,
    LimitPeriod,
    MemoryBudgetLimit,
    TokenUsage,
)


# ---------------------------------------------------------------------------
# TokenUsage
# ---------------------------------------------------------------------------

def test_token_usage_totals() -> None:
    usage = TokenUsage(
        input_tokens=1000,
        output_tokens=500,
        input_cost=0.001,
        output_cost=0.005,
    )
    assert usage.total_tokens == 1500
    assert usage.total_cost == pytest.approx(0.006)


# ---------------------------------------------------------------------------
# MemoryBudgetLimit
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_memory_limit_available() -> None:
    limit = MemoryBudgetLimit(
        name="test", amount=10.0, period=LimitPeriod.TOTAL
    )
    available = await limit.get_available_budget_async()
    assert available == 10.0


@pytest.mark.asyncio
async def test_memory_limit_reserve_and_return() -> None:
    limit = MemoryBudgetLimit(
        name="test", amount=1.0, period=LimitPeriod.TOTAL
    )
    success = await limit.reserve_budget_async(0.3)
    assert success is True
    assert await limit.get_available_budget_async() == pytest.approx(0.7)

    await limit.return_budget_async(0.1)
    assert await limit.get_available_budget_async() == pytest.approx(0.8)


@pytest.mark.asyncio
async def test_memory_limit_exceeds() -> None:
    limit = MemoryBudgetLimit(
        name="test", amount=0.5, period=LimitPeriod.TOTAL
    )
    success = await limit.reserve_budget_async(0.6)
    assert success is False
    assert await limit.get_available_budget_async() == pytest.approx(0.5)


@pytest.mark.asyncio
async def test_memory_limit_reset() -> None:
    limit = MemoryBudgetLimit(
        name="test", amount=1.0, period=LimitPeriod.TOTAL
    )
    await limit.reserve_budget_async(0.8)
    await limit.reset_async()
    assert await limit.get_available_budget_async() == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_memory_limit_daily_period() -> None:
    limit = MemoryBudgetLimit(
        name="daily", amount=5.0, period=LimitPeriod.DAILY
    )
    available = await limit.get_available_budget_async()
    assert available == 5.0
    await limit.reserve_budget_async(2.0)
    assert await limit.get_available_budget_async() == pytest.approx(3.0)


# ---------------------------------------------------------------------------
# LLMBudgetManager
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_budget_manager_available() -> None:
    limits = [
        MemoryBudgetLimit(name="a", amount=10.0, period=LimitPeriod.TOTAL),
        MemoryBudgetLimit(name="b", amount=5.0, period=LimitPeriod.TOTAL),
    ]
    mgr = LLMBudgetManager(limits)
    available = await mgr.available_budget_async()
    # Returns the minimum across all limits
    assert available == 5.0


@pytest.mark.asyncio
async def test_budget_manager_reserve() -> None:
    limits = [
        MemoryBudgetLimit(name="total", amount=1.0, period=LimitPeriod.TOTAL),
    ]
    mgr = LLMBudgetManager(limits)
    # Reserve for 1000 input tokens + 500 max output tokens
    # at $2/1M input, $6/1M output
    await mgr.reserve_budget_async(
        input_tokens=1000,
        max_output_tokens=500,
        input_token_price=2.0,
        output_token_price=6.0,
    )
    # Cost: 1000 * 2/1M + 500 * 6/1M = 0.002 + 0.003 = 0.005
    available = await mgr.available_budget_async()
    assert available == pytest.approx(1.0 - 0.005)


@pytest.mark.asyncio
async def test_budget_manager_return_unused() -> None:
    limits = [
        MemoryBudgetLimit(name="total", amount=1.0, period=LimitPeriod.TOTAL),
    ]
    mgr = LLMBudgetManager(limits)
    await mgr.reserve_budget_async(
        input_tokens=1000,
        max_output_tokens=1000,
        input_token_price=2.0,
        output_token_price=6.0,
    )
    # Reserved: 0.002 + 0.006 = 0.008
    # Actually used only 200 output tokens
    await mgr.return_unused_budget_async(
        reserved_output_tokens=1000,
        actual_output_tokens=200,
        output_token_price=6.0,
    )
    # Returned: (1000 - 200) * 6/1M = 0.0048
    available = await mgr.available_budget_async()
    assert available == pytest.approx(1.0 - 0.008 + 0.0048)


@pytest.mark.asyncio
async def test_budget_manager_insufficient() -> None:
    limits = [
        MemoryBudgetLimit(
            name="tiny", amount=0.001, period=LimitPeriod.TOTAL
        ),
    ]
    mgr = LLMBudgetManager(limits)
    with pytest.raises(InsufficientBudgetError, match="tiny"):
        await mgr.reserve_budget_async(
            input_tokens=100_000,
            max_output_tokens=100_000,
            input_token_price=5.0,
            output_token_price=25.0,
        )


@pytest.mark.asyncio
async def test_budget_manager_insufficient_raises_on_second_limit() -> None:
    """If second limit fails, InsufficientBudgetError is raised."""
    limits = [
        MemoryBudgetLimit(name="big", amount=100.0, period=LimitPeriod.TOTAL),
        MemoryBudgetLimit(
            name="small", amount=0.001, period=LimitPeriod.TOTAL
        ),
    ]
    mgr = LLMBudgetManager(limits)
    with pytest.raises(InsufficientBudgetError, match="small"):
        await mgr.reserve_budget_async(
            input_tokens=100_000,
            max_output_tokens=100_000,
            input_token_price=5.0,
            output_token_price=25.0,
        )


@pytest.mark.asyncio
async def test_budget_manager_reset() -> None:
    limits = [
        MemoryBudgetLimit(name="t", amount=1.0, period=LimitPeriod.TOTAL),
    ]
    mgr = LLMBudgetManager(limits)
    await mgr.reserve_budget_async(
        input_tokens=10_000,
        max_output_tokens=10_000,
        input_token_price=5.0,
        output_token_price=25.0,
    )
    await mgr.reset_async()
    assert await mgr.available_budget_async() == pytest.approx(1.0)
