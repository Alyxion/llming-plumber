"""Unit tests for CostTracker — no real API calls."""

from __future__ import annotations

import pytest

from llming_plumber.costs import CallRecord, CostTracker, RunCostReport
from llming_plumber.llm.budget import (
    InsufficientBudgetError,
    LLMBudgetManager,
    LimitPeriod,
    MemoryBudgetLimit,
)
from llming_plumber.llm.providers.llm_provider_models import LLMInfo, ModelSize


def _model(
    price_in: float = 1.0,
    price_out: float = 5.0,
    price_cached: float = 0.1,
    provider: str = "test",
    model: str = "test-model",
) -> LLMInfo:
    return LLMInfo(
        provider=provider,
        name="test",
        label="Test",
        model=model,
        description="test",
        input_token_price=price_in,
        cached_input_token_price=price_cached,
        output_token_price=price_out,
    )


# ---------------------------------------------------------------------------
# CallRecord
# ---------------------------------------------------------------------------


def test_call_record_total_cost() -> None:
    r = CallRecord(
        model="m",
        provider="p",
        input_tokens=1000,
        output_tokens=500,
        input_cost=0.001,
        cached_input_cost=0.0002,
        output_cost=0.0025,
    )
    assert r.total_cost == pytest.approx(0.0037)


# ---------------------------------------------------------------------------
# RunCostReport
# ---------------------------------------------------------------------------


def test_report_aggregations() -> None:
    calls = [
        CallRecord(
            model="a", provider="p", input_tokens=100, output_tokens=50,
            input_cost=0.001, output_cost=0.002, duration_ms=100,
            block_type="chat", block_id="b1",
        ),
        CallRecord(
            model="a", provider="p", input_tokens=200, output_tokens=80,
            input_cost=0.002, output_cost=0.004, duration_ms=150,
            block_type="chat", block_id="b1",
        ),
        CallRecord(
            model="b", provider="q", input_tokens=50, output_tokens=20,
            cached_input_tokens=30, cached_input_cost=0.0001,
            input_cost=0.0005, output_cost=0.001, duration_ms=50,
            block_type="summarize", block_id="b2",
        ),
    ]
    report = RunCostReport(
        run_id="r1", pipeline_id="p1", calls=calls, budget_limit=1.0
    )
    assert report.total_input_tokens == 350
    assert report.total_output_tokens == 150
    assert report.total_cached_input_tokens == 30
    assert report.call_count == 3
    # 0.001+0.002 + 0.002+0.004 + 0.0005+0.0001+0.001 = 0.0106
    assert report.total_cost == pytest.approx(0.0106)
    assert report.total_duration_ms == pytest.approx(300.0)

    by_model = report.cost_by_model
    assert "a" in by_model
    assert "b" in by_model
    assert by_model["a"] == pytest.approx(0.009)

    by_block = report.cost_by_block
    assert "b1" in by_block
    assert "b2" in by_block


# ---------------------------------------------------------------------------
# CostTracker — basic recording
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tracker_record_no_budget() -> None:
    tracker = CostTracker(run_id="r1", pipeline_id="p1")
    m = _model(price_in=2.0, price_out=10.0, price_cached=0.2)

    record = await tracker.record(
        model_info=m,
        input_tokens=1000,
        output_tokens=200,
        cached_input_tokens=500,
        duration_ms=250.0,
        block_type="chat",
        block_id="block-1",
        call_path=["outer", "inner"],
    )
    assert record.model == "test-model"
    assert record.provider == "test"
    # Non-cached: 500 * 2.0/1M = 0.001
    assert record.input_cost == pytest.approx(0.001)
    # Cached: 500 * 0.2/1M = 0.0001
    assert record.cached_input_cost == pytest.approx(0.0001)
    # Output: 200 * 10.0/1M = 0.002
    assert record.output_cost == pytest.approx(0.002)
    assert record.total_cost == pytest.approx(0.0031)
    assert record.call_path == ["outer", "inner"]

    assert tracker.total_cost == pytest.approx(0.0031)
    assert tracker.call_count == 1

    report = tracker.report()
    assert report.run_id == "r1"
    assert report.call_count == 1
    assert report.total_cost == pytest.approx(0.0031)


@pytest.mark.asyncio
async def test_tracker_multiple_records() -> None:
    tracker = CostTracker()
    m = _model()
    await tracker.record(m, input_tokens=100, output_tokens=50)
    await tracker.record(m, input_tokens=200, output_tokens=100)
    assert tracker.call_count == 2
    assert tracker.total_cost > 0


# ---------------------------------------------------------------------------
# CostTracker — per-run budget limit
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tracker_run_budget_enforced() -> None:
    tracker = CostTracker(max_run_cost=0.001)
    m = _model(price_in=5.0, price_out=25.0)

    # This should exceed the 0.001 run limit
    with pytest.raises(InsufficientBudgetError, match="Run budget"):
        await tracker.reserve(
            model_info=m, estimated_input=1000, max_output=1000
        )


@pytest.mark.asyncio
async def test_tracker_run_budget_ok_then_exceeded() -> None:
    tracker = CostTracker(max_run_cost=0.01)
    m = _model(price_in=2.0, price_out=10.0)

    # First call fits
    await tracker.reserve(m, estimated_input=100, max_output=100)
    await tracker.record(m, input_tokens=100, output_tokens=50)

    # Second call still fits
    await tracker.reserve(m, estimated_input=100, max_output=100)
    await tracker.record(m, input_tokens=100, output_tokens=50)

    # Eventually exceeds
    with pytest.raises(InsufficientBudgetError, match="Run budget"):
        await tracker.reserve(m, estimated_input=100_000, max_output=100_000)


# ---------------------------------------------------------------------------
# CostTracker — global budget manager integration
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tracker_with_budget_manager() -> None:
    mgr = LLMBudgetManager([
        MemoryBudgetLimit(name="total", amount=1.0, period=LimitPeriod.TOTAL),
    ])
    tracker = CostTracker(budget_manager=mgr)
    m = _model(price_in=2.0, price_out=10.0)

    # Reserve and record
    await tracker.reserve(m, estimated_input=500, max_output=500)
    await tracker.record(
        m,
        input_tokens=500,
        output_tokens=100,
        reserved_max_output=500,
    )

    # Budget should have been partially returned
    available = await mgr.available_budget_async()
    # Reserved: 500*2/1M + 500*10/1M = 0.001 + 0.005 = 0.006
    # Returned: (500-100)*10/1M = 0.004
    # Net used: 0.006 - 0.004 = 0.002
    assert available == pytest.approx(1.0 - 0.002)


@pytest.mark.asyncio
async def test_tracker_global_budget_exceeded() -> None:
    mgr = LLMBudgetManager([
        MemoryBudgetLimit(
            name="tight", amount=0.0001, period=LimitPeriod.TOTAL
        ),
    ])
    tracker = CostTracker(budget_manager=mgr)
    m = _model(price_in=5.0, price_out=25.0)

    with pytest.raises(InsufficientBudgetError):
        await tracker.reserve(m, estimated_input=10_000, max_output=10_000)


# ---------------------------------------------------------------------------
# CostTracker — usage_callback for streaming
# ---------------------------------------------------------------------------


def test_usage_callback() -> None:
    tracker = CostTracker()
    m = _model(price_in=2.0, price_out=10.0, price_cached=0.2)

    cb = tracker.usage_callback(
        model_info=m,
        block_type="stream_chat",
        block_id="b1",
        call_path=["root"],
    )
    # Simulate stream completion
    cb(input_tokens=1000, output_tokens=200, cached_input_tokens=400)

    assert tracker.call_count == 1
    record = tracker.report().calls[0]
    assert record.block_type == "stream_chat"
    assert record.call_path == ["root"]
    assert record.input_tokens == 1000
    assert record.output_tokens == 200
    assert record.cached_input_tokens == 400
    assert record.duration_ms > 0  # measured real elapsed time
    assert record.total_cost > 0


# ---------------------------------------------------------------------------
# CostTracker — nested/recursive call paths
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_nested_call_paths() -> None:
    tracker = CostTracker(run_id="r1", pipeline_id="p1")
    m = _model()

    await tracker.record(
        m, input_tokens=100, output_tokens=50,
        block_type="outer", call_path=["outer"],
    )
    await tracker.record(
        m, input_tokens=100, output_tokens=50,
        block_type="inner", call_path=["outer", "sub_pipeline", "inner"],
    )
    await tracker.record(
        m, input_tokens=100, output_tokens=50,
        block_type="deep",
        call_path=["outer", "sub_pipeline", "inner", "recursive"],
    )

    report = tracker.report()
    assert report.call_count == 3
    paths = [c.call_path for c in report.calls]
    assert ["outer"] in paths
    assert ["outer", "sub_pipeline", "inner"] in paths
    assert ["outer", "sub_pipeline", "inner", "recursive"] in paths


# ---------------------------------------------------------------------------
# CostTracker — empty report
# ---------------------------------------------------------------------------


def test_empty_report() -> None:
    tracker = CostTracker(run_id="empty", pipeline_id="p")
    report = tracker.report()
    assert report.call_count == 0
    assert report.total_cost == 0.0
    assert report.total_input_tokens == 0
    assert report.total_output_tokens == 0
    assert report.cost_by_model == {}
    assert report.cost_by_block == {}
