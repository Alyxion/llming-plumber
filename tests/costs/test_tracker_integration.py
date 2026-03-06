"""Integration tests for CostTracker with real LLM calls.

Run with:
    pytest tests/costs/test_tracker_integration.py -m integration -v
"""

from __future__ import annotations

import pytest

from llming_plumber.costs import CostTracker
from llming_plumber.llm.budget import (
    LLMBudgetManager,
    LimitPeriod,
    MemoryBudgetLimit,
)
from llming_plumber.llm.messages import LlmHumanMessage, LlmSystemMessage
from llming_plumber.llm.providers import get_provider
from llming_plumber.llm.providers.llm_provider_models import LLMInfo, ModelSize


def _get_model_and_client(provider_name: str):
    cls = get_provider(provider_name)
    provider = cls()
    if not provider.is_available:
        pytest.skip(f"{provider_name} not available")
    models = provider.get_models()
    small = [m for m in models if m.size == ModelSize.SMALL]
    model = small[0] if small else models[-1]
    temp = model.enforced_temperature if model.enforced_temperature is not None else 0.0
    client = provider.create_client(model=model.model, temperature=temp, max_tokens=256)
    return model, client


def _messages():
    return [
        LlmSystemMessage(content="Be concise."),
        LlmHumanMessage(content="Reply with exactly: ok"),
    ]


@pytest.mark.integration
class TestTrackerFullCycleAnthropic:
    @pytest.mark.asyncio
    async def test_reserve_call_record_report(self) -> None:
        model, client = _get_model_and_client("anthropic")
        tracker = CostTracker(
            run_id="int-test",
            pipeline_id="test-pipe",
            max_run_cost=1.0,
        )

        await tracker.reserve(model, estimated_input=200, max_output=256)
        result = await client.ainvoke(_messages())
        meta = result.response_metadata or {}

        record = await tracker.record(
            model_info=model,
            input_tokens=meta.get("input_tokens", 0),
            output_tokens=meta.get("output_tokens", 0),
            duration_ms=100.0,
            block_type="llm_chat",
            block_id="block-1",
        )

        assert record.input_tokens > 0
        assert record.output_tokens > 0
        assert record.total_cost > 0
        assert record.provider == "anthropic"

        report = tracker.report()
        assert report.call_count == 1
        assert report.total_cost > 0
        assert report.total_cost < 0.01  # tiny prompt
        assert "block-1" in report.cost_by_block
        assert model.model in report.cost_by_model


@pytest.mark.integration
class TestTrackerWithBudgetManager:
    @pytest.mark.asyncio
    async def test_tracks_against_global_budget(self) -> None:
        model, client = _get_model_and_client("mistral")
        mgr = LLMBudgetManager([
            MemoryBudgetLimit(
                name="test", amount=1.0, period=LimitPeriod.TOTAL
            ),
        ])
        tracker = CostTracker(
            run_id="budget-test",
            pipeline_id="test-pipe",
            budget_manager=mgr,
        )

        budget_before = await mgr.available_budget_async()

        await tracker.reserve(model, estimated_input=200, max_output=256)
        result = await client.ainvoke(_messages())
        meta = result.response_metadata or {}

        await tracker.record(
            model_info=model,
            input_tokens=meta.get("input_tokens", 0),
            output_tokens=meta.get("output_tokens", 0),
            reserved_max_output=256,
        )

        budget_after = await mgr.available_budget_async()
        # Budget should have decreased
        assert budget_after < budget_before
        # But should have returned unused output tokens
        assert budget_after > budget_before - 0.01

        report = tracker.report()
        assert report.total_cost > 0


@pytest.mark.integration
class TestTrackerStreamingCallback:
    @pytest.mark.asyncio
    async def test_usage_callback_with_real_stream(self) -> None:
        model, client = _get_model_and_client("anthropic")
        tracker = CostTracker(run_id="stream-test", pipeline_id="p")

        cb = tracker.usage_callback(
            model_info=model,
            block_type="stream_chat",
            block_id="s1",
        )

        async for _chunk in client.astream(_messages(), usage_callback=cb):
            pass

        assert tracker.call_count == 1
        report = tracker.report()
        assert report.total_cost > 0
        assert report.calls[0].block_type == "stream_chat"
        assert report.calls[0].duration_ms > 0
