"""Integration tests for budget tracking with real LLM calls.

Run with:
    pytest tests/llm/test_budget_integration.py -m integration -v

Verifies that real API calls report token usage and that the budget
manager correctly tracks costs.
"""

from __future__ import annotations

import pytest

from llming_plumber.llm.budget import (
    LLMBudgetManager,
    LimitPeriod,
    MemoryBudgetLimit,
    TokenUsage,
)
from llming_plumber.llm.llm_base_client import LlmClient
from llming_plumber.llm.messages import (
    LlmAIMessage,
    LlmHumanMessage,
    LlmSystemMessage,
)
from llming_plumber.llm.providers import get_provider
from llming_plumber.llm.providers.llm_provider_models import LLMInfo, ModelSize

PROMPT = "Reply with exactly: ok"
SYSTEM = "Be concise."


def _get_model_and_client(
    provider_name: str,
) -> tuple[LLMInfo, LlmClient]:
    cls = get_provider(provider_name)
    provider = cls()
    if not provider.is_available:
        pytest.skip(f"{provider_name} not available")

    models = provider.get_models()
    small = [m for m in models if m.size == ModelSize.SMALL]
    model = small[0] if small else models[-1]

    temp = (
        model.enforced_temperature
        if model.enforced_temperature is not None
        else 0.0
    )
    client = provider.create_client(
        model=model.model, temperature=temp, max_tokens=256
    )
    return model, client


def _calc_cost(
    model: LLMInfo, input_tokens: int, output_tokens: int
) -> float:
    input_cost = input_tokens * (model.input_token_price / 1_000_000)
    output_cost = output_tokens * (model.output_token_price / 1_000_000)
    return input_cost + output_cost


def _messages() -> list[LlmSystemMessage | LlmHumanMessage]:
    return [
        LlmSystemMessage(content=SYSTEM),
        LlmHumanMessage(content=PROMPT),
    ]


@pytest.mark.integration
class TestCostTrackingOpenAI:
    def test_invoke_reports_usage(self) -> None:
        model, client = _get_model_and_client("openai")
        result = client.invoke(_messages())
        meta = result.response_metadata or {}
        assert "input_tokens" in meta or "total_tokens" in meta
        input_t = meta.get("input_tokens", 0)
        output_t = meta.get("output_tokens", 0)
        assert input_t > 0 or output_t > 0
        cost = _calc_cost(model, input_t, output_t)
        assert cost > 0
        assert cost < 0.01  # sanity: tiny prompt should cost < 1 cent


@pytest.mark.integration
class TestCostTrackingAnthropic:
    def test_invoke_reports_usage(self) -> None:
        model, client = _get_model_and_client("anthropic")
        result = client.invoke(_messages())
        meta = result.response_metadata or {}
        input_t = meta.get("input_tokens", 0)
        output_t = meta.get("output_tokens", 0)
        assert input_t > 0
        assert output_t > 0
        cost = _calc_cost(model, input_t, output_t)
        assert cost > 0
        assert cost < 0.01


@pytest.mark.integration
class TestCostTrackingGoogle:
    def test_invoke_reports_usage(self) -> None:
        model, client = _get_model_and_client("google")
        result = client.invoke(_messages())
        meta = result.response_metadata or {}
        input_t = meta.get("input_tokens", 0)
        output_t = meta.get("output_tokens", 0)
        assert input_t > 0 or output_t > 0
        cost = _calc_cost(model, input_t, output_t)
        assert cost >= 0


@pytest.mark.integration
class TestCostTrackingMistral:
    def test_invoke_reports_usage(self) -> None:
        model, client = _get_model_and_client("mistral")
        result = client.invoke(_messages())
        meta = result.response_metadata or {}
        input_t = meta.get("input_tokens", 0)
        output_t = meta.get("output_tokens", 0)
        assert input_t > 0
        assert output_t > 0
        cost = _calc_cost(model, input_t, output_t)
        assert cost > 0
        assert cost < 0.01


@pytest.mark.integration
class TestBudgetManagerWithRealCalls:
    @pytest.mark.asyncio
    async def test_budget_reserve_and_track(self) -> None:
        """Full cycle: reserve budget, make real call, return unused."""
        model, client = _get_model_and_client("anthropic")
        mgr = LLMBudgetManager([
            MemoryBudgetLimit(
                name="test", amount=1.0, period=LimitPeriod.TOTAL
            ),
        ])

        # Reserve budget for the call
        await mgr.reserve_budget_async(
            input_tokens=200,
            max_output_tokens=256,
            input_token_price=model.input_token_price,
            output_token_price=model.output_token_price,
        )

        budget_after_reserve = await mgr.available_budget_async()
        assert budget_after_reserve < 1.0

        # Make the real call
        result = await client.ainvoke(_messages())
        meta = result.response_metadata or {}
        actual_output = meta.get("output_tokens", 0)

        # Return unused budget
        await mgr.return_unused_budget_async(
            reserved_output_tokens=256,
            actual_output_tokens=actual_output,
            output_token_price=model.output_token_price,
        )

        budget_after_return = await mgr.available_budget_async()
        # Should have gotten some budget back (we reserved 256 max
        # but likely used far fewer)
        assert budget_after_return > budget_after_reserve
        # But still less than initial since we used some tokens
        assert budget_after_return < 1.0
