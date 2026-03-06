"""Per-run cost tracker with detailed call logging.

Records every LLM call made during a pipeline run, calculates costs
from model pricing, enforces per-run budget limits, and produces a
detailed post-run cost report.

Supports arbitrarily nested/recursive block executions — each call
is tagged with a ``call_path`` (e.g. ``["summarize", "sub_pipeline",
"translate"]``) so the full cost tree can be reconstructed.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from llming_plumber.llm.budget import (
    InsufficientBudgetError,
    LLMBudgetManager,
)
from llming_plumber.llm.providers.llm_provider_models import LLMInfo


@dataclass
class CallRecord:
    """A single LLM API call with full cost breakdown."""

    model: str
    provider: str
    input_tokens: int
    output_tokens: int
    cached_input_tokens: int = 0
    input_cost: float = 0.0
    cached_input_cost: float = 0.0
    output_cost: float = 0.0
    duration_ms: float = 0.0
    block_type: str = ""
    block_id: str = ""
    call_path: list[str] = field(default_factory=list)

    @property
    def total_cost(self) -> float:
        return self.input_cost + self.cached_input_cost + self.output_cost


@dataclass
class RunCostReport:
    """Aggregated cost report for an entire run."""

    run_id: str
    pipeline_id: str
    calls: list[CallRecord]
    budget_limit: float | None = None

    @property
    def total_input_tokens(self) -> int:
        return sum(c.input_tokens for c in self.calls)

    @property
    def total_output_tokens(self) -> int:
        return sum(c.output_tokens for c in self.calls)

    @property
    def total_cached_input_tokens(self) -> int:
        return sum(c.cached_input_tokens for c in self.calls)

    @property
    def total_cost(self) -> float:
        return sum(c.total_cost for c in self.calls)

    @property
    def total_duration_ms(self) -> float:
        return sum(c.duration_ms for c in self.calls)

    @property
    def cost_by_model(self) -> dict[str, float]:
        by_model: dict[str, float] = {}
        for c in self.calls:
            by_model[c.model] = by_model.get(c.model, 0.0) + c.total_cost
        return by_model

    @property
    def cost_by_block(self) -> dict[str, float]:
        by_block: dict[str, float] = {}
        for c in self.calls:
            key = c.block_id or c.block_type
            by_block[key] = by_block.get(key, 0.0) + c.total_cost
        return by_block

    @property
    def call_count(self) -> int:
        return len(self.calls)


def _calc_cost(
    model_info: LLMInfo,
    input_tokens: int,
    output_tokens: int,
    cached_input_tokens: int = 0,
) -> tuple[float, float, float]:
    """Calculate costs from token counts and model pricing.

    Returns (input_cost, cached_input_cost, output_cost).
    Cached input tokens are charged at the cached rate, non-cached
    at the regular rate.
    """
    non_cached = input_tokens - cached_input_tokens
    input_cost = non_cached * (model_info.input_token_price / 1_000_000)
    cached_cost = cached_input_tokens * (
        model_info.cached_input_token_price / 1_000_000
    )
    output_cost = output_tokens * (model_info.output_token_price / 1_000_000)
    return input_cost, cached_cost, output_cost


class CostTracker:
    """Tracks costs for a single pipeline run.

    Usage::

        tracker = CostTracker(
            run_id="run-123",
            pipeline_id="my-pipeline",
            budget_manager=mgr,  # optional, enforces limits
        )

        # Before each LLM call
        await tracker.reserve(model_info, estimated_input=500, max_output=1000)

        # After each LLM call
        await tracker.record(
            model_info=model_info,
            input_tokens=480,
            output_tokens=120,
            cached_input_tokens=200,
            duration_ms=350.0,
            block_type="llm_chat",
            block_id="block-abc",
            call_path=["summarize"],
        )

        # After run completes
        report = tracker.report()
    """

    def __init__(
        self,
        run_id: str = "",
        pipeline_id: str = "",
        budget_manager: LLMBudgetManager | None = None,
        max_run_cost: float | None = None,
    ) -> None:
        self.run_id = run_id
        self.pipeline_id = pipeline_id
        self._budget_manager = budget_manager
        self._max_run_cost = max_run_cost
        self._calls: list[CallRecord] = []
        self._total_cost: float = 0.0

    @property
    def total_cost(self) -> float:
        return self._total_cost

    @property
    def call_count(self) -> int:
        return len(self._calls)

    async def reserve(
        self,
        model_info: LLMInfo,
        estimated_input: int,
        max_output: int,
    ) -> None:
        """Reserve budget before an LLM call.

        Raises ``InsufficientBudgetError`` if per-run or global budget
        would be exceeded.
        """
        input_cost, _, output_cost = _calc_cost(
            model_info, estimated_input, max_output
        )
        estimated_cost = input_cost + output_cost

        # Check per-run limit first (cheap, no I/O)
        if self._max_run_cost is not None:
            if self._total_cost + estimated_cost > self._max_run_cost:
                msg = (
                    f"Run budget exceeded: "
                    f"{self._total_cost + estimated_cost:.6f} > "
                    f"{self._max_run_cost:.6f}"
                )
                raise InsufficientBudgetError(msg, limit_name="run")

        # Check global budget (may hit MongoDB)
        if self._budget_manager is not None:
            await self._budget_manager.reserve_budget_async(
                input_tokens=estimated_input,
                max_output_tokens=max_output,
                input_token_price=model_info.input_token_price,
                output_token_price=model_info.output_token_price,
            )

    async def record(
        self,
        model_info: LLMInfo,
        input_tokens: int,
        output_tokens: int,
        cached_input_tokens: int = 0,
        duration_ms: float = 0.0,
        block_type: str = "",
        block_id: str = "",
        call_path: list[str] | None = None,
        reserved_max_output: int | None = None,
    ) -> CallRecord:
        """Record a completed LLM call and return unused budget.

        Args:
            model_info: The model that was used.
            input_tokens: Actual input tokens consumed.
            output_tokens: Actual output tokens generated.
            cached_input_tokens: Subset of input_tokens served from cache.
            duration_ms: Wall-clock time of the API call.
            block_type: Block type that made the call.
            block_id: Block instance ID within the pipeline.
            call_path: Nesting path for recursive pipelines.
            reserved_max_output: Max output tokens that were reserved
                (pass this so unused budget can be returned).
        """
        input_cost, cached_cost, output_cost = _calc_cost(
            model_info, input_tokens, output_tokens, cached_input_tokens
        )

        record = CallRecord(
            model=model_info.model,
            provider=model_info.provider,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cached_input_tokens=cached_input_tokens,
            input_cost=input_cost,
            cached_input_cost=cached_cost,
            output_cost=output_cost,
            duration_ms=duration_ms,
            block_type=block_type,
            block_id=block_id,
            call_path=call_path or [],
        )
        self._calls.append(record)
        self._total_cost += record.total_cost

        # Return unused budget to global manager
        if (
            self._budget_manager is not None
            and reserved_max_output is not None
        ):
            await self._budget_manager.return_unused_budget_async(
                reserved_output_tokens=reserved_max_output,
                actual_output_tokens=output_tokens,
                output_token_price=model_info.output_token_price,
            )

        return record

    def usage_callback(
        self,
        model_info: LLMInfo,
        block_type: str = "",
        block_id: str = "",
        call_path: list[str] | None = None,
    ):
        """Create a ``usage_callback`` for ``astream()``.

        Returns a callback compatible with the lodge client's
        ``astream(usage_callback=...)`` parameter. Automatically
        records the call when the stream finishes.
        """
        start = time.monotonic()

        def callback(
            input_tokens: int,
            output_tokens: int,
            cached_input_tokens: int = 0,
        ) -> None:
            elapsed = (time.monotonic() - start) * 1000
            input_cost, cached_cost, output_cost = _calc_cost(
                model_info,
                input_tokens,
                output_tokens,
                cached_input_tokens,
            )
            record = CallRecord(
                model=model_info.model,
                provider=model_info.provider,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                cached_input_tokens=cached_input_tokens,
                input_cost=input_cost,
                cached_input_cost=cached_cost,
                output_cost=output_cost,
                duration_ms=elapsed,
                block_type=block_type,
                block_id=block_id,
                call_path=call_path or [],
            )
            self._calls.append(record)
            self._total_cost += record.total_cost

        return callback

    def report(self) -> RunCostReport:
        """Generate the final cost report for this run."""
        return RunCostReport(
            run_id=self.run_id,
            pipeline_id=self.pipeline_id,
            calls=list(self._calls),
            budget_limit=self._max_run_cost,
        )
