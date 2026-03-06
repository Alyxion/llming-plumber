from __future__ import annotations

import pytest

from llming_plumber.blocks.core.aggregate import (
    AggregateBlock,
    AggregateInput,
    AggregateOutput,
)

ITEMS = [
    {"name": "a", "score": 10},
    {"name": "b", "score": 20},
    {"name": "c", "score": 30},
]


async def test_sum() -> None:
    block = AggregateBlock()
    result = await block.execute(
        AggregateInput(items=ITEMS, field="score", operation="sum")
    )
    assert isinstance(result, AggregateOutput)
    assert result.result == 60.0
    assert result.operation == "sum"
    assert result.field == "score"


async def test_avg() -> None:
    block = AggregateBlock()
    result = await block.execute(
        AggregateInput(items=ITEMS, field="score", operation="avg")
    )
    assert result.result == 20.0


async def test_min_max() -> None:
    block = AggregateBlock()
    r_min = await block.execute(
        AggregateInput(items=ITEMS, field="score", operation="min")
    )
    r_max = await block.execute(
        AggregateInput(items=ITEMS, field="score", operation="max")
    )
    assert r_min.result == 10.0
    assert r_max.result == 30.0


async def test_count() -> None:
    block = AggregateBlock()
    result = await block.execute(
        AggregateInput(items=ITEMS, field="score", operation="count")
    )
    assert result.result == 3.0


async def test_avg_empty_raises() -> None:
    block = AggregateBlock()
    with pytest.raises(ValueError, match="Cannot compute avg"):
        await block.execute(
            AggregateInput(items=[], field="score", operation="avg")
        )


async def test_min_empty_raises() -> None:
    block = AggregateBlock()
    with pytest.raises(ValueError, match="Cannot compute min"):
        await block.execute(
            AggregateInput(items=[], field="score", operation="min")
        )


async def test_max_empty_raises() -> None:
    block = AggregateBlock()
    with pytest.raises(ValueError, match="Cannot compute max"):
        await block.execute(
            AggregateInput(items=[], field="score", operation="max")
        )


async def test_unknown_operation_raises() -> None:
    block = AggregateBlock()
    with pytest.raises(ValueError, match="Unknown operation"):
        await block.execute(
            AggregateInput(items=ITEMS, field="score", operation="median")
        )
