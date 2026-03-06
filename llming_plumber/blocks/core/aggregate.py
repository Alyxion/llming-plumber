"""Compute statistics across items."""

from __future__ import annotations

from typing import Any, ClassVar

from pydantic import Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput


class AggregateInput(BlockInput):
    items: list[dict[str, Any]] = Field(
        title="Items",
        description="List of items to aggregate",
    )
    field: str = Field(
        title="Field",
        description="The numeric field to compute statistics on",
        json_schema_extra={"placeholder": "score"},
    )
    operation: str = Field(
        title="Operation",
        description="The aggregation operation to perform",
        json_schema_extra={
            "widget": "select",
            "options": ["sum", "avg", "min", "max", "count"],
        },
    )


class AggregateOutput(BlockOutput):
    result: float
    operation: str
    field: str


class AggregateBlock(BaseBlock[AggregateInput, AggregateOutput]):
    block_type: ClassVar[str] = "aggregate"
    icon: ClassVar[str] = "tabler/math-function"
    categories: ClassVar[list[str]] = ["core/transform"]
    description: ClassVar[str] = (
        "Compute statistics across items (sum, avg, min, max, count)"
    )
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self, input: AggregateInput, ctx: BlockContext | None = None
    ) -> AggregateOutput:
        values = [
            float(item[input.field])
            for item in input.items
            if input.field in item
        ]
        op = input.operation

        if op == "count":
            result = float(len(values))
        elif op == "sum":
            result = sum(values)
        elif op == "avg":
            if not values:
                msg = "Cannot compute avg on empty set"
                raise ValueError(msg)
            result = sum(values) / len(values)
        elif op == "min":
            if not values:
                msg = "Cannot compute min on empty set"
                raise ValueError(msg)
            result = min(values)
        elif op == "max":
            if not values:
                msg = "Cannot compute max on empty set"
                raise ValueError(msg)
            result = max(values)
        else:
            msg = f"Unknown operation: {op}"
            raise ValueError(msg)

        return AggregateOutput(
            result=result,
            operation=input.operation,
            field=input.field,
        )
