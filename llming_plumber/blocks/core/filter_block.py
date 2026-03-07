"""Filter items by field condition."""

from __future__ import annotations

import re
from typing import Any, ClassVar

from pydantic import Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput
from llming_plumber.blocks.limits import check_list_size


class FilterInput(BlockInput):
    items: list[dict[str, Any]] = Field(
        title="Items",
        description="List of items to filter",
    )
    field: str = Field(
        title="Field",
        description="The field name to evaluate the condition against",
        json_schema_extra={"placeholder": "status"},
    )
    operator: str = Field(
        title="Operator",
        description="Comparison operator for the filter condition",
        json_schema_extra={
            "widget": "select",
            "options": [
                "eq", "ne", "gt", "lt", "gte", "lte",
                "contains", "startswith", "endswith",
                "regex", "exists", "not_exists",
            ],
        },
    )
    value: str = Field(
        title="Value",
        description="The value to compare the field against",
        json_schema_extra={"placeholder": "active"},
    )


class FilterOutput(BlockOutput):
    items: list[dict[str, Any]]
    filtered_count: int
    total_count: int


def _matches(item: dict[str, Any], field: str, operator: str, value: str) -> bool:  # noqa: C901
    if operator == "exists":
        return field in item
    if operator == "not_exists":
        return field not in item

    if field not in item:
        return False

    field_val = item[field]

    if operator == "eq":
        return str(field_val) == value
    if operator == "ne":
        return str(field_val) != value
    if operator == "contains":
        return value in str(field_val)
    if operator == "startswith":
        return str(field_val).startswith(value)
    if operator == "endswith":
        return str(field_val).endswith(value)
    if operator == "regex":
        return re.search(value, str(field_val)) is not None

    # Numeric comparisons
    try:
        num_field = float(field_val)
        num_value = float(value)
    except (ValueError, TypeError):
        return False

    if operator == "gt":
        return num_field > num_value
    if operator == "lt":
        return num_field < num_value
    if operator == "gte":
        return num_field >= num_value
    if operator == "lte":
        return num_field <= num_value

    msg = f"Unknown operator: {operator}"
    raise ValueError(msg)


class FilterBlock(BaseBlock[FilterInput, FilterOutput]):
    block_type: ClassVar[str] = "filter"
    icon: ClassVar[str] = "tabler/filter"
    categories: ClassVar[list[str]] = ["core/flow"]
    description: ClassVar[str] = "Filter items by field condition"
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self, input: FilterInput, ctx: BlockContext | None = None
    ) -> FilterOutput:
        check_list_size(input.items, label="Filter input")
        total = len(input.items)
        filtered = [
            item
            for item in input.items
            if _matches(item, input.field, input.operator, input.value)
        ]
        return FilterOutput(
            items=filtered,
            filtered_count=len(filtered),
            total_count=total,
        )
