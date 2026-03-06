from __future__ import annotations

from llming_plumber.blocks.core.filter_block import (
    FilterBlock,
    FilterInput,
    FilterOutput,
)

ITEMS = [
    {"name": "alice", "age": 30, "city": "NYC"},
    {"name": "bob", "age": 25, "city": "LA"},
    {"name": "charlie", "age": 35, "city": "NYC"},
    {"name": "diana", "age": 28, "city": "Chicago"},
]


async def test_eq_operator() -> None:
    block = FilterBlock()
    result = await block.execute(
        FilterInput(items=ITEMS, field="city", operator="eq", value="NYC")
    )
    assert isinstance(result, FilterOutput)
    assert result.filtered_count == 2
    assert result.total_count == 4
    assert all(i["city"] == "NYC" for i in result.items)


async def test_gt_operator_numeric() -> None:
    block = FilterBlock()
    result = await block.execute(
        FilterInput(items=ITEMS, field="age", operator="gt", value="28")
    )
    assert result.filtered_count == 2
    assert {i["name"] for i in result.items} == {"alice", "charlie"}


async def test_contains_operator() -> None:
    block = FilterBlock()
    result = await block.execute(
        FilterInput(items=ITEMS, field="name", operator="contains", value="li")
    )
    assert result.filtered_count == 2
    assert {i["name"] for i in result.items} == {"alice", "charlie"}


async def test_regex_operator() -> None:
    block = FilterBlock()
    result = await block.execute(
        FilterInput(items=ITEMS, field="name", operator="regex", value="^[a-b]")
    )
    assert result.filtered_count == 2
    assert {i["name"] for i in result.items} == {"alice", "bob"}


async def test_exists_and_not_exists() -> None:
    block = FilterBlock()
    items = [{"a": 1}, {"b": 2}, {"a": 3, "b": 4}]
    result = await block.execute(
        FilterInput(items=items, field="a", operator="exists", value="")
    )
    assert result.filtered_count == 2

    result2 = await block.execute(
        FilterInput(items=items, field="a", operator="not_exists", value="")
    )
    assert result2.filtered_count == 1


async def test_ne_operator() -> None:
    block = FilterBlock()
    result = await block.execute(
        FilterInput(items=ITEMS, field="city", operator="ne", value="NYC")
    )
    assert result.filtered_count == 2
    assert {i["name"] for i in result.items} == {"bob", "diana"}


async def test_startswith_endswith() -> None:
    block = FilterBlock()
    result = await block.execute(
        FilterInput(items=ITEMS, field="name", operator="startswith", value="ch")
    )
    assert result.filtered_count == 1
    assert result.items[0]["name"] == "charlie"

    result2 = await block.execute(
        FilterInput(items=ITEMS, field="name", operator="endswith", value="na")
    )
    assert result2.filtered_count == 1
    assert result2.items[0]["name"] == "diana"


async def test_lt_gte_lte_operators() -> None:
    block = FilterBlock()
    result_lt = await block.execute(
        FilterInput(items=ITEMS, field="age", operator="lt", value="28")
    )
    assert result_lt.filtered_count == 1
    assert result_lt.items[0]["name"] == "bob"

    result_gte = await block.execute(
        FilterInput(items=ITEMS, field="age", operator="gte", value="30")
    )
    assert result_gte.filtered_count == 2

    result_lte = await block.execute(
        FilterInput(items=ITEMS, field="age", operator="lte", value="25")
    )
    assert result_lte.filtered_count == 1


async def test_missing_field_returns_false() -> None:
    block = FilterBlock()
    result = await block.execute(
        FilterInput(items=ITEMS, field="missing", operator="eq", value="x")
    )
    assert result.filtered_count == 0


async def test_numeric_comparison_non_numeric_value() -> None:
    block = FilterBlock()
    items = [{"val": "not_a_number"}]
    result = await block.execute(
        FilterInput(items=items, field="val", operator="gt", value="5")
    )
    assert result.filtered_count == 0


async def test_unknown_operator_raises() -> None:
    block = FilterBlock()
    import pytest

    with pytest.raises(ValueError, match="Unknown operator"):
        await block.execute(
            FilterInput(items=ITEMS, field="age", operator="invalid", value="1")
        )


async def test_empty_items() -> None:
    block = FilterBlock()
    result = await block.execute(
        FilterInput(items=[], field="x", operator="eq", value="y")
    )
    assert result.filtered_count == 0
    assert result.total_count == 0
