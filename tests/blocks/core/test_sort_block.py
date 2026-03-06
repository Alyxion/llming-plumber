from __future__ import annotations

from llming_plumber.blocks.core.sort_block import SortBlock, SortInput, SortOutput

ITEMS = [
    {"name": "charlie", "age": 35},
    {"name": "alice", "age": 30},
    {"name": "bob", "age": 25},
]


async def test_sort_ascending_by_name() -> None:
    block = SortBlock()
    result = await block.execute(SortInput(items=ITEMS, field="name"))
    assert isinstance(result, SortOutput)
    assert [i["name"] for i in result.items] == ["alice", "bob", "charlie"]


async def test_sort_descending() -> None:
    block = SortBlock()
    result = await block.execute(
        SortInput(items=ITEMS, field="name", descending=True)
    )
    assert [i["name"] for i in result.items] == ["charlie", "bob", "alice"]


async def test_sort_by_numeric_field() -> None:
    block = SortBlock()
    result = await block.execute(SortInput(items=ITEMS, field="age"))
    assert [i["age"] for i in result.items] == [25, 30, 35]


async def test_sort_empty_list() -> None:
    block = SortBlock()
    result = await block.execute(SortInput(items=[], field="name"))
    assert result.items == []


async def test_sort_missing_field_uses_default() -> None:
    block = SortBlock()
    items = [{"name": "b"}, {"x": 1}, {"name": "a"}]
    result = await block.execute(SortInput(items=items, field="name"))
    # Items missing the field get "" as sort key
    assert result.items[0] == {"x": 1}
