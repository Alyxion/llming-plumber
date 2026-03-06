from __future__ import annotations

from llming_plumber.blocks.core.deduplicator import (
    DeduplicatorBlock,
    DeduplicatorInput,
    DeduplicatorOutput,
)


async def test_removes_duplicates() -> None:
    block = DeduplicatorBlock()
    items = [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
        {"id": 1, "name": "alice-copy"},
    ]
    result = await block.execute(DeduplicatorInput(items=items, field="id"))
    assert isinstance(result, DeduplicatorOutput)
    assert len(result.items) == 2
    assert result.duplicates_removed == 1


async def test_no_duplicates() -> None:
    block = DeduplicatorBlock()
    items = [{"id": 1}, {"id": 2}, {"id": 3}]
    result = await block.execute(DeduplicatorInput(items=items, field="id"))
    assert len(result.items) == 3
    assert result.duplicates_removed == 0


async def test_keeps_first_occurrence() -> None:
    block = DeduplicatorBlock()
    items = [{"k": "a", "v": 1}, {"k": "a", "v": 2}]
    result = await block.execute(DeduplicatorInput(items=items, field="k"))
    assert result.items[0]["v"] == 1


async def test_empty_list() -> None:
    block = DeduplicatorBlock()
    result = await block.execute(DeduplicatorInput(items=[], field="id"))
    assert result.items == []
    assert result.duplicates_removed == 0


async def test_missing_field_treated_as_none() -> None:
    block = DeduplicatorBlock()
    items = [{"a": 1}, {"b": 2}, {"c": 3}]
    # All have field missing -> all map to str(None), so first kept only
    result = await block.execute(DeduplicatorInput(items=items, field="x"))
    assert len(result.items) == 1
    assert result.duplicates_removed == 2
