from __future__ import annotations

from llming_plumber.blocks.core.merge import MergeBlock, MergeInput, MergeOutput


async def test_merge_two_lists() -> None:
    block = MergeBlock()
    result = await block.execute(
        MergeInput(item_lists=[[{"a": 1}], [{"b": 2}]])
    )
    assert isinstance(result, MergeOutput)
    assert result.items == [{"a": 1}, {"b": 2}]
    assert result.source_count == 2


async def test_merge_three_lists() -> None:
    block = MergeBlock()
    result = await block.execute(
        MergeInput(item_lists=[[{"a": 1}], [{"b": 2}], [{"c": 3}]])
    )
    assert len(result.items) == 3
    assert result.source_count == 3


async def test_merge_empty_lists() -> None:
    block = MergeBlock()
    result = await block.execute(MergeInput(item_lists=[[], []]))
    assert result.items == []
    assert result.source_count == 2


async def test_merge_no_lists() -> None:
    block = MergeBlock()
    result = await block.execute(MergeInput(item_lists=[]))
    assert result.items == []
    assert result.source_count == 0


async def test_preserves_order() -> None:
    block = MergeBlock()
    result = await block.execute(
        MergeInput(
            item_lists=[[{"v": 1}, {"v": 2}], [{"v": 3}], [{"v": 4}, {"v": 5}]]
        )
    )
    assert [i["v"] for i in result.items] == [1, 2, 3, 4, 5]
