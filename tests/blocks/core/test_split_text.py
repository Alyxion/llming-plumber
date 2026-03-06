from __future__ import annotations

from llming_plumber.blocks.core.split_text import (
    SplitTextBlock,
    SplitTextInput,
    SplitTextOutput,
)


async def test_split_by_newline() -> None:
    block = SplitTextBlock()
    result = await block.execute(SplitTextInput(text="a\nb\nc"))
    assert isinstance(result, SplitTextOutput)
    assert result.chunks == ["a", "b", "c"]
    assert result.chunk_count == 3


async def test_strip_empty_lines() -> None:
    block = SplitTextBlock()
    result = await block.execute(SplitTextInput(text="a\n\nb\n\n"))
    assert result.chunks == ["a", "b"]
    assert result.chunk_count == 2


async def test_keep_empty_lines() -> None:
    block = SplitTextBlock()
    result = await block.execute(
        SplitTextInput(text="a\n\nb", strip_empty=False)
    )
    assert result.chunks == ["a", "", "b"]
    assert result.chunk_count == 3


async def test_custom_delimiter() -> None:
    block = SplitTextBlock()
    result = await block.execute(
        SplitTextInput(text="one,two,three", delimiter=",")
    )
    assert result.chunks == ["one", "two", "three"]


async def test_empty_text() -> None:
    block = SplitTextBlock()
    result = await block.execute(SplitTextInput(text=""))
    assert result.chunks == []
    assert result.chunk_count == 0
