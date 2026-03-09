"""Unit tests for the content_diff block."""

from __future__ import annotations

import pytest

from llming_plumber.blocks.web.content_diff import ContentDiffBlock, ContentDiffInput


@pytest.mark.asyncio
async def test_first_run_no_previous() -> None:
    block = ContentDiffBlock()
    result = await block.execute(ContentDiffInput(
        previous="",
        current="Hello world\nNew content here",
        label="test",
    ))
    assert result.has_changes is True
    assert result.change_ratio == 1.0
    assert result.added_lines == 2
    assert "Initial snapshot" in result.summary


@pytest.mark.asyncio
async def test_no_changes() -> None:
    block = ContentDiffBlock()
    text = "Line one\nLine two\nLine three"
    result = await block.execute(ContentDiffInput(
        previous=text, current=text,
    ))
    assert result.has_changes is False
    assert result.change_ratio == 0.0
    assert result.added_lines == 0
    assert result.removed_lines == 0


@pytest.mark.asyncio
async def test_added_lines() -> None:
    block = ContentDiffBlock()
    result = await block.execute(ContentDiffInput(
        previous="Line one\nLine two",
        current="Line one\nLine two\nLine three\nLine four",
    ))
    assert result.has_changes is True
    assert result.added_lines >= 2
    assert "Line three" in result.added_content


@pytest.mark.asyncio
async def test_removed_lines() -> None:
    block = ContentDiffBlock()
    result = await block.execute(ContentDiffInput(
        previous="Line one\nLine two\nLine three",
        current="Line one",
    ))
    assert result.has_changes is True
    assert result.removed_lines >= 2
    assert "Line two" in result.removed_content


@pytest.mark.asyncio
async def test_modified_lines() -> None:
    block = ContentDiffBlock()
    result = await block.execute(ContentDiffInput(
        previous="Product A $100\nProduct B $200",
        current="Product A $100\nProduct B $250",
    ))
    assert result.has_changes is True
    assert result.added_lines >= 1
    assert result.removed_lines >= 1


@pytest.mark.asyncio
async def test_below_threshold() -> None:
    block = ContentDiffBlock()
    # Very minor change
    long_text = "\n".join([f"Line {i}" for i in range(200)])
    modified = long_text.replace("Line 100", "Line 100 modified")
    result = await block.execute(ContentDiffInput(
        previous=long_text,
        current=modified,
        min_change_threshold=0.99,  # Very high threshold
    ))
    assert result.has_changes is False


@pytest.mark.asyncio
async def test_label_in_summary() -> None:
    block = ContentDiffBlock()
    result = await block.execute(ContentDiffInput(
        previous="old", current="new", label="example.com",
    ))
    assert "example.com" in result.summary
    assert result.label == "example.com"


@pytest.mark.asyncio
async def test_diff_text_present() -> None:
    block = ContentDiffBlock()
    result = await block.execute(ContentDiffInput(
        previous="Alpha\nBravo\nCharlie",
        current="Alpha\nBravo\nDelta",
    ))
    assert result.diff_text  # Non-empty
    assert "Charlie" in result.diff_text
    assert "Delta" in result.diff_text
