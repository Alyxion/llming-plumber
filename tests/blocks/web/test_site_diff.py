"""Unit tests for the site_diff block."""

from __future__ import annotations

import pytest

from llming_plumber.blocks.web.site_diff import SiteDiffBlock, SiteDiffInput


@pytest.mark.asyncio
async def test_no_changes() -> None:
    pages = [
        {"url": "https://x.com/", "title": "Home", "text": "Hello", "content_hash": "abc"},
        {"url": "https://x.com/about", "title": "About", "text": "About us", "content_hash": "def"},
    ]
    block = SiteDiffBlock()
    result = await block.execute(SiteDiffInput(
        previous_pages=pages, current_pages=pages,
    ))
    assert result.has_changes is False
    assert result.unchanged_count == 2
    assert result.new_count == 0
    assert result.removed_count == 0
    assert result.modified_count == 0


@pytest.mark.asyncio
async def test_new_page() -> None:
    prev = [{"url": "https://x.com/", "title": "Home", "text": "Hello", "content_hash": "abc"}]
    curr = prev + [{"url": "https://x.com/new", "title": "New Page", "text": "New!", "content_hash": "xyz"}]
    block = SiteDiffBlock()
    result = await block.execute(SiteDiffInput(
        previous_pages=prev, current_pages=curr,
    ))
    assert result.has_changes is True
    assert result.new_count == 1
    assert result.new_pages[0]["url"] == "https://x.com/new"
    assert result.new_pages[0]["change_type"] == "new"


@pytest.mark.asyncio
async def test_removed_page() -> None:
    prev = [
        {"url": "https://x.com/", "title": "Home", "text": "Hello", "content_hash": "abc"},
        {"url": "https://x.com/old", "title": "Old", "text": "Old page", "content_hash": "old"},
    ]
    curr = [{"url": "https://x.com/", "title": "Home", "text": "Hello", "content_hash": "abc"}]
    block = SiteDiffBlock()
    result = await block.execute(SiteDiffInput(
        previous_pages=prev, current_pages=curr,
    ))
    assert result.has_changes is True
    assert result.removed_count == 1
    assert result.removed_pages[0]["url"] == "https://x.com/old"


@pytest.mark.asyncio
async def test_modified_page() -> None:
    prev = [{"url": "https://x.com/", "title": "Home", "text": "Old content here\nMore old stuff", "content_hash": "abc"}]
    curr = [{"url": "https://x.com/", "title": "Home", "text": "New content here\nMore new stuff", "content_hash": "xyz"}]
    block = SiteDiffBlock()
    result = await block.execute(SiteDiffInput(
        previous_pages=prev, current_pages=curr,
    ))
    assert result.has_changes is True
    assert result.modified_count == 1
    assert result.modified_pages[0]["change_type"] == "modified"
    assert result.modified_pages[0]["change_ratio"] > 0


@pytest.mark.asyncio
async def test_report_includes_label() -> None:
    block = SiteDiffBlock()
    result = await block.execute(SiteDiffInput(
        previous_pages=[], current_pages=[{"url": "https://x.com/", "title": "Home", "text": "Hi", "content_hash": "a"}],
        label="example.com",
    ))
    assert "example.com" in result.report
    assert result.label == "example.com"


@pytest.mark.asyncio
async def test_empty_inputs() -> None:
    block = SiteDiffBlock()
    result = await block.execute(SiteDiffInput(
        previous_pages=[], current_pages=[],
    ))
    assert result.has_changes is False
    assert result.total_previous == 0
    assert result.total_current == 0


@pytest.mark.asyncio
async def test_min_change_ratio_filters_noise() -> None:
    # Very minor text change
    prev = [{"url": "https://x.com/", "title": "Home", "text": "A " * 500, "content_hash": "abc"}]
    curr = [{"url": "https://x.com/", "title": "Home", "text": "A " * 499 + "B ", "content_hash": "xyz"}]
    block = SiteDiffBlock()
    result = await block.execute(SiteDiffInput(
        previous_pages=prev, current_pages=curr,
        min_change_ratio=0.5,  # High threshold
    ))
    assert result.modified_count == 0  # Filtered out
    assert result.unchanged_count == 1


@pytest.mark.asyncio
async def test_mixed_changes() -> None:
    prev = [
        {"url": "https://x.com/a", "title": "A", "text": "Page A content", "content_hash": "h1"},
        {"url": "https://x.com/b", "title": "B", "text": "Page B content", "content_hash": "h2"},
        {"url": "https://x.com/c", "title": "C", "text": "Page C content", "content_hash": "h3"},
    ]
    curr = [
        {"url": "https://x.com/a", "title": "A", "text": "Page A content", "content_hash": "h1"},  # unchanged
        {"url": "https://x.com/b", "title": "B", "text": "COMPLETELY NEW B CONTENT!!!", "content_hash": "h4"},  # modified
        {"url": "https://x.com/d", "title": "D", "text": "New page D", "content_hash": "h5"},  # new
    ]
    block = SiteDiffBlock()
    result = await block.execute(SiteDiffInput(
        previous_pages=prev, current_pages=curr,
    ))
    assert result.has_changes is True
    assert result.new_count == 1
    assert result.removed_count == 1
    assert result.modified_count == 1
    assert result.unchanged_count == 1
    assert result.total_previous == 3
    assert result.total_current == 3
