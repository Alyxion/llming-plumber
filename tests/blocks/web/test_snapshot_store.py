"""Unit tests for snapshot_save and snapshot_load blocks."""

from __future__ import annotations

import json
import pathlib
import tempfile

import pytest

from llming_plumber.blocks.web.snapshot_store import (
    SnapshotLoadBlock,
    SnapshotLoadInput,
    SnapshotSaveBlock,
    SnapshotSaveInput,
)


@pytest.fixture
def tmp_dir(tmp_path: pathlib.Path) -> str:
    return str(tmp_path)


@pytest.mark.asyncio
async def test_save_first_snapshot(tmp_dir: str) -> None:
    block = SnapshotSaveBlock()
    pages = [{"url": "https://example.com/", "title": "Home", "text": "Hello"}]
    result = await block.execute(SnapshotSaveInput(
        snapshot_id="test-site", pages=pages, storage_dir=tmp_dir,
    ))
    assert result.page_count == 1
    assert result.snapshot_id == "test-site"
    assert result.previous_exists is False
    assert result.size_bytes > 0
    # Verify file exists
    assert pathlib.Path(result.path).exists()


@pytest.mark.asyncio
async def test_save_rotates_previous(tmp_dir: str) -> None:
    block = SnapshotSaveBlock()
    pages1 = [{"url": "https://example.com/", "title": "V1"}]
    pages2 = [{"url": "https://example.com/", "title": "V2"}]

    await block.execute(SnapshotSaveInput(
        snapshot_id="test-site", pages=pages1, storage_dir=tmp_dir,
    ))
    result2 = await block.execute(SnapshotSaveInput(
        snapshot_id="test-site", pages=pages2, storage_dir=tmp_dir,
    ))
    assert result2.previous_exists is True

    # Check previous file contains V1
    prev_path = pathlib.Path(tmp_dir) / "test-site.prev.json"
    assert prev_path.exists()
    prev_data = json.loads(prev_path.read_text())
    assert prev_data["pages"][0]["title"] == "V1"

    # Check current file contains V2
    curr_path = pathlib.Path(tmp_dir) / "test-site.json"
    curr_data = json.loads(curr_path.read_text())
    assert curr_data["pages"][0]["title"] == "V2"


@pytest.mark.asyncio
async def test_load_nonexistent(tmp_dir: str) -> None:
    block = SnapshotLoadBlock()
    result = await block.execute(SnapshotLoadInput(
        snapshot_id="missing", storage_dir=tmp_dir,
    ))
    assert result.exists is False
    assert result.page_count == 0
    assert result.pages == []


@pytest.mark.asyncio
async def test_load_current(tmp_dir: str) -> None:
    saver = SnapshotSaveBlock()
    pages = [{"url": "https://example.com/", "title": "Home", "text": "Content"}]
    await saver.execute(SnapshotSaveInput(
        snapshot_id="site", pages=pages, storage_dir=tmp_dir,
    ))

    loader = SnapshotLoadBlock()
    result = await loader.execute(SnapshotLoadInput(
        snapshot_id="site", which="current", storage_dir=tmp_dir,
    ))
    assert result.exists is True
    assert result.page_count == 1
    assert result.pages[0]["title"] == "Home"
    assert result.timestamp  # Non-empty


@pytest.mark.asyncio
async def test_load_previous(tmp_dir: str) -> None:
    saver = SnapshotSaveBlock()
    pages1 = [{"url": "https://example.com/", "title": "V1"}]
    pages2 = [{"url": "https://example.com/", "title": "V2"}]
    await saver.execute(SnapshotSaveInput(
        snapshot_id="site", pages=pages1, storage_dir=tmp_dir,
    ))
    await saver.execute(SnapshotSaveInput(
        snapshot_id="site", pages=pages2, storage_dir=tmp_dir,
    ))

    loader = SnapshotLoadBlock()
    result = await loader.execute(SnapshotLoadInput(
        snapshot_id="site", which="previous", storage_dir=tmp_dir,
    ))
    assert result.exists is True
    assert result.pages[0]["title"] == "V1"


@pytest.mark.asyncio
async def test_roundtrip_save_load_diff(tmp_dir: str) -> None:
    """Full roundtrip: save → save → load previous → compare."""
    from llming_plumber.blocks.web.site_diff import SiteDiffBlock, SiteDiffInput

    saver = SnapshotSaveBlock()
    loader = SnapshotLoadBlock()
    differ = SiteDiffBlock()

    pages_v1 = [
        {"url": "https://example.com/", "title": "Home", "text": "Welcome", "content_hash": "aaa"},
        {"url": "https://example.com/about", "title": "About", "text": "About us", "content_hash": "bbb"},
    ]
    pages_v2 = [
        {"url": "https://example.com/", "title": "Home", "text": "Welcome", "content_hash": "aaa"},
        {"url": "https://example.com/products", "title": "Products", "text": "New products!", "content_hash": "ccc"},
    ]

    await saver.execute(SnapshotSaveInput(snapshot_id="ex", pages=pages_v1, storage_dir=tmp_dir))
    await saver.execute(SnapshotSaveInput(snapshot_id="ex", pages=pages_v2, storage_dir=tmp_dir))

    prev = await loader.execute(SnapshotLoadInput(snapshot_id="ex", which="previous", storage_dir=tmp_dir))
    curr = await loader.execute(SnapshotLoadInput(snapshot_id="ex", which="current", storage_dir=tmp_dir))

    diff = await differ.execute(SiteDiffInput(
        previous_pages=prev.pages, current_pages=curr.pages,
    ))
    assert diff.has_changes is True
    assert diff.new_count == 1  # /products is new
    assert diff.removed_count == 1  # /about is removed
    assert diff.unchanged_count == 1  # / is unchanged
