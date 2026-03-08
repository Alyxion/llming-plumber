"""Unit tests for archive blocks — zip create/extract/list."""

from __future__ import annotations

import base64
import io
import zipfile

import pytest


def _make_zip(files: dict[str, str]) -> str:
    """Create a zip with given {name: content} pairs, return base64."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return base64.b64encode(buf.getvalue()).decode()


# ── ZipCreateBlock ──


@pytest.mark.asyncio
async def test_zip_create() -> None:
    from llming_plumber.blocks.data.archive import ZipCreateBlock, ZipCreateInput

    files = [
        {"name": "hello.txt", "content_base64": base64.b64encode(b"Hello!").decode()},
        {"name": "data.csv", "content_base64": base64.b64encode(b"a,b,c\n1,2,3").decode()},
    ]
    import json

    block = ZipCreateBlock()
    result = await block.execute(
        ZipCreateInput(files=json.dumps(files), archive_name="test.zip"),
    )
    assert result.file_count == 2
    assert result.archive_name == "test.zip"
    assert result.size_bytes > 0

    # Verify the zip is valid
    raw = base64.b64decode(result.archive_base64)
    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
        assert len(zf.namelist()) == 2
        assert zf.read("hello.txt") == b"Hello!"


@pytest.mark.asyncio
async def test_zip_create_empty() -> None:
    from llming_plumber.blocks.data.archive import ZipCreateBlock, ZipCreateInput

    block = ZipCreateBlock()
    result = await block.execute(ZipCreateInput(files="[]"))
    assert result.file_count == 0
    assert result.size_bytes > 0  # Empty zip still has metadata


# ── ZipExtractBlock ──


@pytest.mark.asyncio
async def test_zip_extract() -> None:
    from llming_plumber.blocks.data.archive import ZipExtractBlock, ZipExtractInput

    archive = _make_zip({"readme.md": "# Hello", "data.json": '{"x": 1}'})

    block = ZipExtractBlock()
    result = await block.execute(ZipExtractInput(archive_base64=archive))
    assert result.file_count == 2
    assert len(result.files) == 2

    names = [f["name"] for f in result.files]
    assert "readme.md" in names
    assert "data.json" in names

    # Verify content
    for f in result.files:
        if f["name"] == "readme.md":
            assert base64.b64decode(f["content_base64"]) == b"# Hello"


@pytest.mark.asyncio
async def test_zip_extract_empty() -> None:
    from llming_plumber.blocks.data.archive import ZipExtractBlock, ZipExtractInput

    archive = _make_zip({})
    block = ZipExtractBlock()
    result = await block.execute(ZipExtractInput(archive_base64=archive))
    assert result.file_count == 0


# ── ZipListBlock ──


@pytest.mark.asyncio
async def test_zip_list() -> None:
    from llming_plumber.blocks.data.archive import ZipListBlock, ZipListInput

    archive = _make_zip({"a.txt": "aaa", "sub/b.txt": "bbb"})

    block = ZipListBlock()
    result = await block.execute(ZipListInput(archive_base64=archive))
    assert result.file_count == 2
    names = [e["name"] for e in result.entries]
    assert "a.txt" in names
    assert "sub/b.txt" in names

    for entry in result.entries:
        assert "size_bytes" in entry
        assert "is_dir" in entry
        assert entry["is_dir"] is False


@pytest.mark.asyncio
async def test_zip_list_with_directory() -> None:
    from llming_plumber.blocks.data.archive import ZipListBlock, ZipListInput

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.mkdir("subdir")
        zf.writestr("subdir/file.txt", "content")
    archive = base64.b64encode(buf.getvalue()).decode()

    block = ZipListBlock()
    result = await block.execute(ZipListInput(archive_base64=archive))
    assert result.file_count == 2  # dir + file
    dirs = [e for e in result.entries if e["is_dir"]]
    assert len(dirs) == 1


# ── Registry ──


def test_archive_blocks_in_registry() -> None:
    from llming_plumber.blocks.registry import BlockRegistry

    BlockRegistry.reset()
    BlockRegistry.discover()
    for bt in ["zip_create", "zip_extract", "zip_list"]:
        assert bt in BlockRegistry._registry, f"{bt} not registered"
