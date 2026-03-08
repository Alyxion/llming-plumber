"""Unit tests for file operation blocks."""

from __future__ import annotations

import json
import os
import tempfile

import pytest


# ── FileListBlock ──


@pytest.mark.asyncio
async def test_file_list_basic() -> None:
    from llming_plumber.blocks.data.file_ops import FileListBlock, FileListInput

    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test files
        for name in ["a.txt", "b.txt", "c.csv"]:
            with open(os.path.join(tmpdir, name), "w") as f:
                f.write("content")

        block = FileListBlock()
        result = await block.execute(FileListInput(path=tmpdir, pattern="*"))
        assert result.count == 3


@pytest.mark.asyncio
async def test_file_list_with_pattern() -> None:
    from llming_plumber.blocks.data.file_ops import FileListBlock, FileListInput

    with tempfile.TemporaryDirectory() as tmpdir:
        for name in ["a.txt", "b.txt", "c.csv"]:
            with open(os.path.join(tmpdir, name), "w") as f:
                f.write("content")

        block = FileListBlock()
        result = await block.execute(FileListInput(path=tmpdir, pattern="*.txt"))
        assert result.count == 2


@pytest.mark.asyncio
async def test_file_list_recursive() -> None:
    from llming_plumber.blocks.data.file_ops import FileListBlock, FileListInput

    with tempfile.TemporaryDirectory() as tmpdir:
        os.makedirs(os.path.join(tmpdir, "sub"))
        with open(os.path.join(tmpdir, "top.txt"), "w") as f:
            f.write("top")
        with open(os.path.join(tmpdir, "sub", "deep.txt"), "w") as f:
            f.write("deep")

        block = FileListBlock()
        result = await block.execute(
            FileListInput(path=tmpdir, pattern="*.txt", recursive=True),
        )
        assert result.count == 2


@pytest.mark.asyncio
async def test_file_list_empty_dir() -> None:
    from llming_plumber.blocks.data.file_ops import FileListBlock, FileListInput

    with tempfile.TemporaryDirectory() as tmpdir:
        block = FileListBlock()
        result = await block.execute(FileListInput(path=tmpdir))
        assert result.count == 0


# ── FileReadBlock ──


@pytest.mark.asyncio
async def test_file_read_text() -> None:
    from llming_plumber.blocks.data.file_ops import FileReadBlock, FileReadInput

    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
        f.write("Hello World")
        path = f.name

    try:
        block = FileReadBlock()
        result = await block.execute(FileReadInput(path=path))
        assert result.content == "Hello World"
        assert result.size_bytes == 11
    finally:
        os.unlink(path)


@pytest.mark.asyncio
async def test_file_read_binary() -> None:
    from llming_plumber.blocks.data.file_ops import FileReadBlock, FileReadInput

    import base64

    with tempfile.NamedTemporaryFile(suffix=".bin", delete=False) as f:
        f.write(b"\x00\x01\x02\x03")
        path = f.name

    try:
        block = FileReadBlock()
        result = await block.execute(FileReadInput(path=path, encoding="binary"))
        decoded = base64.b64decode(result.content)
        assert decoded == b"\x00\x01\x02\x03"
    finally:
        os.unlink(path)


@pytest.mark.asyncio
async def test_file_read_not_found() -> None:
    from llming_plumber.blocks.data.file_ops import FileReadBlock, FileReadInput

    block = FileReadBlock()
    result = await block.execute(FileReadInput(path="/nonexistent/file.txt"))
    assert result.content == ""
    assert result.size_bytes == 0


# ── FileWriteBlock ──


@pytest.mark.asyncio
async def test_file_write() -> None:
    from llming_plumber.blocks.data.file_ops import FileWriteBlock, FileWriteInput

    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "output.txt")
        block = FileWriteBlock()
        result = await block.execute(FileWriteInput(path=path, content="Hello"))
        assert result.created is True
        assert result.size_bytes == 5
        with open(path) as f:
            assert f.read() == "Hello"


@pytest.mark.asyncio
async def test_file_write_mkdir() -> None:
    from llming_plumber.blocks.data.file_ops import FileWriteBlock, FileWriteInput

    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "sub", "deep", "file.txt")
        block = FileWriteBlock()
        result = await block.execute(FileWriteInput(path=path, content="nested", mkdir=True))
        assert result.created is True
        assert os.path.exists(path)


# ── FileMoveBlock ──


@pytest.mark.asyncio
async def test_file_move() -> None:
    from llming_plumber.blocks.data.file_ops import FileMoveBlock, FileMoveInput

    with tempfile.TemporaryDirectory() as tmpdir:
        src = os.path.join(tmpdir, "src.txt")
        dst = os.path.join(tmpdir, "dst.txt")
        with open(src, "w") as f:
            f.write("data")

        block = FileMoveBlock()
        result = await block.execute(FileMoveInput(source=src, destination=dst))
        assert result.success is True
        assert not os.path.exists(src)
        assert os.path.exists(dst)


@pytest.mark.asyncio
async def test_file_move_no_overwrite() -> None:
    from llming_plumber.blocks.data.file_ops import FileMoveBlock, FileMoveInput

    with tempfile.TemporaryDirectory() as tmpdir:
        src = os.path.join(tmpdir, "src.txt")
        dst = os.path.join(tmpdir, "dst.txt")
        with open(src, "w") as f:
            f.write("source")
        with open(dst, "w") as f:
            f.write("existing")

        block = FileMoveBlock()
        result = await block.execute(
            FileMoveInput(source=src, destination=dst, overwrite=False),
        )
        assert result.success is False
        assert os.path.exists(src)  # Source still exists


# ── FileDeleteBlock ──


@pytest.mark.asyncio
async def test_file_delete() -> None:
    from llming_plumber.blocks.data.file_ops import FileDeleteBlock, FileDeleteInput

    with tempfile.NamedTemporaryFile(delete=False) as f:
        path = f.name

    block = FileDeleteBlock()
    result = await block.execute(FileDeleteInput(path=path))
    assert result.deleted is True
    assert not os.path.exists(path)


@pytest.mark.asyncio
async def test_file_delete_not_found() -> None:
    from llming_plumber.blocks.data.file_ops import FileDeleteBlock, FileDeleteInput

    block = FileDeleteBlock()
    result = await block.execute(FileDeleteInput(path="/nonexistent"))
    assert result.deleted is False


# ── FileCollectorBlock ──


@pytest.mark.asyncio
async def test_file_collector() -> None:
    from llming_plumber.blocks.data.file_ops import FileCollectorBlock, FileCollectorInput

    items = [
        {"name": "a.txt", "path": "/tmp/a.txt", "size_bytes": 10},
        {"name": "b.txt", "path": "/tmp/b.txt", "size_bytes": 20},
    ]

    block = FileCollectorBlock()
    result = await block.execute(FileCollectorInput(items=items))
    assert result.count == 2
    assert len(result.files) == 2


# ── Registry ──


def test_file_blocks_in_registry() -> None:
    from llming_plumber.blocks.registry import BlockRegistry

    BlockRegistry.reset()
    BlockRegistry.discover()
    for bt in [
        "file_list", "file_read", "file_write", "file_collector",
        "file_move", "file_delete",
    ]:
        assert bt in BlockRegistry._registry, f"{bt} not registered"
