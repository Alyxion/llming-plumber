"""Unit tests for sink_file_iterator and sink_file_writer blocks."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from llming_plumber.blocks.base import BlockContext, FileInfo, Sink


# ---------------------------------------------------------------------------
# MemorySink — in-memory Sink for testing
# ---------------------------------------------------------------------------


class MemorySink(Sink):
    """In-memory Sink that stores files in a dict."""

    def __init__(self, files: dict[str, bytes] | None = None) -> None:
        self.files: dict[str, bytes] = dict(files or {})
        self.written: list[tuple[str, bytes]] = []

    async def write(
        self,
        path: str,
        content: str | bytes,
        *,
        content_type: str = "",
        metadata: dict[str, str] | None = None,
    ) -> None:
        data = content.encode("utf-8") if isinstance(content, str) else content
        self.files[path] = data
        self.written.append((path, data))

    async def read(self, path: str) -> bytes | None:
        return self.files.get(path)

    async def list(  # type: ignore[override]
        self,
        prefix: str = "",
        pattern: str = "*",
    ):
        import fnmatch

        for path, data in sorted(self.files.items()):
            if prefix and not path.startswith(prefix):
                continue
            filename = path.rsplit("/", 1)[-1]
            if pattern != "*" and not fnmatch.fnmatch(filename, pattern):
                continue
            yield FileInfo(
                path=path,
                filename=filename,
                size_bytes=len(data),
            )

    async def finalize(self) -> dict[str, Any]:
        return {"files_written": len(self.written)}


def _ctx(
    *,
    source_sink: Sink | None = None,
    sink: Sink | None = None,
) -> BlockContext:
    mock_console = AsyncMock()
    mock_console.write = AsyncMock()
    return BlockContext(
        pipeline_id="pl1",
        run_id="run1",
        block_id="b1",
        console=mock_console,
        sink=sink,
        source_sink=source_sink,
    )


# ---------------------------------------------------------------------------
# SinkFileIteratorBlock
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_iterator_lists_and_reads_files() -> None:
    from llming_plumber.blocks.data.sink_io import (
        SinkFileIteratorBlock,
        SinkFileIteratorInput,
    )

    source = MemorySink({
        "text/page1.txt": b"Hello page 1",
        "text/page2.txt": b"Hello page 2",
        "html/page1.html": b"<html>",
    })
    ctx = _ctx(source_sink=source)
    block = SinkFileIteratorBlock()
    result = await block.execute(
        SinkFileIteratorInput(folder="text/", pattern="*.txt"),
        ctx=ctx,
    )

    assert result.total_listed == 2
    assert result.total_returned == 2
    assert result.total_skipped == 0
    assert len(result.files) == 2
    assert result.files[0]["filename"] == "page1.txt"
    assert result.files[0]["text"] == "Hello page 1"
    assert result.files[1]["filename"] == "page2.txt"


@pytest.mark.asyncio
async def test_iterator_skips_already_processed() -> None:
    from llming_plumber.blocks.data.sink_io import (
        SinkFileIteratorBlock,
        SinkFileIteratorInput,
    )

    source = MemorySink({
        "text/page1.txt": b"Hello page 1",
        "text/page2.txt": b"Hello page 2",
        # page1 already has output
        "summaries/page1.json": b'{"summary": "existing"}',
    })
    ctx = _ctx(source_sink=source)
    block = SinkFileIteratorBlock()
    result = await block.execute(
        SinkFileIteratorInput(
            folder="text/", pattern="*.txt",
            skip_output_folder="summaries/",
        ),
        ctx=ctx,
    )

    assert result.total_listed == 2
    assert result.total_skipped == 1
    assert result.total_returned == 1
    assert result.files[0]["filename"] == "page2.txt"


@pytest.mark.asyncio
async def test_iterator_max_files() -> None:
    from llming_plumber.blocks.data.sink_io import (
        SinkFileIteratorBlock,
        SinkFileIteratorInput,
    )

    source = MemorySink({
        f"text/page{i}.txt": f"Content {i}".encode()
        for i in range(10)
    })
    ctx = _ctx(source_sink=source)
    block = SinkFileIteratorBlock()
    result = await block.execute(
        SinkFileIteratorInput(folder="text/", max_files=3),
        ctx=ctx,
    )

    assert result.total_returned == 3
    assert len(result.files) == 3


@pytest.mark.asyncio
async def test_iterator_empty_source() -> None:
    from llming_plumber.blocks.data.sink_io import (
        SinkFileIteratorBlock,
        SinkFileIteratorInput,
    )

    source = MemorySink({})
    ctx = _ctx(source_sink=source)
    block = SinkFileIteratorBlock()
    result = await block.execute(
        SinkFileIteratorInput(folder="text/"),
        ctx=ctx,
    )

    assert result.total_listed == 0
    assert result.total_returned == 0
    assert result.files == []


@pytest.mark.asyncio
async def test_iterator_requires_source_sink() -> None:
    from llming_plumber.blocks.data.sink_io import (
        SinkFileIteratorBlock,
        SinkFileIteratorInput,
    )

    block = SinkFileIteratorBlock()
    with pytest.raises(ValueError, match="requires a pipe from a resource"):
        await block.execute(SinkFileIteratorInput(folder="text/"))


@pytest.mark.asyncio
async def test_iterator_requires_source_sink_in_ctx() -> None:
    from llming_plumber.blocks.data.sink_io import (
        SinkFileIteratorBlock,
        SinkFileIteratorInput,
    )

    ctx = _ctx(source_sink=None)
    block = SinkFileIteratorBlock()
    with pytest.raises(ValueError, match="requires a pipe from a resource"):
        await block.execute(SinkFileIteratorInput(folder="text/"), ctx=ctx)


@pytest.mark.asyncio
async def test_iterator_logs_progress() -> None:
    from llming_plumber.blocks.data.sink_io import (
        SinkFileIteratorBlock,
        SinkFileIteratorInput,
    )

    source = MemorySink({"text/a.txt": b"data"})
    ctx = _ctx(source_sink=source)
    block = SinkFileIteratorBlock()
    await block.execute(
        SinkFileIteratorInput(folder="text/"),
        ctx=ctx,
    )

    assert ctx.console.write.call_count >= 1


def test_iterator_metadata() -> None:
    from llming_plumber.blocks.data.sink_io import SinkFileIteratorBlock

    assert SinkFileIteratorBlock.block_type == "sink_file_iterator"
    assert SinkFileIteratorBlock.fan_out_field == "files"
    assert "data/storage" in SinkFileIteratorBlock.categories


# ---------------------------------------------------------------------------
# SinkFileWriterBlock
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_writer_writes_to_sink() -> None:
    from llming_plumber.blocks.data.sink_io import (
        SinkFileWriterBlock,
        SinkFileWriterInput,
    )

    sink = MemorySink()
    ctx = _ctx(sink=sink)
    block = SinkFileWriterBlock()
    result = await block.execute(
        SinkFileWriterInput(path="summaries/page1.json", content='{"summary": "test"}'),
        ctx=ctx,
    )

    assert result.written is True
    assert result.path == "summaries/page1.json"
    assert result.size_bytes == len('{"summary": "test"}')
    assert sink.files["summaries/page1.json"] == b'{"summary": "test"}'


@pytest.mark.asyncio
async def test_writer_requires_sink() -> None:
    from llming_plumber.blocks.data.sink_io import (
        SinkFileWriterBlock,
        SinkFileWriterInput,
    )

    block = SinkFileWriterBlock()
    with pytest.raises(ValueError, match="requires a pipe to a resource"):
        await block.execute(
            SinkFileWriterInput(path="out.json", content="data"),
        )


@pytest.mark.asyncio
async def test_writer_requires_sink_in_ctx() -> None:
    from llming_plumber.blocks.data.sink_io import (
        SinkFileWriterBlock,
        SinkFileWriterInput,
    )

    ctx = _ctx(sink=None)
    block = SinkFileWriterBlock()
    with pytest.raises(ValueError, match="requires a pipe to a resource"):
        await block.execute(
            SinkFileWriterInput(path="out.json", content="data"),
            ctx=ctx,
        )


@pytest.mark.asyncio
async def test_writer_logs_progress() -> None:
    from llming_plumber.blocks.data.sink_io import (
        SinkFileWriterBlock,
        SinkFileWriterInput,
    )

    sink = MemorySink()
    ctx = _ctx(sink=sink)
    block = SinkFileWriterBlock()
    await block.execute(
        SinkFileWriterInput(path="out.json", content="data"),
        ctx=ctx,
    )

    assert ctx.console.write.call_count >= 1


def test_writer_metadata() -> None:
    from llming_plumber.blocks.data.sink_io import SinkFileWriterBlock

    assert SinkFileWriterBlock.block_type == "sink_file_writer"
    assert "data/storage" in SinkFileWriterBlock.categories
