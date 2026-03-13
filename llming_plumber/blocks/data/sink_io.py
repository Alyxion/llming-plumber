"""Sink file I/O blocks — iterate and write files in resource storage.

``sink_file_iterator``
    Lists files in a connected resource (Azure Blob, etc.), reads their
    content, and fans out individual file parcels.  Built-in "skip already
    processed" logic detects output files in a configurable folder.

``sink_file_writer``
    Writes content to a connected resource.  Designed for fan-out
    iterations: each upstream parcel produces one output file.
"""

from __future__ import annotations

from typing import Any, ClassVar

from pydantic import Field

from llming_plumber.blocks.base import (
    BaseBlock,
    BlockContext,
    BlockInput,
    BlockOutput,
)
from llming_plumber.blocks.limits import MAX_FAN_OUT_ITEMS, check_list_size


# ------------------------------------------------------------------
# Sink File Iterator
# ------------------------------------------------------------------


class SinkFileIteratorInput(BlockInput):
    folder: str = Field(
        default="",
        title="Folder",
        description=(
            "Subfolder prefix to list (e.g. 'text/'). "
            "Supports {date}, {run_id} templates."
        ),
        json_schema_extra={"placeholder": "text/"},
    )
    pattern: str = Field(
        default="*.txt",
        title="File Pattern",
        description="Glob pattern to filter files (e.g. *.txt, *.html)",
    )
    skip_output_folder: str = Field(
        default="",
        title="Skip If Output Exists",
        description=(
            "Skip files that already have output in this folder "
            "(e.g. 'summaries/'). Checks for {stem}.json."
        ),
        json_schema_extra={"placeholder": "summaries/"},
    )
    max_files: int = Field(
        default=0,
        title="Max Files",
        description="Maximum files to process (0 = no limit)",
        json_schema_extra={"min": 0},
    )
    encoding: str = Field(
        default="utf-8",
        title="Encoding",
        description="Text encoding for reading file content",
    )


class SinkFileIteratorOutput(BlockOutput):
    files: list[dict[str, Any]] = []
    total_listed: int = 0
    total_skipped: int = 0
    total_returned: int = 0


class SinkFileIteratorBlock(
    BaseBlock[SinkFileIteratorInput, SinkFileIteratorOutput],
):
    block_type: ClassVar[str] = "sink_file_iterator"
    icon: ClassVar[str] = "tabler/file-search"
    categories: ClassVar[list[str]] = ["data/storage"]
    description: ClassVar[str] = (
        "List and read files from a resource — fans out individual file parcels"
    )
    fan_out_field: ClassVar[str | None] = "files"

    async def execute(
        self,
        input: SinkFileIteratorInput,
        ctx: BlockContext | None = None,
    ) -> SinkFileIteratorOutput:
        if ctx is None or ctx.source_sink is None:
            raise ValueError(
                "sink_file_iterator requires a pipe from a resource block"
            )

        source = ctx.source_sink
        files: list[dict[str, Any]] = []
        total_listed = 0
        total_skipped = 0
        skip_folder = input.skip_output_folder.strip().rstrip("/")

        async for fi in source.list(prefix=input.folder, pattern=input.pattern):
            total_listed += 1

            # Skip-already-processed check
            if skip_folder:
                stem = fi.filename.rsplit(".", 1)[0] if "." in fi.filename else fi.filename
                output_path = f"{skip_folder}/{stem}.json"
                existing = await source.read(output_path)
                if existing is not None:
                    total_skipped += 1
                    continue

            # Read file content
            raw = await source.read(fi.path)
            if raw is None:
                continue

            text = raw.decode(input.encoding, errors="replace")

            files.append({
                "path": fi.path,
                "filename": fi.filename,
                "text": text,
                "size": fi.size_bytes,
                "modified": fi.modified_iso,
            })

            if input.max_files > 0 and len(files) >= input.max_files:
                break

        check_list_size(files, limit=MAX_FAN_OUT_ITEMS, label="Sink file iterator")

        if ctx:
            await ctx.log(
                f"Listed {total_listed} files, skipped {total_skipped}, "
                f"returning {len(files)} for processing"
            )

        return SinkFileIteratorOutput(
            files=files,
            total_listed=total_listed,
            total_skipped=total_skipped,
            total_returned=len(files),
        )


# ------------------------------------------------------------------
# Sink File Writer
# ------------------------------------------------------------------


class SinkFileWriterInput(BlockInput):
    path: str = Field(
        title="Output Path",
        description=(
            "Path for the output file in the resource. "
            "Supports templates like {filename}, {date}."
        ),
        json_schema_extra={"placeholder": "summaries/{filename}.json"},
    )
    content: str = Field(
        title="Content",
        description="Content to write",
        json_schema_extra={"widget": "textarea"},
    )


class SinkFileWriterOutput(BlockOutput):
    path: str = ""
    size_bytes: int = 0
    written: bool = False


class SinkFileWriterBlock(
    BaseBlock[SinkFileWriterInput, SinkFileWriterOutput],
):
    block_type: ClassVar[str] = "sink_file_writer"
    icon: ClassVar[str] = "tabler/file-upload"
    categories: ClassVar[list[str]] = ["data/storage"]
    description: ClassVar[str] = (
        "Write a file to a resource — designed for fan-out iterations"
    )

    async def execute(
        self,
        input: SinkFileWriterInput,
        ctx: BlockContext | None = None,
    ) -> SinkFileWriterOutput:
        if ctx is None or ctx.sink is None:
            raise ValueError(
                "sink_file_writer requires a pipe to a resource block"
            )

        data = (
            input.content.encode("utf-8")
            if isinstance(input.content, str)
            else input.content
        )

        await ctx.sink.write(input.path, data)

        if ctx:
            await ctx.log(f"Wrote {len(data):,} bytes → {input.path}")

        return SinkFileWriterOutput(
            path=input.path,
            size_bytes=len(data),
            written=True,
        )
