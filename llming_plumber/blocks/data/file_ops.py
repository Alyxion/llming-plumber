"""File operation blocks — list, read, write, move, delete, and collect files.

All filesystem I/O is offloaded to a thread via ``asyncio.to_thread`` so the
event loop stays free.  Blocks work standalone with ``ctx=None``.

Usage patterns:
    FileList → (fan-out) → FileRead → downstream
    upstream → FileWrite → confirmation
    FileList → FileCollector → aggregate
    FileMove / FileDelete → housekeeping
"""

from __future__ import annotations

import asyncio
import base64
import logging
import os
import pathlib
from datetime import UTC, datetime
from typing import Any, ClassVar, Literal

from pydantic import Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput
from llming_plumber.blocks.limits import check_file_size, check_list_size

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# FileList
# ------------------------------------------------------------------


class FileListInput(BlockInput):
    path: str = Field(
        title="Directory Path",
        description="Absolute path to the directory to list",
    )
    pattern: str = Field(
        default="*",
        title="Glob Pattern",
        description="Glob pattern to filter files (e.g. *.txt, *.csv)",
    )
    recursive: bool = Field(
        default=False,
        title="Recursive",
        description="Search subdirectories recursively",
        json_schema_extra={"widget": "toggle"},
    )


class FileListOutput(BlockOutput):
    files: list[dict[str, Any]] = []
    count: int = 0


def _list_files(
    path: str, pattern: str, recursive: bool,
) -> FileListOutput:
    """Synchronous file listing (runs in a thread)."""
    p = pathlib.Path(path)
    if not p.is_dir():
        return FileListOutput()

    if recursive:
        matches = list(p.rglob(pattern))
    else:
        matches = list(p.glob(pattern))

    files: list[dict[str, Any]] = []
    for m in matches:
        if not m.is_file():
            continue
        try:
            stat = m.stat()
            files.append({
                "name": m.name,
                "path": str(m),
                "size_bytes": stat.st_size,
                "modified_iso": datetime.fromtimestamp(
                    stat.st_mtime, tz=UTC,
                ).isoformat(),
            })
        except OSError:
            # Skip files we cannot stat (permissions, broken symlinks, etc.)
            continue

    return FileListOutput(files=files, count=len(files))


class FileListBlock(BaseBlock[FileListInput, FileListOutput]):
    block_type: ClassVar[str] = "file_list"
    icon: ClassVar[str] = "tabler/files"
    categories: ClassVar[list[str]] = ["data/files"]
    description: ClassVar[str] = "List files in a directory with optional glob pattern"
    fan_out_field: ClassVar[str | None] = "files"

    async def execute(
        self,
        input: FileListInput,
        ctx: BlockContext | None = None,
    ) -> FileListOutput:
        try:
            result = await asyncio.to_thread(
                _list_files, input.path, input.pattern, input.recursive,
            )
            check_list_size(result.files, label="File list")
        except Exception:
            logger.exception("file_list: failed to list %s", input.path)
            return FileListOutput()

        if ctx:
            await ctx.log(f"Found {result.count} file(s) in {input.path}")
        return result


# ------------------------------------------------------------------
# FileRead
# ------------------------------------------------------------------


class FileReadInput(BlockInput):
    path: str = Field(
        title="File Path",
        description="Absolute path to the file to read",
    )
    encoding: Literal["utf-8", "latin-1", "ascii", "binary"] = Field(
        default="utf-8",
        title="Encoding",
        description="Text encoding, or 'binary' for base64 output",
        json_schema_extra={
            "widget": "select",
            "options": ["utf-8", "latin-1", "ascii", "binary"],
        },
    )


class FileReadOutput(BlockOutput):
    content: str = ""
    path: str = ""
    size_bytes: int = 0
    encoding: str = ""


def _read_file(path: str, encoding: str) -> FileReadOutput:
    """Synchronous file read (runs in a thread)."""
    p = pathlib.Path(path)
    size = p.stat().st_size
    check_file_size(size, label=p.name)

    if encoding == "binary":
        raw = p.read_bytes()
        content = base64.b64encode(raw).decode()
    else:
        content = p.read_text(encoding=encoding)

    return FileReadOutput(
        content=content,
        path=str(p),
        size_bytes=size,
        encoding=encoding,
    )


class FileReadBlock(BaseBlock[FileReadInput, FileReadOutput]):
    block_type: ClassVar[str] = "file_read"
    icon: ClassVar[str] = "tabler/file-text"
    categories: ClassVar[list[str]] = ["data/files"]
    description: ClassVar[str] = "Read a file's content (text or binary as base64)"

    async def execute(
        self,
        input: FileReadInput,
        ctx: BlockContext | None = None,
    ) -> FileReadOutput:
        try:
            result = await asyncio.to_thread(
                _read_file, input.path, input.encoding,
            )
        except FileNotFoundError:
            logger.error("file_read: file not found: %s", input.path)
            return FileReadOutput(path=input.path, encoding=input.encoding)
        except Exception:
            logger.exception("file_read: failed to read %s", input.path)
            return FileReadOutput(path=input.path, encoding=input.encoding)

        if ctx:
            await ctx.log(
                f"Read {result.size_bytes:,} bytes from {input.path}"
            )
        return result


# ------------------------------------------------------------------
# FileWrite
# ------------------------------------------------------------------


class FileWriteInput(BlockInput):
    path: str = Field(
        title="File Path",
        description="Absolute path for the output file",
    )
    content: str = Field(
        title="Content",
        description="Content to write (text or base64 for binary)",
        json_schema_extra={"widget": "textarea", "rows": 6},
    )
    encoding: str = Field(
        default="utf-8",
        title="Encoding",
        description="Text encoding for writing",
    )
    mkdir: bool = Field(
        default=True,
        title="Create Directories",
        description="Create parent directories if they don't exist",
        json_schema_extra={"widget": "toggle"},
    )


class FileWriteOutput(BlockOutput):
    path: str = ""
    size_bytes: int = 0
    created: bool = False


def _write_file(
    path: str, content: str, encoding: str, mkdir: bool,
) -> FileWriteOutput:
    """Synchronous file write (runs in a thread)."""
    p = pathlib.Path(path)
    already_exists = p.exists()

    if mkdir:
        p.parent.mkdir(parents=True, exist_ok=True)

    p.write_text(content, encoding=encoding)
    size = p.stat().st_size

    return FileWriteOutput(
        path=str(p),
        size_bytes=size,
        created=not already_exists,
    )


class FileWriteBlock(BaseBlock[FileWriteInput, FileWriteOutput]):
    block_type: ClassVar[str] = "file_write"
    icon: ClassVar[str] = "tabler/file-pencil"
    categories: ClassVar[list[str]] = ["data/files"]
    description: ClassVar[str] = "Write content to a file"

    async def execute(
        self,
        input: FileWriteInput,
        ctx: BlockContext | None = None,
    ) -> FileWriteOutput:
        try:
            result = await asyncio.to_thread(
                _write_file,
                input.path,
                input.content,
                input.encoding,
                input.mkdir,
            )
        except Exception:
            logger.exception("file_write: failed to write %s", input.path)
            return FileWriteOutput(path=input.path)

        if ctx:
            verb = "Created" if result.created else "Updated"
            await ctx.log(
                f"{verb} {input.path} ({result.size_bytes:,} bytes)"
            )
        return result


# ------------------------------------------------------------------
# FileCollector
# ------------------------------------------------------------------


class FileCollectorInput(BlockInput):
    items: list[dict[str, Any]] = Field(
        default_factory=list,
        title="Items",
        description="Collected file entries (populated by executor during fan-in)",
    )


class FileCollectorOutput(BlockOutput):
    files: list[dict[str, Any]] = []
    count: int = 0


class FileCollectorBlock(BaseBlock[FileCollectorInput, FileCollectorOutput]):
    block_type: ClassVar[str] = "file_collector"
    icon: ClassVar[str] = "tabler/arrows-merge"
    categories: ClassVar[list[str]] = ["data/files"]
    description: ClassVar[str] = (
        "Fan-in: collect multiple file entries into a single list"
    )
    fan_in: ClassVar[bool] = True

    async def execute(
        self,
        input: FileCollectorInput,
        ctx: BlockContext | None = None,
    ) -> FileCollectorOutput:
        check_list_size(input.items, label="File collector items")
        result = FileCollectorOutput(
            files=input.items,
            count=len(input.items),
        )
        if ctx:
            await ctx.log(f"Collected {result.count} file entry/entries")
        return result


# ------------------------------------------------------------------
# FileMove
# ------------------------------------------------------------------


class FileMoveInput(BlockInput):
    source: str = Field(
        title="Source Path",
        description="Absolute path of the file to move",
    )
    destination: str = Field(
        title="Destination Path",
        description="Absolute path for the new location",
    )
    overwrite: bool = Field(
        default=False,
        title="Overwrite",
        description="Overwrite if destination already exists",
        json_schema_extra={"widget": "toggle"},
    )


class FileMoveOutput(BlockOutput):
    source: str = ""
    destination: str = ""
    success: bool = False


def _move_file(
    source: str, destination: str, overwrite: bool,
) -> FileMoveOutput:
    """Synchronous file move (runs in a thread)."""
    src = pathlib.Path(source)
    dst = pathlib.Path(destination)

    if not src.exists():
        return FileMoveOutput(source=source, destination=destination)

    if dst.exists() and not overwrite:
        return FileMoveOutput(source=source, destination=destination)

    dst.parent.mkdir(parents=True, exist_ok=True)
    os.replace(str(src), str(dst))

    return FileMoveOutput(
        source=source,
        destination=destination,
        success=True,
    )


class FileMoveBlock(BaseBlock[FileMoveInput, FileMoveOutput]):
    block_type: ClassVar[str] = "file_move"
    icon: ClassVar[str] = "tabler/file-arrow-right"
    categories: ClassVar[list[str]] = ["data/files"]
    description: ClassVar[str] = "Move or rename a file"

    async def execute(
        self,
        input: FileMoveInput,
        ctx: BlockContext | None = None,
    ) -> FileMoveOutput:
        try:
            result = await asyncio.to_thread(
                _move_file,
                input.source,
                input.destination,
                input.overwrite,
            )
        except Exception:
            logger.exception(
                "file_move: failed to move %s → %s",
                input.source,
                input.destination,
            )
            return FileMoveOutput(
                source=input.source,
                destination=input.destination,
            )

        if ctx and result.success:
            await ctx.log(
                f"Moved {input.source} → {input.destination}"
            )
        return result


# ------------------------------------------------------------------
# FileDelete
# ------------------------------------------------------------------


class FileDeleteInput(BlockInput):
    path: str = Field(
        title="File Path",
        description="Absolute path of the file to delete",
    )


class FileDeleteOutput(BlockOutput):
    path: str = ""
    deleted: bool = False


def _delete_file(path: str) -> FileDeleteOutput:
    """Synchronous file delete (runs in a thread)."""
    p = pathlib.Path(path)
    if not p.exists():
        return FileDeleteOutput(path=path)
    if not p.is_file():
        return FileDeleteOutput(path=path)
    p.unlink()
    return FileDeleteOutput(path=path, deleted=True)


class FileDeleteBlock(BaseBlock[FileDeleteInput, FileDeleteOutput]):
    block_type: ClassVar[str] = "file_delete"
    icon: ClassVar[str] = "tabler/file-x"
    categories: ClassVar[list[str]] = ["data/files"]
    description: ClassVar[str] = "Delete a file"

    async def execute(
        self,
        input: FileDeleteInput,
        ctx: BlockContext | None = None,
    ) -> FileDeleteOutput:
        try:
            result = await asyncio.to_thread(_delete_file, input.path)
        except Exception:
            logger.exception("file_delete: failed to delete %s", input.path)
            return FileDeleteOutput(path=input.path)

        if ctx and result.deleted:
            await ctx.log(f"Deleted {input.path}")
        return result
