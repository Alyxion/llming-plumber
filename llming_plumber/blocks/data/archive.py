"""Archive blocks — create, extract, and list zip archives.

Files are passed as base64-encoded strings so they can travel through
pipes without touching the filesystem.  All I/O is offloaded to a
thread via ``asyncio.to_thread`` to keep the event loop free.

Usage pattern:
    FileRead → ZipCreate → downstream
    ZipExtract → (fan-out per file) → FileWrite
    ZipList → inspect contents without extracting
"""

from __future__ import annotations

import asyncio
import base64
import io
import json
import logging
import zipfile
from typing import Any, ClassVar

from pydantic import Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput
from llming_plumber.blocks.limits import (
    check_base64_size,
    check_list_size,
)

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# ZipCreate
# ------------------------------------------------------------------


class ZipCreateInput(BlockInput):
    files: str = Field(
        title="Files",
        description=(
            "JSON array of {name: str, content_base64: str} objects"
        ),
        json_schema_extra={"widget": "textarea", "rows": 6},
    )
    archive_name: str = Field(
        default="archive.zip",
        title="Archive Name",
        description="File name for the resulting zip archive",
    )


class ZipCreateOutput(BlockOutput):
    archive_base64: str = ""
    archive_name: str = ""
    file_count: int = 0
    size_bytes: int = 0


def _create_zip(files_json: str, archive_name: str) -> ZipCreateOutput:
    """Synchronous zip creation (runs in a thread)."""
    entries: list[dict[str, Any]] = json.loads(files_json)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for entry in entries:
            name = entry["name"]
            data = base64.b64decode(entry["content_base64"])
            zf.writestr(name, data)
    raw = buf.getvalue()
    return ZipCreateOutput(
        archive_base64=base64.b64encode(raw).decode(),
        archive_name=archive_name,
        file_count=len(entries),
        size_bytes=len(raw),
    )


class ZipCreateBlock(BaseBlock[ZipCreateInput, ZipCreateOutput]):
    block_type: ClassVar[str] = "zip_create"
    icon: ClassVar[str] = "tabler/file-zip"
    categories: ClassVar[list[str]] = ["data/archive"]
    description: ClassVar[str] = "Create a zip archive from files"

    async def execute(
        self,
        input: ZipCreateInput,
        ctx: BlockContext | None = None,
    ) -> ZipCreateOutput:
        try:
            entries: list[dict[str, Any]] = json.loads(input.files)
            check_list_size(entries, label="Zip entries")
            for entry in entries:
                check_base64_size(
                    entry.get("content_base64", ""),
                    label=entry.get("name", "file"),
                )
        except json.JSONDecodeError as exc:
            logger.error("zip_create: invalid JSON input: %s", exc)
            return ZipCreateOutput(archive_name=input.archive_name)

        try:
            result = await asyncio.to_thread(
                _create_zip, input.files, input.archive_name,
            )
        except Exception:
            logger.exception("zip_create: failed to build archive")
            return ZipCreateOutput(archive_name=input.archive_name)

        if ctx:
            await ctx.log(
                f"Created {result.archive_name} with {result.file_count} "
                f"file(s), {result.size_bytes:,} bytes"
            )
        return result


# ------------------------------------------------------------------
# ZipExtract
# ------------------------------------------------------------------


class ZipExtractInput(BlockInput):
    archive_base64: str = Field(
        title="Archive (base64)",
        description="Base64-encoded zip archive",
        json_schema_extra={"widget": "textarea", "rows": 4},
    )
    password: str = Field(
        default="",
        title="Password",
        description="Optional password for encrypted archives",
        json_schema_extra={"secret": True},
    )


class ZipExtractOutput(BlockOutput):
    files: list[dict[str, Any]] = []
    file_count: int = 0


def _extract_zip(
    archive_b64: str, password: str,
) -> ZipExtractOutput:
    """Synchronous zip extraction (runs in a thread)."""
    raw = base64.b64decode(archive_b64)
    buf = io.BytesIO(raw)
    pwd = password.encode() if password else None
    files: list[dict[str, Any]] = []
    with zipfile.ZipFile(buf, "r") as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            data = zf.read(info.filename, pwd=pwd)
            files.append({
                "name": info.filename,
                "content_base64": base64.b64encode(data).decode(),
                "size_bytes": len(data),
            })
    return ZipExtractOutput(files=files, file_count=len(files))


class ZipExtractBlock(BaseBlock[ZipExtractInput, ZipExtractOutput]):
    block_type: ClassVar[str] = "zip_extract"
    icon: ClassVar[str] = "tabler/file-zip"
    categories: ClassVar[list[str]] = ["data/archive"]
    description: ClassVar[str] = "Extract files from a zip archive"
    fan_out_field: ClassVar[str | None] = "files"

    async def execute(
        self,
        input: ZipExtractInput,
        ctx: BlockContext | None = None,
    ) -> ZipExtractOutput:
        check_base64_size(input.archive_base64, label="Archive")
        try:
            result = await asyncio.to_thread(
                _extract_zip, input.archive_base64, input.password,
            )
        except Exception:
            logger.exception("zip_extract: failed to extract archive")
            return ZipExtractOutput()

        if ctx:
            await ctx.log(f"Extracted {result.file_count} file(s)")
        return result


# ------------------------------------------------------------------
# ZipList
# ------------------------------------------------------------------


class ZipListInput(BlockInput):
    archive_base64: str = Field(
        title="Archive (base64)",
        description="Base64-encoded zip archive",
        json_schema_extra={"widget": "textarea", "rows": 4},
    )


class ZipListOutput(BlockOutput):
    entries: list[dict[str, Any]] = []
    file_count: int = 0


def _list_zip(archive_b64: str) -> ZipListOutput:
    """Synchronous zip listing (runs in a thread)."""
    raw = base64.b64decode(archive_b64)
    buf = io.BytesIO(raw)
    entries: list[dict[str, Any]] = []
    with zipfile.ZipFile(buf, "r") as zf:
        for info in zf.infolist():
            entries.append({
                "name": info.filename,
                "size_bytes": info.file_size,
                "compressed_size": info.compress_size,
                "is_dir": info.is_dir(),
            })
    return ZipListOutput(entries=entries, file_count=len(entries))


class ZipListBlock(BaseBlock[ZipListInput, ZipListOutput]):
    block_type: ClassVar[str] = "zip_list"
    icon: ClassVar[str] = "tabler/file-zip"
    categories: ClassVar[list[str]] = ["data/archive"]
    description: ClassVar[str] = "List contents of a zip archive without extracting"
    fan_out_field: ClassVar[str | None] = "entries"

    async def execute(
        self,
        input: ZipListInput,
        ctx: BlockContext | None = None,
    ) -> ZipListOutput:
        check_base64_size(input.archive_base64, label="Archive")
        try:
            result = await asyncio.to_thread(_list_zip, input.archive_base64)
        except Exception:
            logger.exception("zip_list: failed to read archive")
            return ZipListOutput()

        if ctx:
            await ctx.log(f"Archive contains {result.file_count} entry/entries")
        return result
