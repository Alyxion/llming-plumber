"""Snapshot store blocks — save and load site snapshots for comparison.

Two blocks:
  - ``snapshot_save``: writes crawl results to a JSON file on disk
  - ``snapshot_load``: reads the previous snapshot back for diffing

Snapshots are stored as JSON in a configurable directory, keyed by a
``snapshot_id`` (typically the domain or pipeline label).
"""

from __future__ import annotations

import asyncio
import json
import logging
import pathlib
from datetime import UTC, datetime
from typing import Any, ClassVar

from pydantic import Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput

logger = logging.getLogger(__name__)

_DEFAULT_DIR = "/tmp/plumber_snapshots"


# ------------------------------------------------------------------
# SnapshotSave
# ------------------------------------------------------------------


class SnapshotSaveInput(BlockInput):
    snapshot_id: str = Field(
        title="Snapshot ID",
        description="Unique ID for this snapshot (e.g. 'example.com' or 'my-site')",
        json_schema_extra={"placeholder": "my-site"},
    )
    pages: list[dict[str, Any]] = Field(
        default_factory=list,
        title="Pages",
        description="List of crawled page dicts to snapshot",
    )
    storage_dir: str = Field(
        default=_DEFAULT_DIR,
        title="Storage Directory",
        description="Directory to store snapshot files",
    )


class SnapshotSaveOutput(BlockOutput):
    path: str = ""
    page_count: int = 0
    snapshot_id: str = ""
    timestamp: str = ""
    size_bytes: int = 0
    previous_exists: bool = False


def _save_snapshot(
    snapshot_id: str,
    pages: list[dict[str, Any]],
    storage_dir: str,
) -> SnapshotSaveOutput:
    """Save snapshot to disk (sync, runs in thread)."""
    base = pathlib.Path(storage_dir)
    base.mkdir(parents=True, exist_ok=True)

    current_file = base / f"{snapshot_id}.json"
    previous_file = base / f"{snapshot_id}.prev.json"
    previous_exists = current_file.exists()

    # Rotate: current → previous
    if previous_exists:
        if previous_file.exists():
            previous_file.unlink()
        current_file.rename(previous_file)

    # Write new snapshot
    ts = datetime.now(UTC).isoformat()
    data = {
        "snapshot_id": snapshot_id,
        "timestamp": ts,
        "page_count": len(pages),
        "pages": pages,
    }
    raw = json.dumps(data, ensure_ascii=False, indent=None)
    current_file.write_text(raw, encoding="utf-8")

    return SnapshotSaveOutput(
        path=str(current_file),
        page_count=len(pages),
        snapshot_id=snapshot_id,
        timestamp=ts,
        size_bytes=len(raw),
        previous_exists=previous_exists,
    )


class SnapshotSaveBlock(BaseBlock[SnapshotSaveInput, SnapshotSaveOutput]):
    block_type: ClassVar[str] = "snapshot_save"
    icon: ClassVar[str] = "tabler/device-floppy"
    categories: ClassVar[list[str]] = ["web/monitor"]
    description: ClassVar[str] = "Save a crawl snapshot to disk for later comparison"
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self, input: SnapshotSaveInput, ctx: BlockContext | None = None
    ) -> SnapshotSaveOutput:
        try:
            result = await asyncio.to_thread(
                _save_snapshot, input.snapshot_id, input.pages, input.storage_dir,
            )
        except Exception:
            logger.exception("snapshot_save: failed for %s", input.snapshot_id)
            return SnapshotSaveOutput(snapshot_id=input.snapshot_id)

        if ctx:
            prev_note = " (previous snapshot rotated)" if result.previous_exists else " (first snapshot)"
            await ctx.log(
                f"Saved snapshot '{input.snapshot_id}': {result.page_count} pages, "
                f"{result.size_bytes:,} bytes{prev_note}"
            )
        return result


# ------------------------------------------------------------------
# SnapshotLoad
# ------------------------------------------------------------------


class SnapshotLoadInput(BlockInput):
    snapshot_id: str = Field(
        title="Snapshot ID",
        description="The snapshot ID to load (matches what was used in snapshot_save)",
        json_schema_extra={"placeholder": "my-site"},
    )
    which: str = Field(
        default="previous",
        title="Which Snapshot",
        description="Load the 'current' or 'previous' snapshot",
        json_schema_extra={"widget": "select", "options": ["previous", "current"]},
    )
    storage_dir: str = Field(
        default=_DEFAULT_DIR,
        title="Storage Directory",
        description="Directory where snapshots are stored",
    )


class SnapshotLoadOutput(BlockOutput):
    pages: list[dict[str, Any]] = Field(default_factory=list)
    page_count: int = 0
    snapshot_id: str = ""
    timestamp: str = ""
    exists: bool = False


def _load_snapshot(
    snapshot_id: str,
    which: str,
    storage_dir: str,
) -> SnapshotLoadOutput:
    """Load snapshot from disk (sync, runs in thread)."""
    base = pathlib.Path(storage_dir)
    suffix = ".prev.json" if which == "previous" else ".json"
    path = base / f"{snapshot_id}{suffix}"

    if not path.exists():
        return SnapshotLoadOutput(snapshot_id=snapshot_id)

    raw = path.read_text(encoding="utf-8")
    data = json.loads(raw)

    return SnapshotLoadOutput(
        pages=data.get("pages", []),
        page_count=data.get("page_count", 0),
        snapshot_id=snapshot_id,
        timestamp=data.get("timestamp", ""),
        exists=True,
    )


class SnapshotLoadBlock(BaseBlock[SnapshotLoadInput, SnapshotLoadOutput]):
    block_type: ClassVar[str] = "snapshot_load"
    icon: ClassVar[str] = "tabler/history"
    categories: ClassVar[list[str]] = ["web/monitor"]
    description: ClassVar[str] = "Load a previously saved crawl snapshot for comparison"
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self, input: SnapshotLoadInput, ctx: BlockContext | None = None
    ) -> SnapshotLoadOutput:
        try:
            result = await asyncio.to_thread(
                _load_snapshot, input.snapshot_id, input.which, input.storage_dir,
            )
        except Exception:
            logger.exception("snapshot_load: failed for %s", input.snapshot_id)
            return SnapshotLoadOutput(snapshot_id=input.snapshot_id)

        if ctx:
            if result.exists:
                await ctx.log(
                    f"Loaded {input.which} snapshot '{input.snapshot_id}': "
                    f"{result.page_count} pages from {result.timestamp}"
                )
            else:
                await ctx.log(f"No {input.which} snapshot found for '{input.snapshot_id}'")
        return result
