"""Trigger block that detects new or modified blobs.

Polls a container and compares against a stored state (etags/timestamps)
to detect additions, modifications, and deletions since the last check.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, ClassVar

from pydantic import Field

from llming_plumber.blocks.azure._storage import get_blob_service
from llming_plumber.blocks.base import (
    BaseBlock,
    BlockContext,
    BlockInput,
    BlockOutput,
)


class BlobTriggerInput(BlockInput):
    connection_string: str = Field(
        default="",
        title="Connection String",
        json_schema_extra={"secret": True},
    )
    container: str = Field(
        title="Container",
        description="Container to monitor",
    )
    prefix: str = Field(
        default="",
        title="Prefix",
        description="Only monitor blobs with this prefix",
    )
    events: list[str] = Field(
        default=["created", "modified"],
        title="Events",
        description="Which events to detect",
        json_schema_extra={
            "widget": "select",
            "options": ["created", "modified", "deleted"],
        },
    )
    previous_state: dict[str, str] = Field(
        default_factory=dict,
        title="Previous State",
        description=(
            "Mapping of blob_name -> etag from the last poll. "
            "Normally injected by the scheduler."
        ),
    )


class BlobEvent(BlockOutput):
    blob_name: str
    event: str  # "created" | "modified" | "deleted"
    etag: str
    size: int
    last_modified: str
    content_type: str


class BlobTriggerOutput(BlockOutput):
    events: list[dict[str, Any]]
    current_state: dict[str, str]
    checked_at: str
    container: str


class BlobTriggerBlock(
    BaseBlock[BlobTriggerInput, BlobTriggerOutput],
):
    block_type: ClassVar[str] = "azure_blob_trigger"
    icon: ClassVar[str] = "tabler/bell-ringing"
    categories: ClassVar[list[str]] = ["azure", "storage", "trigger"]
    description: ClassVar[str] = (
        "Detect new, modified, or deleted blobs in a container"
    )

    async def execute(
        self,
        input: BlobTriggerInput,
        ctx: BlockContext | None = None,
    ) -> BlobTriggerOutput:
        service = get_blob_service(input.connection_string)
        async with service:
            container_client = service.get_container_client(
                input.container,
            )

            # Snapshot current state
            current: dict[str, dict[str, Any]] = {}
            async for blob in container_client.list_blobs(
                name_starts_with=input.prefix or None,
            ):
                last_mod = blob.last_modified
                current[blob.name] = {
                    "etag": blob.etag or "",
                    "size": blob.size or 0,
                    "content_type": (
                        blob.content_settings.content_type
                        if blob.content_settings
                        else "application/octet-stream"
                    ),
                    "last_modified": (
                        last_mod.isoformat() if last_mod else ""
                    ),
                }

            prev = input.previous_state
            detected: list[dict[str, Any]] = []

            # Detect created and modified
            for name, info in current.items():
                if name not in prev:
                    if "created" in input.events:
                        detected.append({
                            "blob_name": name,
                            "event": "created",
                            **info,
                        })
                elif info["etag"] != prev[name]:
                    if "modified" in input.events:
                        detected.append({
                            "blob_name": name,
                            "event": "modified",
                            **info,
                        })

            # Detect deleted
            if "deleted" in input.events:
                for name in prev:
                    if name not in current:
                        detected.append({
                            "blob_name": name,
                            "event": "deleted",
                            "etag": prev[name],
                            "size": 0,
                            "content_type": "",
                            "last_modified": "",
                        })

            # Build new state map (name -> etag)
            new_state = {
                name: info["etag"] for name, info in current.items()
            }

            return BlobTriggerOutput(
                events=detected,
                current_state=new_state,
                checked_at=datetime.now(UTC).isoformat(),
                container=input.container,
            )
