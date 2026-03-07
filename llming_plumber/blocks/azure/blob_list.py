"""List blobs in an Azure Blob Storage container."""

from __future__ import annotations

from typing import Any, ClassVar

from pydantic import Field

from llming_plumber.blocks.azure._storage import get_blob_service
from llming_plumber.blocks.base import (
    BaseBlock,
    BlockContext,
    BlockInput,
    BlockOutput,
)


class BlobListInput(BlockInput):
    connection_string: str = Field(
        default="",
        title="Connection String",
        json_schema_extra={"secret": True},
    )
    container: str = Field(
        title="Container",
        description="Blob container name",
    )
    prefix: str = Field(
        default="",
        title="Prefix",
        description=(
            "Filter blobs by name prefix (e.g. 'data/' "
            "for all blobs in the data folder)"
        ),
    )
    max_results: int = Field(
        default=100,
        title="Max Results",
        description="Maximum number of blobs to return",
        json_schema_extra={"min": 1, "max": 5000},
    )


class BlobInfo(BlockOutput):
    name: str
    size: int
    content_type: str
    last_modified: str
    etag: str


class BlobListOutput(BlockOutput):
    blobs: list[dict[str, Any]]
    count: int
    container: str


class BlobListBlock(BaseBlock[BlobListInput, BlobListOutput]):
    block_type: ClassVar[str] = "azure_blob_list"
    icon: ClassVar[str] = "tabler/list"
    categories: ClassVar[list[str]] = ["azure", "storage"]
    description: ClassVar[str] = (
        "List blobs in an Azure Blob Storage container"
    )

    async def execute(
        self,
        input: BlobListInput,
        ctx: BlockContext | None = None,
    ) -> BlobListOutput:
        service = get_blob_service(input.connection_string)
        async with service:
            container_client = service.get_container_client(
                input.container,
            )
            blobs: list[dict[str, Any]] = []
            async for blob in container_client.list_blobs(
                name_starts_with=input.prefix or None,
            ):
                if len(blobs) >= input.max_results:
                    break
                last_mod = blob.last_modified
                blobs.append({
                    "name": blob.name,
                    "size": blob.size or 0,
                    "content_type": (
                        blob.content_settings.content_type
                        if blob.content_settings
                        else "application/octet-stream"
                    ),
                    "last_modified": (
                        last_mod.isoformat() if last_mod else ""
                    ),
                    "etag": blob.etag or "",
                })

            return BlobListOutput(
                blobs=blobs,
                count=len(blobs),
                container=input.container,
            )
