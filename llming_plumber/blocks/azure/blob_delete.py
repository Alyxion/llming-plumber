"""Delete a blob from Azure Blob Storage."""

from __future__ import annotations

from typing import ClassVar

from pydantic import Field

from llming_plumber.blocks.azure._storage import get_blob_service
from llming_plumber.blocks.base import (
    BaseBlock,
    BlockContext,
    BlockInput,
    BlockOutput,
)


class BlobDeleteInput(BlockInput):
    connection_string: str = Field(
        default="",
        title="Connection String",
        json_schema_extra={"secret": True},
    )
    container: str = Field(
        title="Container",
        description="Blob container name",
    )
    blob_name: str = Field(
        title="Blob Name",
        description="Path of the blob to delete",
    )
    delete_snapshots: str = Field(
        default="include",
        title="Delete Snapshots",
        description="How to handle snapshots",
        json_schema_extra={
            "widget": "select",
            "options": ["include", "only"],
        },
    )


class BlobDeleteOutput(BlockOutput):
    blob_name: str
    container: str
    deleted: bool


class BlobDeleteBlock(
    BaseBlock[BlobDeleteInput, BlobDeleteOutput],
):
    block_type: ClassVar[str] = "azure_blob_delete"
    icon: ClassVar[str] = "tabler/trash"
    categories: ClassVar[list[str]] = ["azure", "storage"]
    description: ClassVar[str] = (
        "Delete a blob from Azure Blob Storage"
    )

    async def execute(
        self,
        input: BlobDeleteInput,
        ctx: BlockContext | None = None,
    ) -> BlobDeleteOutput:
        service = get_blob_service(input.connection_string)
        async with service:
            blob_client = service.get_blob_client(
                container=input.container,
                blob=input.blob_name,
            )
            await blob_client.delete_blob(
                delete_snapshots=input.delete_snapshots,
            )

            return BlobDeleteOutput(
                blob_name=input.blob_name,
                container=input.container,
                deleted=True,
            )
