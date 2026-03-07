"""Read a blob from Azure Blob Storage."""

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


class BlobReadInput(BlockInput):
    connection_string: str = Field(
        default="",
        title="Connection String",
        description=(
            "Azure Storage connection string. "
            "Falls back to AZURE_STORAGE_CONNECTION_STRING env var."
        ),
        json_schema_extra={"secret": True},
    )
    container: str = Field(
        title="Container",
        description="Blob container name",
    )
    blob_name: str = Field(
        title="Blob Name",
        description="Path to the blob (e.g. 'data/file.json')",
    )
    encoding: str = Field(
        default="utf-8",
        title="Encoding",
        description="Text encoding. Use 'binary' for raw bytes.",
        json_schema_extra={
            "widget": "select",
            "options": ["utf-8", "ascii", "latin-1", "binary"],
        },
    )


class BlobReadOutput(BlockOutput):
    content: str
    content_length: int
    content_type: str
    blob_name: str
    container: str
    etag: str = ""
    last_modified: str = ""


class BlobReadBlock(BaseBlock[BlobReadInput, BlobReadOutput]):
    block_type: ClassVar[str] = "azure_blob_read"
    icon: ClassVar[str] = "tabler/cloud-download"
    categories: ClassVar[list[str]] = ["azure", "storage"]
    description: ClassVar[str] = (
        "Read a blob from Azure Blob Storage"
    )

    async def execute(
        self,
        input: BlobReadInput,
        ctx: BlockContext | None = None,
    ) -> BlobReadOutput:
        service = get_blob_service(input.connection_string)
        async with service:
            blob_client = service.get_blob_client(
                container=input.container,
                blob=input.blob_name,
            )
            download = await blob_client.download_blob()
            raw = await download.readall()
            props = download.properties

            if input.encoding == "binary":
                import base64

                content = base64.b64encode(raw).decode("ascii")
            else:
                content = raw.decode(input.encoding)

            last_mod = props.get("last_modified")

            return BlobReadOutput(
                content=content,
                content_length=len(raw),
                content_type=props.get(
                    "content_settings", {},
                ).get("content_type", "application/octet-stream"),
                blob_name=input.blob_name,
                container=input.container,
                etag=props.get("etag", ""),
                last_modified=(
                    last_mod.isoformat() if last_mod else ""
                ),
            )
