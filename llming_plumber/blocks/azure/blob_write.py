"""Write (upload) a blob to Azure Blob Storage."""

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


class BlobWriteInput(BlockInput):
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
        description="Destination path (e.g. 'output/result.json')",
    )
    content: str = Field(
        title="Content",
        description="Data to upload",
        json_schema_extra={"widget": "textarea"},
    )
    content_type: str = Field(
        default="application/octet-stream",
        title="Content Type",
        json_schema_extra={
            "widget": "select",
            "options": [
                "application/json",
                "text/plain",
                "text/csv",
                "application/octet-stream",
            ],
        },
    )
    overwrite: bool = Field(
        default=True,
        title="Overwrite",
        description="Overwrite if blob already exists",
    )
    encoding: str = Field(
        default="utf-8",
        title="Encoding",
        description="Text encoding. Use 'binary' for base64 input.",
        json_schema_extra={
            "widget": "select",
            "options": ["utf-8", "ascii", "latin-1", "binary"],
        },
    )


class BlobWriteOutput(BlockOutput):
    blob_name: str
    container: str
    etag: str
    content_length: int
    url: str


class BlobWriteBlock(BaseBlock[BlobWriteInput, BlobWriteOutput]):
    block_type: ClassVar[str] = "azure_blob_write"
    icon: ClassVar[str] = "tabler/cloud-upload"
    categories: ClassVar[list[str]] = ["azure", "storage"]
    description: ClassVar[str] = (
        "Upload data to Azure Blob Storage"
    )

    async def execute(
        self,
        input: BlobWriteInput,
        ctx: BlockContext | None = None,
    ) -> BlobWriteOutput:
        if input.encoding == "binary":
            import base64

            data = base64.b64decode(input.content)
        else:
            data = input.content.encode(input.encoding)

        service = get_blob_service(input.connection_string)
        async with service:
            from azure.storage.blob import ContentSettings

            blob_client = service.get_blob_client(
                container=input.container,
                blob=input.blob_name,
            )
            result = await blob_client.upload_blob(
                data,
                overwrite=input.overwrite,
                content_settings=ContentSettings(
                    content_type=input.content_type,
                ),
            )

            return BlobWriteOutput(
                blob_name=input.blob_name,
                container=input.container,
                etag=result.get("etag", ""),
                content_length=len(data),
                url=blob_client.url,
            )
