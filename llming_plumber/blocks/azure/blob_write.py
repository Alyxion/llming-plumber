"""Write (upload) a blob to Azure Blob Storage."""

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
from llming_plumber.models.file_ref import FileRef


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
    file_ref: dict[str, Any] | None = Field(
        default=None,
        title="File Reference",
        description="FileRef object (alternative to content string)",
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
        if input.file_ref and not input.content:
            ref = FileRef(**input.file_ref) if isinstance(input.file_ref, dict) else input.file_ref
            data = ref.decode()
            blob_name = input.blob_name or ref.filename
            content_type = (
                input.content_type
                if input.content_type != "application/octet-stream"
                else ref.mime_type
            )
        elif input.encoding == "binary":
            import base64

            data = base64.b64decode(input.content)
            blob_name = input.blob_name
            content_type = input.content_type
        else:
            data = input.content.encode(input.encoding)
            blob_name = input.blob_name
            content_type = input.content_type

        service = get_blob_service(input.connection_string)
        async with service:
            from azure.storage.blob import ContentSettings

            blob_client = service.get_blob_client(
                container=input.container,
                blob=blob_name,
            )
            result = await blob_client.upload_blob(
                data,
                overwrite=input.overwrite,
                content_settings=ContentSettings(
                    content_type=content_type,
                ),
            )

            return BlobWriteOutput(
                blob_name=blob_name,
                container=input.container,
                etag=result.get("etag", ""),
                content_length=len(data),
                url=blob_client.url,
            )
