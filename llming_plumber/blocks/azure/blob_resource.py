"""Azure Blob Storage resource block — defines a storage target.

This is a **resource block** (``block_kind = "resource"``).  It is not
executed as a pipeline step.  Instead, the executor reads its config and
creates an :class:`AzureBlobSink` that connected action blocks use for
streaming writes.

Supports:
- Templatable ``base_path`` for organized storage
- Per-blob ``expires_at`` metadata for lifecycle cleanup
- Streaming writes — one blob per call, no buffering
"""

from __future__ import annotations

import logging
import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any, ClassVar

from pydantic import Field

from llming_plumber.blocks.base import (
    BaseBlock,
    BlockContext,
    BlockInput,
    BlockOutput,
    FileInfo,
    Sink,
)

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Sink implementation
# ------------------------------------------------------------------


class AzureBlobSink(Sink):
    """Streams files to Azure Blob Storage one at a time."""

    def __init__(
        self,
        connection_string: str,
        container: str,
        base_path: str,
        retention_days: int,
    ) -> None:
        self._conn_str = connection_string or os.environ.get(
            "AZURE_STORAGE_CONNECTION_STRING", "",
        )
        self._container = container
        self._base_path = base_path.rstrip("/")
        self._retention_days = retention_days
        self._files_written = 0
        self._total_bytes = 0
        self._service: Any = None

    async def _get_service(self) -> Any:
        if self._service is None:
            from azure.storage.blob.aio import BlobServiceClient

            self._service = BlobServiceClient.from_connection_string(
                self._conn_str,
            )
        return self._service

    async def read(self, path: str) -> bytes | None:
        """Download a blob by relative path.  Returns None if not found."""
        service = await self._get_service()
        full_path = f"{self._base_path}/{path}" if self._base_path else path
        blob_client = service.get_blob_client(
            container=self._container, blob=full_path,
        )
        try:
            stream = await blob_client.download_blob()
            return await stream.readall()
        except Exception:
            return None

    async def list(  # type: ignore[override]
        self,
        prefix: str = "",
        pattern: str = "*",
    ) -> AsyncIterator[FileInfo]:
        """List blobs under base_path/prefix matching the glob pattern."""
        import fnmatch

        service = await self._get_service()
        container_client = service.get_container_client(self._container)
        full_prefix = (
            f"{self._base_path}/{prefix}" if self._base_path else prefix
        )
        full_prefix = full_prefix.rstrip("/")
        if full_prefix:
            full_prefix += "/"

        async for blob in container_client.list_blobs(
            name_starts_with=full_prefix,
        ):
            name: str = blob.name
            filename = name.rsplit("/", 1)[-1]
            if not filename:
                continue
            if pattern != "*" and not fnmatch.fnmatch(filename, pattern):
                continue
            # Relative path (strip base_path prefix)
            if self._base_path and name.startswith(self._base_path):
                rel_path = name[len(self._base_path) :].lstrip("/")
            else:
                rel_path = name
            cs = getattr(blob, "content_settings", None)
            ct = cs.content_type if cs and hasattr(cs, "content_type") else ""
            lm = getattr(blob, "last_modified", None)
            yield FileInfo(
                path=rel_path,
                filename=filename,
                size_bytes=getattr(blob, "size", 0) or 0,
                modified_iso=lm.isoformat() if lm else "",
                content_type=ct or "",
            )

    async def write(
        self,
        path: str,
        content: str | bytes,
        *,
        content_type: str = "",
        metadata: dict[str, str] | None = None,
    ) -> None:
        """Upload a single blob.  Path is relative to base_path."""
        service = await self._get_service()
        full_path = f"{self._base_path}/{path}" if self._base_path else path

        data = content.encode("utf-8") if isinstance(content, str) else content

        blob_meta = dict(metadata) if metadata else {}
        if self._retention_days > 0:
            expires = datetime.now(UTC) + timedelta(days=self._retention_days)
            blob_meta["expires_at"] = expires.isoformat()

        from azure.storage.blob import ContentSettings

        ct = content_type or self._guess_content_type(path)
        blob_client = service.get_blob_client(
            container=self._container, blob=full_path,
        )
        await blob_client.upload_blob(
            data,
            overwrite=True,
            content_settings=ContentSettings(content_type=ct),
            metadata=blob_meta if blob_meta else None,
        )
        self._files_written += 1
        self._total_bytes += len(data)

    async def finalize(self) -> dict[str, Any]:
        """Close the service client and return summary."""
        if self._service is not None:
            await self._service.close()
            self._service = None
        return {
            "files_written": self._files_written,
            "total_bytes": self._total_bytes,
            "base_path": self._base_path,
            "container": self._container,
            "retention_days": self._retention_days,
        }

    @staticmethod
    def _guess_content_type(path: str) -> str:
        p = path.lower()
        if p.endswith(".html") or p.endswith(".htm"):
            return "text/html"
        if p.endswith(".txt"):
            return "text/plain"
        if p.endswith(".json"):
            return "application/json"
        if p.endswith(".csv"):
            return "text/csv"
        if p.endswith(".xml"):
            return "application/xml"
        if p.endswith(".pdf"):
            return "application/pdf"
        return "application/octet-stream"


# ------------------------------------------------------------------
# Resource block
# ------------------------------------------------------------------


class AzureBlobResourceInput(BlockInput):
    connection_string: str = Field(
        default="",
        title="Connection String",
        description="Azure Storage connection string (falls back to env var)",
        json_schema_extra={"secret": True},
    )
    container: str = Field(
        title="Container",
        description="Blob container name",
        json_schema_extra={"placeholder": "plumber-crawls"},
    )
    base_path: str = Field(
        default="",
        title="Base Path",
        description=(
            "Path prefix for all blobs.  Supports {date}, {run_id}, etc."
        ),
        json_schema_extra={"placeholder": "crawls/{date}"},
    )
    retention_days: int = Field(
        default=60,
        title="Retention (days)",
        description="Auto-delete blobs after N days (0 = no expiry). "
        "Set via blob metadata; requires a lifecycle policy on the container.",
        json_schema_extra={"min": 0, "max": 3650},
    )


class AzureBlobResourceOutput(BlockOutput):
    """Summary produced after all connected action blocks finish."""

    files_written: int = 0
    total_bytes: int = 0
    base_path: str = ""
    container: str = ""
    retention_days: int = 0


class AzureBlobResourceBlock(
    BaseBlock[AzureBlobResourceInput, AzureBlobResourceOutput],
):
    block_type: ClassVar[str] = "azure_blob_resource"
    block_kind: ClassVar[str] = "resource"
    icon: ClassVar[str] = "tabler/cloud"
    categories: ClassVar[list[str]] = ["azure", "storage"]
    description: ClassVar[str] = (
        "Azure Blob Storage target — stream files from connected blocks"
    )

    async def execute(
        self,
        input: AzureBlobResourceInput,
        ctx: BlockContext | None = None,
    ) -> AzureBlobResourceOutput:
        # Resource blocks are not executed by the pipeline executor.
        # This is only called if someone runs the block standalone.
        return AzureBlobResourceOutput(
            container=input.container,
            base_path=input.base_path,
            retention_days=input.retention_days,
        )

    def create_sink(self, resolved_config: dict[str, Any]) -> AzureBlobSink:
        return AzureBlobSink(
            connection_string=resolved_config.get("connection_string", ""),
            container=resolved_config.get("container", ""),
            base_path=resolved_config.get("base_path", ""),
            retention_days=int(resolved_config.get("retention_days", 60)),
        )
