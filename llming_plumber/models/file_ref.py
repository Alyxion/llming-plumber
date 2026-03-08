"""FileRef — first-class file type for block I/O.

A ``FileRef`` is a self-describing file envelope that carries metadata
(filename, MIME type, size) alongside the content (base64-encoded) or
a storage reference for externalized blobs.  Blocks that produce or
consume files use ``FileRef`` in their input/output schemas so that
files flow through pipes with full type information.

Conversion helpers allow round-tripping between ``FileRef`` and the
lower-level ``Attachment`` model used on ``Parcel`` transport envelopes.
"""

from __future__ import annotations

import base64
import mimetypes
from typing import Any, Self

from pydantic import BaseModel, Field, model_validator

from llming_plumber.blocks.limits import (
    check_base64_size,
    check_file_size,
)


class FileRef(BaseModel):
    """A self-describing file that flows between blocks."""

    filename: str = Field(
        default="",
        description="Original filename including extension",
    )
    mime_type: str = Field(
        default="application/octet-stream",
        description="MIME type (auto-detected from extension when empty)",
    )
    size_bytes: int = Field(
        default=0,
        description="Size of the decoded content in bytes",
    )
    data: str = Field(
        default="",
        description="Base64-encoded file content",
    )
    storage_id: str = Field(
        default="",
        description="External storage reference (e.g. 'redis:key' or 'gridfs:id')",
    )

    @model_validator(mode="after")
    def _fill_defaults(self) -> Self:
        """Auto-detect MIME type from extension and compute size."""
        if self.filename and self.mime_type == "application/octet-stream":
            guessed, _ = mimetypes.guess_type(self.filename)
            if guessed:
                self.mime_type = guessed
        if self.data and self.size_bytes == 0:
            # Estimate decoded size without decoding
            n = len(self.data)
            padding = self.data.count("=") if n else 0
            self.size_bytes = (n * 3) // 4 - padding
        return self

    # ---- Constructors ----

    @classmethod
    def from_bytes(
        cls,
        raw: bytes,
        filename: str,
        mime_type: str = "",
    ) -> FileRef:
        """Create a FileRef from raw bytes."""
        check_file_size(len(raw), label=filename or "file")
        mt = mime_type
        if not mt and filename:
            guessed, _ = mimetypes.guess_type(filename)
            mt = guessed or "application/octet-stream"
        return cls(
            filename=filename,
            mime_type=mt or "application/octet-stream",
            size_bytes=len(raw),
            data=base64.b64encode(raw).decode("ascii"),
        )

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> FileRef:
        """Coerce a dict with file-like keys into a FileRef.

        Accepts both FileRef-native keys and legacy formats like
        ``{name, content_base64, size_bytes}`` from archive blocks.
        """
        if "filename" in d and ("data" in d or "storage_id" in d):
            return cls(**{k: v for k, v in d.items() if k in cls.model_fields})
        # Legacy archive format
        if "name" in d and "content_base64" in d:
            return cls(
                filename=d["name"],
                data=d["content_base64"],
                size_bytes=d.get("size_bytes", 0),
            )
        msg = "Cannot coerce dict to FileRef: needs 'filename'+'data' or 'name'+'content_base64'"
        raise ValueError(msg)

    # ---- Accessors ----

    def decode(self) -> bytes:
        """Decode the base64 data and return raw bytes.

        Validates size limits before decoding.
        """
        if not self.data:
            return b""
        check_base64_size(self.data, label=self.filename or "file")
        return base64.b64decode(self.data)

    @property
    def extension(self) -> str:
        """File extension including the dot, e.g. '.pdf'."""
        if "." in self.filename:
            return "." + self.filename.rsplit(".", 1)[-1].lower()
        return ""

    def human_size(self) -> str:
        """Human-readable file size."""
        b = self.size_bytes
        for unit in ("B", "KB", "MB", "GB"):
            if b < 1024:
                return f"{b:.1f} {unit}" if unit != "B" else f"{b} {unit}"
            b /= 1024
        return f"{b:.1f} TB"


# ------------------------------------------------------------------
# Conversion: FileRef <-> Attachment
# ------------------------------------------------------------------


def file_ref_from_attachment(att: Any) -> FileRef:
    """Convert an Attachment to a FileRef."""
    return FileRef(
        filename=att.filename,
        mime_type=att.mime_type,
        size_bytes=att.size_bytes,
        data=att.data_b64 or "",
        storage_id=att.storage_ref or "",
    )


def file_ref_to_attachment(ref: FileRef, uid: str = "") -> Any:
    """Convert a FileRef to an Attachment."""
    from llming_plumber.models.parcel import Attachment

    return Attachment(
        uid=uid or ref.filename,
        filename=ref.filename,
        mime_type=ref.mime_type,
        size_bytes=ref.size_bytes,
        data_b64=ref.data or None,
        storage_ref=ref.storage_id or None,
    )
