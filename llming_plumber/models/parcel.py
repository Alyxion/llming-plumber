from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class Attachment(BaseModel):
    """A binary payload attached to a parcel."""

    uid: str
    filename: str
    mime_type: str
    size_bytes: int
    data_b64: str | None = None
    storage_ref: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class Parcel(BaseModel):
    """A single unit of data flowing through a pipeline."""

    uid: str
    fields: dict[str, Any] = Field(default_factory=dict)
    attachments: list[Attachment] = Field(default_factory=list)
