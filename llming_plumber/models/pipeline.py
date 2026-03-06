from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


class BlockPosition(BaseModel):
    """Visual position of a block on the pipeline canvas."""

    x: float = 0.0
    y: float = 0.0


class BlockDefinition(BaseModel):
    """A block instance within a pipeline definition."""

    uid: str
    block_type: str
    label: str
    config: dict[str, Any] = Field(default_factory=dict)
    position: BlockPosition = Field(default_factory=BlockPosition)
    notes: str = ""


class PipeDefinition(BaseModel):
    """A connection between two block fittings in a pipeline."""

    uid: str
    source_block_uid: str
    source_fitting_uid: str
    target_block_uid: str
    target_fitting_uid: str
    field_mapping: dict[str, str] | None = None
    attachment_filter: list[str] | None = None
    transform: str | None = None


class PipelineDefinition(BaseModel):
    """A complete pipeline definition with blocks, pipes, and metadata."""

    id: str = ""
    name: str
    description: str = ""
    blocks: list[BlockDefinition] = Field(default_factory=list)
    pipes: list[PipeDefinition] = Field(default_factory=list)
    version: int = 1
    owner_id: str = ""
    owner_type: Literal["user", "team"] = "user"
    tags: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
