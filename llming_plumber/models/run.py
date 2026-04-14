from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class RunStatus(StrEnum):
    queued = "queued"
    running = "running"
    paused = "paused"
    completed = "completed"
    failed = "failed"
    retrying = "retrying"
    cancelled = "cancelled"


class BlockState(BaseModel):
    """Execution state of a single block within a run."""

    status: str = ""
    output: Any = None
    error: str | None = None
    duration_ms: float | None = None


class BlockLogEntry(BaseModel):
    """Compact summary of one block's execution, stored inline on the Run."""

    uid: str = ""
    block_type: str = ""
    label: str = ""
    status: str = ""
    duration_ms: float = 0
    parcel_count: int = 0
    output_summary: dict[str, Any] = Field(default_factory=dict)
    error: str | None = None


class Run(BaseModel):
    """A single execution of a pipeline."""

    id: str = ""
    pipeline_id: str = ""
    pipeline_version: int = 1
    status: RunStatus = RunStatus.queued

    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    started_at: datetime | None = None
    finished_at: datetime | None = None

    lemming_id: str | None = None
    arq_job_id: str | None = None

    current_block: str | None = None
    block_states: dict[str, BlockState] = Field(default_factory=dict)
    input: dict[str, Any] = Field(default_factory=dict)
    output: dict[str, Any] | None = None

    # Compact inline log — block-level summaries for quick review
    log: list[BlockLogEntry] = Field(default_factory=list)

    debug: bool = False

    attempt: int = 0
    max_attempts: int = 3

    error: str | None = None
    tags: list[str] = Field(default_factory=list)
    cost_report: dict[str, Any] | None = None
