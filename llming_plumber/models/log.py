from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field


class RunLog(BaseModel):
    """A single log entry emitted during block execution."""

    id: str = ""
    run_id: str = ""
    lemming_id: str = ""
    block_id: str = ""
    block_type: str = ""
    ts: datetime = Field(default_factory=lambda: datetime.now(UTC))
    level: str = "info"
    msg: str = ""
    duration_ms: float | None = None
    output_summary: dict[str, Any] | None = None
