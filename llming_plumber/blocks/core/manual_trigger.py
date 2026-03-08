"""Manual trigger block — entry point for manually-run pipelines.

Unlike the timer trigger, this block has no scheduling capabilities.
It emits the current timestamp and any custom test data provided
in the block config, making it ideal for testing and one-off runs.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, ClassVar

from pydantic import ConfigDict, Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput


class ManualTriggerInput(BlockInput):
    model_config = ConfigDict(extra="allow")

    label: str = Field(
        default="Manual Run",
        title="Label",
        description="Descriptive label for this trigger",
    )
    test_data: str = Field(
        default="",
        title="Test Data (JSON)",
        description="Optional JSON object with test fields to emit as output",
        json_schema_extra={"widget": "textarea", "rows": 4},
    )


class ManualTriggerOutput(BlockOutput):
    model_config = ConfigDict(extra="allow")

    triggered_at: str = ""
    date: str = ""
    time: str = ""
    weekday: str = ""
    hour: int = 0
    minute: int = 0
    label: str = ""


class ManualTriggerBlock(BaseBlock[ManualTriggerInput, ManualTriggerOutput]):
    block_type: ClassVar[str] = "manual_trigger"
    icon: ClassVar[str] = "tabler/player-play"
    categories: ClassVar[list[str]] = ["core/trigger"]
    description: ClassVar[str] = (
        "Manual trigger — run pipelines on demand with optional test data"
    )
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self,
        input: ManualTriggerInput,
        ctx: BlockContext | None = None,
    ) -> ManualTriggerOutput:
        import json

        now = datetime.now(UTC)

        # Parse optional test data
        extra: dict[str, Any] = {}
        if input.test_data.strip():
            try:
                parsed = json.loads(input.test_data)
                if isinstance(parsed, dict):
                    extra = parsed
            except json.JSONDecodeError:
                pass

        return ManualTriggerOutput(
            triggered_at=now.isoformat(),
            date=now.strftime("%Y-%m-%d"),
            time=now.strftime("%H:%M:%S"),
            weekday=now.strftime("%A"),
            hour=now.hour,
            minute=now.minute,
            label=input.label,
            **extra,
        )
