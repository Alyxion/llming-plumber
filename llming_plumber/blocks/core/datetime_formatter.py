"""Parse and reformat datetime strings."""

from __future__ import annotations

from datetime import datetime
from typing import ClassVar

from pydantic import Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput


class DatetimeFormatterInput(BlockInput):
    date_string: str = Field(
        title="Date String",
        description="The date/time string to parse and reformat",
        json_schema_extra={"placeholder": "2026-03-06T14:00:00"},
    )
    input_format: str = Field(
        default="%Y-%m-%dT%H:%M:%S",
        title="Input Format",
        description="The strftime format of the input date string",
        json_schema_extra={"placeholder": "%Y-%m-%dT%H:%M:%S"},
    )
    output_format: str = Field(
        default="%Y-%m-%d",
        title="Output Format",
        description="The strftime format for the output date string",
        json_schema_extra={"placeholder": "%Y-%m-%d"},
    )


class DatetimeFormatterOutput(BlockOutput):
    formatted: str
    iso: str


class DatetimeFormatterBlock(
    BaseBlock[DatetimeFormatterInput, DatetimeFormatterOutput]
):
    block_type: ClassVar[str] = "datetime_formatter"
    icon: ClassVar[str] = "tabler/calendar-time"
    categories: ClassVar[list[str]] = ["core/transform"]
    description: ClassVar[str] = "Parse and reformat date/time strings"
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self, input: DatetimeFormatterInput, ctx: BlockContext | None = None
    ) -> DatetimeFormatterOutput:
        dt = datetime.strptime(input.date_string, input.input_format)  # noqa: DTZ007
        return DatetimeFormatterOutput(
            formatted=dt.strftime(input.output_format),
            iso=dt.isoformat(),
        )
