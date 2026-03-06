from __future__ import annotations

import pytest

from llming_plumber.blocks.core.datetime_formatter import (
    DatetimeFormatterBlock,
    DatetimeFormatterInput,
    DatetimeFormatterOutput,
)


async def test_default_formats() -> None:
    block = DatetimeFormatterBlock()
    result = await block.execute(
        DatetimeFormatterInput(date_string="2024-06-15T10:30:00")
    )
    assert isinstance(result, DatetimeFormatterOutput)
    assert result.formatted == "2024-06-15"
    assert result.iso == "2024-06-15T10:30:00"


async def test_custom_output_format() -> None:
    block = DatetimeFormatterBlock()
    result = await block.execute(
        DatetimeFormatterInput(
            date_string="2024-06-15T10:30:00",
            output_format="%d/%m/%Y",
        )
    )
    assert result.formatted == "15/06/2024"


async def test_custom_input_format() -> None:
    block = DatetimeFormatterBlock()
    result = await block.execute(
        DatetimeFormatterInput(
            date_string="15/06/2024 10:30",
            input_format="%d/%m/%Y %H:%M",
            output_format="%Y-%m-%d",
        )
    )
    assert result.formatted == "2024-06-15"
    assert result.iso == "2024-06-15T10:30:00"


async def test_invalid_date_raises() -> None:
    block = DatetimeFormatterBlock()
    with pytest.raises(ValueError):
        await block.execute(
            DatetimeFormatterInput(date_string="not-a-date")
        )


async def test_block_type() -> None:
    assert DatetimeFormatterBlock.block_type == "datetime_formatter"
    assert DatetimeFormatterBlock.cache_ttl == 0
