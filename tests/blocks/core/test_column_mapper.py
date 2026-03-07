"""Tests for the column mapper block."""

from __future__ import annotations

from llming_plumber.blocks.core.column_mapper import (
    ColumnMapperBlock,
    ColumnMapperInput,
)


async def test_basic_rename() -> None:
    block = ColumnMapperBlock()
    result = await block.execute(ColumnMapperInput(
        records=[
            {"Website URL": "https://a.com", "Name": "A"},
            {"Website URL": "https://b.com", "Name": "B"},
        ],
        mapping={"Website URL": "url"},
    ))
    assert result.record_count == 2
    assert result.records[0]["url"] == "https://a.com"
    assert result.records[0]["Name"] == "A"


async def test_defaults_for_missing() -> None:
    block = ColumnMapperBlock()
    result = await block.execute(ColumnMapperInput(
        records=[
            {"Website": "https://a.com"},
            {"Website": "https://b.com", "method": "POST"},
        ],
        mapping={"Website": "url"},
        defaults={"method": "GET"},
    ))
    assert result.records[0]["method"] == "GET"
    assert result.records[1]["method"] == "POST"


async def test_drop_unmapped() -> None:
    block = ColumnMapperBlock()
    result = await block.execute(ColumnMapperInput(
        records=[
            {"Website": "https://a.com", "Notes": "skip"},
        ],
        mapping={"Website": "url"},
        drop_unmapped=True,
    ))
    assert "url" in result.records[0]
    assert "Notes" not in result.records[0]
    assert "Website" not in result.records[0]


async def test_empty_value_uses_default() -> None:
    block = ColumnMapperBlock()
    result = await block.execute(ColumnMapperInput(
        records=[{"Website": ""}],
        mapping={"Website": "url"},
        defaults={"url": "https://fallback.com"},
    ))
    assert result.records[0]["url"] == "https://fallback.com"


async def test_missing_source_uses_default() -> None:
    block = ColumnMapperBlock()
    result = await block.execute(ColumnMapperInput(
        records=[{"other": "value"}],
        mapping={"Website": "url"},
        defaults={"url": "https://default.com"},
    ))
    assert result.records[0]["url"] == "https://default.com"


async def test_mapped_columns_output() -> None:
    block = ColumnMapperBlock()
    result = await block.execute(ColumnMapperInput(
        records=[{"a": 1, "b": 2}],
        mapping={"a": "x", "b": "y"},
    ))
    assert set(result.mapped_columns) == {"x", "y"}


async def test_empty_records() -> None:
    block = ColumnMapperBlock()
    result = await block.execute(ColumnMapperInput(
        records=[],
        mapping={"a": "b"},
    ))
    assert result.record_count == 0
    assert result.records == []
