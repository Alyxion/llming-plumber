"""Unit tests for ManualTriggerBlock."""

from __future__ import annotations

from llming_plumber.blocks.core.manual_trigger import (
    ManualTriggerBlock,
    ManualTriggerInput,
    ManualTriggerOutput,
)


async def test_basic_trigger() -> None:
    block = ManualTriggerBlock()
    result = await block.execute(ManualTriggerInput(label="test"))
    assert isinstance(result, ManualTriggerOutput)
    assert result.triggered_at
    assert result.date
    assert result.time
    assert result.weekday
    assert result.label == "test"


async def test_with_test_data() -> None:
    block = ManualTriggerBlock()
    result = await block.execute(
        ManualTriggerInput(
            label="file test",
            test_data='{"filename": "demo.csv", "rows": 100}',
        )
    )
    d = result.model_dump()
    assert d["filename"] == "demo.csv"
    assert d["rows"] == 100
    assert d["label"] == "file test"


async def test_invalid_json_ignored() -> None:
    block = ManualTriggerBlock()
    result = await block.execute(
        ManualTriggerInput(test_data="not json")
    )
    assert result.triggered_at  # still works
    d = result.model_dump()
    assert "not json" not in d.values()


async def test_non_dict_json_ignored() -> None:
    block = ManualTriggerBlock()
    result = await block.execute(
        ManualTriggerInput(test_data="[1, 2, 3]")
    )
    assert result.triggered_at


async def test_empty_test_data() -> None:
    block = ManualTriggerBlock()
    result = await block.execute(ManualTriggerInput(test_data=""))
    d = result.model_dump()
    assert "triggered_at" in d


def test_block_type() -> None:
    assert ManualTriggerBlock.block_type == "manual_trigger"


def test_categories() -> None:
    assert "core/trigger" in ManualTriggerBlock.categories
