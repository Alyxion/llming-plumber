"""Unit tests for UniteBlock — fan-in barrier that merges upstream results."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from llming_plumber.blocks.core.unite import UniteBlock, UniteInput
from llming_plumber.blocks.limits import ResourceLimitError


# ── All items succeed ──


@pytest.mark.asyncio
async def test_all_succeed() -> None:
    block = UniteBlock()
    result = await block.execute(
        UniteInput(items=[{"value": 1}, {"value": 2}, {"value": 3}]),
    )
    assert result.items == [{"value": 1}, {"value": 2}, {"value": 3}]
    assert result.succeeded == 3
    assert result.failed == 0
    assert result.errors == []
    assert result.all_ok is True


# ── Some items fail with require_all=True ──


@pytest.mark.asyncio
async def test_some_fail_require_all_raises() -> None:
    block = UniteBlock()
    items = [
        {"value": 1},
        {"_error": True, "_block_uid": "block_a", "_message": "timeout"},
        {"value": 2},
    ]
    with pytest.raises(ValueError, match="1 upstream block.*require_all=True.*block_a"):
        await block.execute(UniteInput(items=items, require_all=True))


@pytest.mark.asyncio
async def test_multiple_fail_require_all_lists_all() -> None:
    block = UniteBlock()
    items = [
        {"_error": True, "_block_uid": "b1", "_message": "err1"},
        {"_error": True, "_block_uid": "b2", "_message": "err2"},
    ]
    with pytest.raises(ValueError, match="2 upstream block.*b1.*b2"):
        await block.execute(UniteInput(items=items, require_all=True))


# ── Some items fail with require_all=False ──


@pytest.mark.asyncio
async def test_some_fail_require_all_false() -> None:
    block = UniteBlock()
    items = [
        {"value": 1},
        {"_error": True, "_block_uid": "block_x", "_message": "broken"},
        {"value": 2},
    ]
    result = await block.execute(
        UniteInput(items=items, require_all=False),
    )
    assert result.items == [{"value": 1}, {"value": 2}]
    assert result.succeeded == 2
    assert result.failed == 1
    assert result.errors == [{"block_uid": "block_x", "error": "broken"}]
    assert result.all_ok is False


# ── Empty items list ──


@pytest.mark.asyncio
async def test_empty_items() -> None:
    block = UniteBlock()
    result = await block.execute(UniteInput(items=[]))
    assert result.items == []
    assert result.succeeded == 0
    assert result.failed == 0
    assert result.errors == []
    assert result.all_ok is True


# ── All items fail ──


@pytest.mark.asyncio
async def test_all_fail_require_all_raises() -> None:
    block = UniteBlock()
    items = [
        {"_error": True, "_block_uid": "a", "_message": "fail1"},
        {"_error": True, "_block_uid": "b", "_message": "fail2"},
    ]
    with pytest.raises(ValueError, match="2 upstream block"):
        await block.execute(UniteInput(items=items, require_all=True))


@pytest.mark.asyncio
async def test_all_fail_require_all_false() -> None:
    block = UniteBlock()
    items = [
        {"_error": True, "_block_uid": "a", "_message": "fail1"},
        {"_error": True, "_block_uid": "b", "_message": "fail2"},
    ]
    result = await block.execute(
        UniteInput(items=items, require_all=False),
    )
    assert result.items == []
    assert result.succeeded == 0
    assert result.failed == 2
    assert result.all_ok is False
    assert len(result.errors) == 2


# ── Error items with missing _block_uid / _message ──


@pytest.mark.asyncio
async def test_error_missing_block_uid_defaults_unknown() -> None:
    block = UniteBlock()
    items = [{"_error": True}]
    result = await block.execute(
        UniteInput(items=items, require_all=False),
    )
    assert result.errors == [
        {"block_uid": "unknown", "error": "upstream block failed"},
    ]
    assert result.failed == 1


@pytest.mark.asyncio
async def test_error_missing_message_defaults() -> None:
    block = UniteBlock()
    items = [{"_error": True, "_block_uid": "block_z"}]
    result = await block.execute(
        UniteInput(items=items, require_all=False),
    )
    assert result.errors == [
        {"block_uid": "block_z", "error": "upstream block failed"},
    ]


@pytest.mark.asyncio
async def test_error_missing_block_uid_require_all_raises() -> None:
    block = UniteBlock()
    items = [{"_error": True}]
    with pytest.raises(ValueError, match="unknown"):
        await block.execute(UniteInput(items=items, require_all=True))


# ── List size check ──


@pytest.mark.asyncio
async def test_check_list_size_called() -> None:
    block = UniteBlock()
    with patch(
        "llming_plumber.blocks.core.unite.check_list_size",
        side_effect=ResourceLimitError("Unite items has 200,000 entries"),
    ) as mock_check:
        with pytest.raises(ResourceLimitError, match="Unite items"):
            await block.execute(UniteInput(items=[{"x": 1}]))
        mock_check.assert_called_once()


# ── Block metadata ──


def test_block_type() -> None:
    assert UniteBlock.block_type == "unite"


def test_fan_in() -> None:
    assert UniteBlock.fan_in is True


def test_tolerate_upstream_errors() -> None:
    assert UniteBlock.tolerate_upstream_errors is True


def test_categories() -> None:
    assert "core/flow" in UniteBlock.categories


def test_require_all_defaults_true() -> None:
    inp = UniteInput()
    assert inp.require_all is True
    assert inp.items == []
