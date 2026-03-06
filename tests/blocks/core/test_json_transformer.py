from __future__ import annotations

from llming_plumber.blocks.core.json_transformer import (
    JsonTransformerBlock,
    JsonTransformerInput,
)


async def test_rename_fields() -> None:
    block = JsonTransformerBlock()
    result = await block.execute(
        JsonTransformerInput(
            data={"name": "Alice", "age": 30},
            rename={"name": "full_name"},
        )
    )
    assert result.data == {"full_name": "Alice", "age": 30}


async def test_keep_only() -> None:
    block = JsonTransformerBlock()
    result = await block.execute(
        JsonTransformerInput(
            data={"a": 1, "b": 2, "c": 3},
            keep_only=["a", "c"],
        )
    )
    assert result.data == {"a": 1, "c": 3}


async def test_remove_fields() -> None:
    block = JsonTransformerBlock()
    result = await block.execute(
        JsonTransformerInput(
            data={"a": 1, "b": 2, "c": 3},
            remove=["b"],
        )
    )
    assert result.data == {"a": 1, "c": 3}


async def test_rename_then_keep_only() -> None:
    block = JsonTransformerBlock()
    result = await block.execute(
        JsonTransformerInput(
            data={"old": "value", "other": "x"},
            rename={"old": "new"},
            keep_only=["new"],
        )
    )
    assert result.data == {"new": "value"}


async def test_no_transforms() -> None:
    block = JsonTransformerBlock()
    result = await block.execute(
        JsonTransformerInput(data={"x": 1})
    )
    assert result.data == {"x": 1}


async def test_remove_nonexistent_key_is_safe() -> None:
    block = JsonTransformerBlock()
    result = await block.execute(
        JsonTransformerInput(
            data={"a": 1},
            remove=["nonexistent"],
        )
    )
    assert result.data == {"a": 1}
