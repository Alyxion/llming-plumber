from __future__ import annotations

import pytest

from llming_plumber.blocks.core.text_template import (
    TextTemplateBlock,
    TextTemplateInput,
)


async def test_simple_substitution() -> None:
    block = TextTemplateBlock()
    result = await block.execute(
        TextTemplateInput(template="Hello, {name}!", values={"name": "World"})
    )
    assert result.rendered == "Hello, World!"


async def test_multiple_fields() -> None:
    block = TextTemplateBlock()
    result = await block.execute(
        TextTemplateInput(
            template="{greeting}, {name}! You are {age}.",
            values={"greeting": "Hi", "name": "Alice", "age": 30},
        )
    )
    assert result.rendered == "Hi, Alice! You are 30."


async def test_no_placeholders() -> None:
    block = TextTemplateBlock()
    result = await block.execute(
        TextTemplateInput(template="No placeholders here.", values={})
    )
    assert result.rendered == "No placeholders here."


async def test_missing_key_raises() -> None:
    block = TextTemplateBlock()
    with pytest.raises(KeyError):
        await block.execute(
            TextTemplateInput(template="{missing}", values={})
        )


async def test_repeated_placeholder() -> None:
    block = TextTemplateBlock()
    result = await block.execute(
        TextTemplateInput(
            template="{x} and {x}", values={"x": "same"}
        )
    )
    assert result.rendered == "same and same"
