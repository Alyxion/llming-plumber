from __future__ import annotations

from llming_plumber.blocks.core.jsonpath import (
    JsonPathBlock,
    JsonPathInput,
    JsonPathOutput,
)

SAMPLE_DATA = {
    "store": {
        "books": [
            {"title": "Foo", "price": 10},
            {"title": "Bar", "price": 20},
            {"title": "Baz", "price": 30},
        ],
        "name": "MyStore",
    }
}


async def test_extract_single_value() -> None:
    block = JsonPathBlock()
    result = await block.execute(
        JsonPathInput(data=SAMPLE_DATA, expression="$.store.name")
    )
    assert isinstance(result, JsonPathOutput)
    assert result.values == ["MyStore"]
    assert result.match_count == 1


async def test_extract_nested_array() -> None:
    block = JsonPathBlock()
    result = await block.execute(
        JsonPathInput(data=SAMPLE_DATA, expression="$.store.books[*].title")
    )
    assert result.values == ["Foo", "Bar", "Baz"]
    assert result.match_count == 3


async def test_extract_numeric_values() -> None:
    block = JsonPathBlock()
    result = await block.execute(
        JsonPathInput(data=SAMPLE_DATA, expression="$.store.books[*].price")
    )
    assert result.values == [10, 20, 30]
    assert result.match_count == 3


async def test_no_matches() -> None:
    block = JsonPathBlock()
    result = await block.execute(
        JsonPathInput(data=SAMPLE_DATA, expression="$.store.nonexistent")
    )
    assert result.values == []
    assert result.match_count == 0


async def test_single_array_element() -> None:
    block = JsonPathBlock()
    result = await block.execute(
        JsonPathInput(data=SAMPLE_DATA, expression="$.store.books[0].title")
    )
    assert result.values == ["Foo"]
    assert result.match_count == 1


async def test_block_metadata() -> None:
    assert JsonPathBlock.block_type == "jsonpath"
    assert JsonPathBlock.cache_ttl == 0
