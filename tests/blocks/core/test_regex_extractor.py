from __future__ import annotations

import pytest

from llming_plumber.blocks.core.regex_extractor import (
    RegexExtractorBlock,
    RegexExtractorInput,
)


async def test_named_groups() -> None:
    block = RegexExtractorBlock()
    result = await block.execute(
        RegexExtractorInput(
            text="John 30, Jane 25",
            pattern=r"(?P<name>\w+) (?P<age>\d+)",
        )
    )
    assert result.match_count == 2
    assert result.matches[0] == {"name": "John", "age": "30"}
    assert result.matches[1] == {"name": "Jane", "age": "25"}


async def test_no_named_groups_returns_full_match() -> None:
    block = RegexExtractorBlock()
    result = await block.execute(
        RegexExtractorInput(text="abc 123 def 456", pattern=r"\d+")
    )
    assert result.match_count == 2
    assert result.matches[0] == {"match": "123"}
    assert result.matches[1] == {"match": "456"}


async def test_no_matches() -> None:
    block = RegexExtractorBlock()
    result = await block.execute(
        RegexExtractorInput(text="hello world", pattern=r"\d+")
    )
    assert result.match_count == 0
    assert result.matches == []


async def test_empty_text() -> None:
    block = RegexExtractorBlock()
    result = await block.execute(
        RegexExtractorInput(text="", pattern=r"\w+")
    )
    assert result.match_count == 0


async def test_invalid_regex_raises() -> None:
    block = RegexExtractorBlock()
    with pytest.raises(Exception):
        await block.execute(
            RegexExtractorInput(text="test", pattern=r"[invalid")
        )


async def test_block_type() -> None:
    assert RegexExtractorBlock.block_type == "regex_extractor"
