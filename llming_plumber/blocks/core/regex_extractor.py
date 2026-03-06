"""Extract patterns from text using regular expressions."""

from __future__ import annotations

import re
from typing import ClassVar

from pydantic import Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput


class RegexExtractorInput(BlockInput):
    text: str = Field(
        title="Text",
        description="The text to search for pattern matches",
        json_schema_extra={"widget": "textarea"},
    )
    pattern: str = Field(
        title="Pattern",
        description="Regular expression pattern to match against the text",
        json_schema_extra={"widget": "code", "placeholder": r'(?P<name>\w+)'},
    )


class RegexExtractorOutput(BlockOutput):
    matches: list[dict[str, str]]
    match_count: int


class RegexExtractorBlock(BaseBlock[RegexExtractorInput, RegexExtractorOutput]):
    block_type: ClassVar[str] = "regex_extractor"
    icon: ClassVar[str] = "tabler/regex"
    categories: ClassVar[list[str]] = ["core/transform"]
    description: ClassVar[str] = "Extract text patterns using regular expressions"
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self, input: RegexExtractorInput, ctx: BlockContext | None = None
    ) -> RegexExtractorOutput:
        compiled = re.compile(input.pattern)
        results: list[dict[str, str]] = []
        for m in compiled.finditer(input.text):
            if m.groupdict():
                results.append(m.groupdict())
            else:
                results.append({"match": m.group()})
        return RegexExtractorOutput(matches=results, match_count=len(results))
