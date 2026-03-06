"""Extract values from JSON data using JSONPath expressions."""

from __future__ import annotations

from typing import Any, ClassVar

from jsonpath_ng import parse as jsonpath_parse  # type: ignore[import-untyped]
from pydantic import Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput


class JsonPathInput(BlockInput):
    data: dict[str, Any] = Field(
        title="Data",
        description="The JSON data to query",
    )
    expression: str = Field(
        title="Expression",
        description="JSONPath expression to extract values",
        json_schema_extra={"widget": "code", "placeholder": "$.store.book[*].author"},
    )


class JsonPathOutput(BlockOutput):
    values: list[Any]
    match_count: int


class JsonPathBlock(BaseBlock[JsonPathInput, JsonPathOutput]):
    block_type: ClassVar[str] = "jsonpath"
    icon: ClassVar[str] = "tabler/braces"
    categories: ClassVar[list[str]] = ["core/transform"]
    description: ClassVar[str] = "Extract values from JSON using JSONPath expressions"
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self, input: JsonPathInput, ctx: BlockContext | None = None
    ) -> JsonPathOutput:
        expr = jsonpath_parse(input.expression)
        matches = expr.find(input.data)
        values: list[Any] = [m.value for m in matches]
        return JsonPathOutput(values=values, match_count=len(values))
