"""Map, rename, and transform columns in a list of records.

Lets non-technical users bridge the gap between column names in
their data source (e.g. "Website URL" in an Excel table) and
the field names expected by downstream blocks (e.g. "url").
"""

from __future__ import annotations

from typing import Any, ClassVar

from pydantic import Field

from llming_plumber.blocks.base import (
    BaseBlock,
    BlockContext,
    BlockInput,
    BlockOutput,
)
from llming_plumber.blocks.limits import check_list_size


class ColumnMapperInput(BlockInput):
    records: list[dict[str, Any]] = Field(
        title="Records",
        description="Input records (e.g. from Excel Reader)",
    )
    mapping: dict[str, str] = Field(
        title="Column Mapping",
        description=(
            "Map source column names to target names. "
            "Example: {\"Website URL\": \"url\", "
            "\"Request Type\": \"method\"}"
        ),
        json_schema_extra={"widget": "code"},
    )
    defaults: dict[str, Any] = Field(
        default={},
        title="Default Values",
        description=(
            "Default values for target fields when source is "
            "missing or empty. Example: {\"method\": \"GET\"}"
        ),
        json_schema_extra={"widget": "code"},
    )
    drop_unmapped: bool = Field(
        default=False,
        title="Drop Unmapped Columns",
        description=(
            "If true, only mapped columns appear in output. "
            "If false, unmapped columns pass through unchanged."
        ),
    )


class ColumnMapperOutput(BlockOutput):
    records: list[dict[str, Any]]
    record_count: int
    mapped_columns: list[str]


class ColumnMapperBlock(
    BaseBlock[ColumnMapperInput, ColumnMapperOutput]
):
    block_type: ClassVar[str] = "column_mapper"
    icon: ClassVar[str] = "tabler/columns"
    categories: ClassVar[list[str]] = ["core", "data"]
    description: ClassVar[str] = (
        "Rename and map columns to match downstream blocks"
    )
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self,
        input: ColumnMapperInput,
        ctx: BlockContext | None = None,
    ) -> ColumnMapperOutput:
        check_list_size(input.records, label="Column mapper records")
        mapped: list[dict[str, Any]] = []

        for record in input.records:
            row: dict[str, Any] = {}

            if not input.drop_unmapped:
                row.update(record)

            for source_col, target_col in input.mapping.items():
                if source_col in record:
                    val = record[source_col]
                    if val == "" and target_col in input.defaults:
                        val = input.defaults[target_col]
                    row[target_col] = val
                elif target_col in input.defaults:
                    row[target_col] = input.defaults[target_col]

                if input.drop_unmapped and source_col in row:
                    del row[source_col]

            for key, default_val in input.defaults.items():
                if key not in row:
                    row[key] = default_val

            mapped.append(row)

        return ColumnMapperOutput(
            records=mapped,
            record_count=len(mapped),
            mapped_columns=list(input.mapping.values()),
        )
