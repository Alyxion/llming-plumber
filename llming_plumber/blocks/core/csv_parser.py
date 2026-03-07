"""Parse CSV text into structured rows."""

from __future__ import annotations

import csv
import io
from typing import ClassVar

from pydantic import Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput
from llming_plumber.blocks.limits import MAX_RECORDS, check_list_size


class CsvParserInput(BlockInput):
    csv_text: str = Field(
        title="CSV Text",
        description="Raw CSV text to parse into structured rows",
        json_schema_extra={"widget": "textarea"},
    )
    delimiter: str = Field(
        default=",",
        title="Delimiter",
        description="Character used to separate columns",
        json_schema_extra={"placeholder": ","},
    )
    has_header: bool = Field(
        default=True,
        title="Has Header",
        description="Whether the first row contains column names",
    )


class CsvParserOutput(BlockOutput):
    rows: list[dict[str, str]]
    column_names: list[str]


class CsvParserBlock(BaseBlock[CsvParserInput, CsvParserOutput]):
    block_type: ClassVar[str] = "csv_parser"
    icon: ClassVar[str] = "tabler/file-spreadsheet"
    categories: ClassVar[list[str]] = ["core/transform", "documents/parsing"]
    description: ClassVar[str] = "Parse CSV text into structured rows"
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self, input: CsvParserInput, ctx: BlockContext | None = None
    ) -> CsvParserOutput:
        reader = csv.reader(io.StringIO(input.csv_text), delimiter=input.delimiter)
        all_rows = list(reader)
        check_list_size(all_rows, limit=MAX_RECORDS, label="CSV rows")

        if not all_rows:
            return CsvParserOutput(rows=[], column_names=[])

        if input.has_header:
            column_names = all_rows[0]
            data_rows = all_rows[1:]
        else:
            num_cols = len(all_rows[0])
            column_names = [f"col_{i}" for i in range(num_cols)]
            data_rows = all_rows

        rows: list[dict[str, str]] = [
            dict(zip(column_names, row, strict=False)) for row in data_rows
        ]

        return CsvParserOutput(rows=rows, column_names=column_names)
