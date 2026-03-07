"""Read Parquet files into structured records."""

from __future__ import annotations

import base64
import io
from typing import Any, ClassVar

from pydantic import Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput


class ParquetReaderInput(BlockInput):
    content: str = Field(
        title="Content",
        description="Base64-encoded Parquet file bytes",
        json_schema_extra={"widget": "textarea"},
    )
    columns: list[str] = Field(
        default=[],
        title="Columns",
        description="Column names to read. Empty list reads all columns.",
    )


class ParquetReaderOutput(BlockOutput):
    records: list[dict[str, Any]]
    columns: list[str]
    row_count: int
    column_schema: list[dict[str, str]]


class ParquetReaderBlock(BaseBlock[ParquetReaderInput, ParquetReaderOutput]):
    block_type: ClassVar[str] = "parquet_reader"
    icon: ClassVar[str] = "tabler/database"
    categories: ClassVar[list[str]] = ["documents", "parquet"]
    description: ClassVar[str] = "Read Parquet files into structured records"
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self, input: ParquetReaderInput, ctx: BlockContext | None = None
    ) -> ParquetReaderOutput:
        import pyarrow.parquet as pq

        raw = base64.b64decode(input.content)
        table = pq.read_table(
            io.BytesIO(raw),
            columns=input.columns if input.columns else None,
        )

        column_schema = [
            {"name": field.name, "type": str(field.type)}
            for field in table.schema
        ]
        columns = [field.name for field in table.schema]
        records = table.to_pydict()

        # Convert columnar dict to list of row dicts
        row_count = table.num_rows
        rows: list[dict[str, Any]] = []
        for i in range(row_count):
            rows.append({col: records[col][i] for col in columns})

        return ParquetReaderOutput(
            records=rows,
            columns=columns,
            row_count=row_count,
            column_schema=column_schema,
        )
