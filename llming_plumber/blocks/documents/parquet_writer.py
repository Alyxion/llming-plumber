"""Write structured records to Parquet files."""

from __future__ import annotations

import base64
import io
from typing import Any, ClassVar

from pydantic import Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput


class ParquetWriterInput(BlockInput):
    records: list[dict[str, Any]] = Field(
        title="Records",
        description="List of row dictionaries to write",
    )
    compression: str = Field(
        default="snappy",
        title="Compression",
        description="Parquet compression codec",
        json_schema_extra={
            "widget": "select",
            "options": ["snappy", "gzip", "zstd", "none"],
        },
    )


class ParquetWriterOutput(BlockOutput):
    content: str
    row_count: int
    column_count: int


class ParquetWriterBlock(BaseBlock[ParquetWriterInput, ParquetWriterOutput]):
    block_type: ClassVar[str] = "parquet_writer"
    icon: ClassVar[str] = "tabler/database"
    categories: ClassVar[list[str]] = ["documents", "parquet"]
    description: ClassVar[str] = "Write structured records to Parquet files"
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self, input: ParquetWriterInput, ctx: BlockContext | None = None
    ) -> ParquetWriterOutput:
        import pyarrow as pa
        import pyarrow.parquet as pq

        if not input.records:
            table = pa.table({})
            buf = io.BytesIO()
            pq.write_table(table, buf, compression=input.compression)
            encoded = base64.b64encode(buf.getvalue()).decode("ascii")
            return ParquetWriterOutput(content=encoded, row_count=0, column_count=0)

        columns = list(input.records[0].keys())
        columnar: dict[str, list[Any]] = {col: [] for col in columns}
        for record in input.records:
            for col in columns:
                columnar[col].append(record.get(col))

        table = pa.table(columnar)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression=input.compression)
        encoded = base64.b64encode(buf.getvalue()).decode("ascii")

        return ParquetWriterOutput(
            content=encoded,
            row_count=len(input.records),
            column_count=len(columns),
        )
