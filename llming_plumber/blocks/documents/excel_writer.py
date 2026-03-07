"""Write data to Excel (xlsx) files."""

from __future__ import annotations

import base64
import io
from typing import Any, ClassVar

from pydantic import Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput


class ExcelWriterInput(BlockInput):
    records: list[dict[str, Any]] = Field(
        title="Records",
        description="List of row dictionaries to write",
    )
    sheet_name: str = Field(
        default="Sheet1",
        title="Sheet Name",
        description="Name of the worksheet",
    )
    columns: list[str] = Field(
        default=[],
        title="Columns",
        description="Column ordering. Empty list uses keys from the first record.",
    )


class ExcelWriterOutput(BlockOutput):
    content: str
    row_count: int
    column_count: int


class ExcelWriterBlock(BaseBlock[ExcelWriterInput, ExcelWriterOutput]):
    block_type: ClassVar[str] = "excel_writer"
    icon: ClassVar[str] = "tabler/file-spreadsheet"
    categories: ClassVar[list[str]] = ["documents", "excel"]
    description: ClassVar[str] = "Write data to Excel (xlsx) files"
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self, input: ExcelWriterInput, ctx: BlockContext | None = None
    ) -> ExcelWriterOutput:
        import openpyxl

        wb = openpyxl.Workbook()
        ws = wb.active
        if ws is None:  # pragma: no cover
            ws = wb.create_sheet()
        ws.title = input.sheet_name

        if not input.records:
            buf = io.BytesIO()
            wb.save(buf)
            encoded = base64.b64encode(buf.getvalue()).decode("ascii")
            return ExcelWriterOutput(content=encoded, row_count=0, column_count=0)

        columns = input.columns if input.columns else list(input.records[0].keys())
        ws.append(columns)

        for record in input.records:
            ws.append([record.get(col, "") for col in columns])

        buf = io.BytesIO()
        wb.save(buf)
        encoded = base64.b64encode(buf.getvalue()).decode("ascii")

        return ExcelWriterOutput(
            content=encoded,
            row_count=len(input.records),
            column_count=len(columns),
        )
