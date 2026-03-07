"""Read and extract text from PDF documents."""

from __future__ import annotations

import base64
import io
from typing import Any, ClassVar

from pydantic import Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput


class PdfReaderInput(BlockInput):
    content: str = Field(
        title="Content",
        description="Base64-encoded PDF bytes",
        json_schema_extra={"widget": "textarea"},
    )
    pages: list[int] = Field(
        default=[],
        title="Pages",
        description="Page numbers to extract (1-based). Empty list means all pages.",
    )
    extract_tables: bool = Field(
        default=False,
        title="Extract Tables",
        description="Also extract tables as list of lists",
    )


class PdfReaderOutput(BlockOutput):
    text: str
    pages: list[dict[str, Any]]
    page_count: int
    metadata: dict[str, Any]


class PdfReaderBlock(BaseBlock[PdfReaderInput, PdfReaderOutput]):
    block_type: ClassVar[str] = "pdf_reader"
    icon: ClassVar[str] = "tabler/file-type-pdf"
    categories: ClassVar[list[str]] = ["documents", "pdf"]
    description: ClassVar[str] = "Read and extract text from PDF documents"
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self, input: PdfReaderInput, ctx: BlockContext | None = None
    ) -> PdfReaderOutput:
        import pdfplumber

        raw = base64.b64decode(input.content)
        pdf = pdfplumber.open(io.BytesIO(raw))

        page_results: list[dict[str, Any]] = []
        all_text_parts: list[str] = []

        target_pages = (
            input.pages if input.pages else list(range(1, len(pdf.pages) + 1))
        )

        for page_num in target_pages:
            if page_num < 1 or page_num > len(pdf.pages):
                continue
            page = pdf.pages[page_num - 1]
            page_text = page.extract_text() or ""
            all_text_parts.append(page_text)

            page_data: dict[str, Any] = {
                "page_number": page_num,
                "text": page_text,
            }
            if input.extract_tables:
                tables = page.extract_tables() or []
                page_data["tables"] = tables
            page_results.append(page_data)

        metadata: dict[str, Any] = {}
        if pdf.metadata:
            for key in ("Title", "Author", "Subject", "Creator", "Producer"):
                if key in pdf.metadata:
                    metadata[key.lower()] = pdf.metadata[key]

        total_pages = len(pdf.pages)
        pdf.close()

        return PdfReaderOutput(
            text="\n".join(all_text_parts),
            pages=page_results,
            page_count=total_pages,
            metadata=metadata,
        )
