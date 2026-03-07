"""Extract structured PageDef models from PDF files.

Supports three extraction modes matching the builder:
- text: Extract flowing paragraphs
- geometric: Extract positioned text rects with coordinates and font info
- mixed: Both paragraphs and positioned elements
"""

from __future__ import annotations

import base64
import io
from typing import Any, ClassVar, Literal

from pydantic import Field

from llming_plumber.blocks.base import (
    BaseBlock,
    BlockContext,
    BlockInput,
    BlockOutput,
)
from llming_plumber.blocks.documents.pdf_builder import (
    FontSpec,
    LineElement,
    PageDef,
    RectElement,
    TextParagraph,
    TextRect,
)


class PdfExtractorInput(BlockInput):
    content: str = Field(
        title="Content",
        description="Base64-encoded PDF bytes",
        json_schema_extra={"widget": "textarea"},
    )
    mode: Literal["text", "geometric", "mixed"] = Field(
        default="mixed",
        title="Mode",
        description=(
            "text: extract flowing paragraphs. "
            "geometric: extract positioned text rects. "
            "mixed: extract both."
        ),
        json_schema_extra={"widget": "select"},
    )
    pages: list[int] = Field(
        default=[],
        title="Pages",
        description=(
            "Page numbers to extract (1-based). "
            "Empty list extracts all."
        ),
    )


class PdfExtractorOutput(BlockOutput):
    pages_def: list[PageDef] = Field(
        description="Structured page definitions",
    )
    pages_json: str = Field(
        description="JSON string of pages for piping to builders",
    )
    mode: str


class PdfExtractorBlock(
    BaseBlock[PdfExtractorInput, PdfExtractorOutput]
):
    block_type: ClassVar[str] = "pdf_extractor"
    icon: ClassVar[str] = "tabler/file-type-pdf"
    categories: ClassVar[list[str]] = ["documents", "pdf"]
    description: ClassVar[str] = (
        "Extract structured PageDef models from PDFs "
        "with text, geometric, or mixed modes"
    )
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self,
        input: PdfExtractorInput,
        ctx: BlockContext | None = None,
    ) -> PdfExtractorOutput:
        import json

        import pdfplumber

        raw = base64.b64decode(input.content)
        pdf = pdfplumber.open(io.BytesIO(raw))

        target_pages = input.pages or list(
            range(1, len(pdf.pages) + 1)
        )
        pages_def: list[PageDef] = []

        for page_num in target_pages:
            if page_num < 1 or page_num > len(pdf.pages):
                continue
            page = pdf.pages[page_num - 1]
            pages_def.append(
                self._extract_page(page, input.mode)
            )

        pdf.close()

        pages_json = json.dumps(
            [p.model_dump() for p in pages_def],
            default=str,
        )

        return PdfExtractorOutput(
            pages_def=pages_def,
            pages_json=pages_json,
            mode=input.mode,
        )

    def _extract_page(
        self, page: Any, mode: str
    ) -> PageDef:
        width = float(page.width)
        height = float(page.height)

        paragraphs: list[TextParagraph] = []
        text_rects: list[TextRect] = []
        lines: list[LineElement] = []
        rects: list[RectElement] = []

        if mode in ("text", "mixed"):
            paragraphs = self._extract_paragraphs(page)

        if mode in ("geometric", "mixed"):
            text_rects = self._extract_text_rects(page)
            lines = self._extract_lines(page)
            rects = self._extract_rects(page)

        return PageDef(
            width=width,
            height=height,
            paragraphs=paragraphs,
            text_rects=text_rects,
            lines=lines,
            rects=rects,
        )

    def _extract_paragraphs(
        self, page: Any
    ) -> list[TextParagraph]:
        text = page.extract_text() or ""
        if not text.strip():
            return []

        paragraphs: list[TextParagraph] = []
        for line in text.split("\n"):
            stripped = line.strip()
            if not stripped:
                continue
            paragraphs.append(
                TextParagraph(text=stripped, style="body")
            )
        return paragraphs

    def _extract_text_rects(
        self, page: Any
    ) -> list[TextRect]:
        text_rects: list[TextRect] = []
        words = page.extract_words(
            extra_attrs=["fontname", "size"]
        )

        for word in words:
            font_name = word.get("fontname", "Helvetica")
            font_size = float(word.get("size", 12))

            base_family = "Helvetica"
            bold = False
            italic = False

            name_lower = font_name.lower()
            if "bold" in name_lower:
                bold = True
            if "italic" in name_lower or "oblique" in name_lower:
                italic = True

            if "times" in name_lower:
                base_family = "Times-Roman"
            elif "courier" in name_lower:
                base_family = "Courier"

            x = float(word["x0"])
            y = float(page.height) - float(word["top"])

            text_rects.append(TextRect(
                x=round(x, 2),
                y=round(y, 2),
                text=word["text"],
                font=FontSpec(
                    family=base_family,
                    size=round(font_size, 1),
                    bold=bold,
                    italic=italic,
                ),
            ))

        return text_rects

    def _extract_lines(self, page: Any) -> list[LineElement]:
        lines: list[LineElement] = []
        for line in page.lines or []:
            lines.append(LineElement(
                x1=round(float(line["x0"]), 2),
                y1=round(
                    float(page.height) - float(line["top"]), 2
                ),
                x2=round(float(line["x1"]), 2),
                y2=round(
                    float(page.height) - float(line["bottom"]),
                    2,
                ),
            ))
        return lines

    def _extract_rects(self, page: Any) -> list[RectElement]:
        rects: list[RectElement] = []
        for rect in page.rects or []:
            rects.append(RectElement(
                x=round(float(rect["x0"]), 2),
                y=round(
                    float(page.height) - float(rect["top"]), 2
                ),
                width=round(
                    float(rect["x1"]) - float(rect["x0"]), 2
                ),
                height=round(
                    float(rect["bottom"]) - float(rect["top"]), 2
                ),
            ))
        return rects
