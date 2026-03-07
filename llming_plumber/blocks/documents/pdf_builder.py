"""Build PDF documents from structured JSON definitions.

Supports three content modes:
- text: Simple flowing text with paragraphs and headings
- geometric: Precise positioned elements (text rects, lines, images)
- mixed: Both flowing text and geometric overlays per page
"""

from __future__ import annotations

import base64
import io
from typing import Any, ClassVar, Literal

from pydantic import BaseModel, Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput
from llming_plumber.blocks.limits import MAX_PAGES, check_page_count


class FontSpec(BaseModel):
    """Font specification for text elements."""

    family: str = Field(
        default="Helvetica",
        description="Font family: Helvetica, Times-Roman, Courier",
    )
    size: float = Field(default=12, description="Font size in points")
    bold: bool = False
    italic: bool = False
    color: str = Field(
        default="000000",
        description="Hex color without #, e.g. 'FF0000'",
    )


class TextRect(BaseModel):
    """A positioned text element on the page."""

    x: float = Field(description="X position in points from left edge")
    y: float = Field(description="Y position in points from bottom edge")
    width: float = Field(
        default=0,
        description="Text wrap width in points. 0 for no wrapping.",
    )
    text: str = Field(description="Text content")
    font: FontSpec = Field(default_factory=FontSpec)
    alignment: Literal["left", "center", "right"] = "left"


class LineElement(BaseModel):
    """A line drawn on the page."""

    x1: float
    y1: float
    x2: float
    y2: float
    color: str = Field(default="000000", description="Hex color without #")
    width: float = Field(default=1, description="Line width in points")


class RectElement(BaseModel):
    """A rectangle drawn on the page."""

    x: float
    y: float
    width: float
    height: float
    stroke_color: str = Field(default="000000", description="Border hex color")
    fill_color: str = Field(
        default="", description="Fill hex color. Empty for no fill."
    )
    stroke_width: float = 1


class ImageElement(BaseModel):
    """An embedded image on the page."""

    x: float
    y: float
    width: float
    height: float
    data_b64: str = Field(description="Base64-encoded image data (PNG or JPEG)")


class TableDef(BaseModel):
    """A table to render on the page."""

    x: float = Field(description="X position of top-left corner")
    y: float = Field(description="Y position of top-left corner")
    headers: list[str] = Field(default=[], description="Column headers")
    rows: list[list[str]] = Field(description="Table data rows")
    col_widths: list[float] = Field(
        default=[],
        description="Column widths in points. Empty for equal widths.",
    )
    row_height: float = Field(default=20, description="Row height in points")
    header_font: FontSpec = Field(
        default_factory=lambda: FontSpec(bold=True),
    )
    cell_font: FontSpec = Field(default_factory=FontSpec)
    border_color: str = Field(default="000000", description="Border hex color")
    header_bg: str = Field(
        default="", description="Header background hex color"
    )


class TextParagraph(BaseModel):
    """A flowing text paragraph for text mode."""

    text: str
    style: Literal[
        "heading1", "heading2", "heading3", "body", "caption"
    ] = "body"
    font: FontSpec | None = Field(
        default=None,
        description="Override font. None uses style defaults.",
    )
    space_after: float = Field(
        default=0,
        description="Extra space after paragraph in points. 0 uses style default.",
    )


class PageDef(BaseModel):
    """Definition of a single PDF page."""

    width: float = Field(default=595, description="Page width in points (595 = A4)")
    height: float = Field(default=842, description="Page height in points (842 = A4)")

    paragraphs: list[TextParagraph] = Field(
        default=[],
        description="Flowing text paragraphs (text/mixed mode)",
    )
    text_rects: list[TextRect] = Field(
        default=[],
        description="Positioned text elements (geometric/mixed mode)",
    )
    lines: list[LineElement] = Field(
        default=[],
        description="Line elements (geometric/mixed mode)",
    )
    rects: list[RectElement] = Field(
        default=[],
        description="Rectangle elements (geometric/mixed mode)",
    )
    images: list[ImageElement] = Field(
        default=[],
        description="Embedded images (geometric/mixed mode)",
    )
    tables: list[TableDef] = Field(
        default=[],
        description="Tables rendered on the page",
    )
    margin: float = Field(
        default=72,
        description="Page margin in points for flowing text (72 = 1 inch)",
    )


class PdfBuilderInput(BlockInput):
    pages: list[PageDef] = Field(
        title="Pages",
        description="List of page definitions",
        min_length=1,
    )
    mode: Literal["text", "geometric", "mixed"] = Field(
        default="text",
        title="Mode",
        description=(
            "text: flowing paragraphs auto-wrapped and paginated. "
            "geometric: precise positioned elements. "
            "mixed: both paragraphs and positioned elements."
        ),
        json_schema_extra={"widget": "select"},
    )
    metadata: dict[str, str] = Field(
        default_factory=dict,
        title="Metadata",
        description="PDF metadata: title, author, subject, creator",
    )
    json_data: str = Field(
        default="",
        title="JSON Data",
        description=(
            "Alternative: provide pages as a JSON string matching "
            "the PageDef schema. Ignored if pages has data."
        ),
        json_schema_extra={"widget": "code", "rows": 20},
    )


class PdfBuilderOutput(BlockOutput):
    content: str = Field(description="Base64-encoded PDF")
    page_count: int
    mode: str


def _hex_to_rgb(hex_color: str) -> tuple[float, float, float]:
    """Convert hex color string to RGB floats 0-1."""
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return r / 255, g / 255, b / 255


class PdfBuilderBlock(BaseBlock[PdfBuilderInput, PdfBuilderOutput]):
    block_type: ClassVar[str] = "pdf_builder"
    icon: ClassVar[str] = "tabler/file-type-pdf"
    categories: ClassVar[list[str]] = ["documents", "pdf"]
    description: ClassVar[str] = (
        "Build PDF documents with text, geometric positioning, or both"
    )
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self, input: PdfBuilderInput, ctx: BlockContext | None = None
    ) -> PdfBuilderOutput:
        import json

        pages = input.pages
        if input.json_data and not any(
            p.paragraphs or p.text_rects for p in pages
        ):
            raw = json.loads(input.json_data)
            pages = [PageDef.model_validate(p) for p in raw]

        check_page_count(len(pages), limit=MAX_PAGES, label="PDF pages")
        return self._build_pdf(pages, input.mode, input.metadata)

    def _build_pdf(
        self,
        pages: list[PageDef],
        mode: str,
        metadata: dict[str, str],
    ) -> PdfBuilderOutput:
        from reportlab.lib.pagesizes import landscape, portrait  # noqa: F401
        from reportlab.pdfgen import canvas

        buf = io.BytesIO()
        first_page = pages[0] if pages else PageDef()
        c = canvas.Canvas(buf, pagesize=(first_page.width, first_page.height))

        if metadata.get("title"):
            c.setTitle(metadata["title"])
        if metadata.get("author"):
            c.setAuthor(metadata["author"])
        if metadata.get("subject"):
            c.setSubject(metadata["subject"])

        for page_idx, page_def in enumerate(pages):
            if page_idx > 0:
                c.showPage()
            c.setPageSize((page_def.width, page_def.height))

            if mode in ("text", "mixed"):
                self._render_paragraphs(c, page_def)

            if mode in ("geometric", "mixed"):
                self._render_geometric(c, page_def)

            self._render_tables(c, page_def)

        c.save()
        encoded = base64.b64encode(buf.getvalue()).decode("ascii")
        return PdfBuilderOutput(
            content=encoded,
            page_count=len(pages),
            mode=mode,
        )

    def _resolve_font(self, font: FontSpec) -> str:
        name = font.family
        if font.bold and font.italic:
            name += "-BoldOblique"
        elif font.bold:
            name += "-Bold"
        elif font.italic:
            name += "-Oblique"
        return name

    def _apply_font(self, c: Any, font: FontSpec) -> None:
        c.setFont(self._resolve_font(font), font.size)
        r, g, b = _hex_to_rgb(font.color)
        c.setFillColorRGB(r, g, b)

    _STYLE_DEFAULTS: ClassVar[dict[str, FontSpec]] = {
        "heading1": FontSpec(size=24, bold=True),
        "heading2": FontSpec(size=18, bold=True),
        "heading3": FontSpec(size=14, bold=True),
        "body": FontSpec(size=12),
        "caption": FontSpec(size=10, italic=True, color="666666"),
    }

    _STYLE_SPACING: ClassVar[dict[str, float]] = {
        "heading1": 30,
        "heading2": 24,
        "heading3": 18,
        "body": 14,
        "caption": 12,
    }

    def _render_paragraphs(self, c: Any, page: PageDef) -> None:
        margin = page.margin
        usable_width = page.width - 2 * margin
        y_cursor = page.height - margin

        for para in page.paragraphs:
            font = para.font or self._STYLE_DEFAULTS.get(
                para.style, FontSpec()
            )
            spacing = para.space_after or self._STYLE_SPACING.get(
                para.style, 14
            )
            self._apply_font(c, font)

            lines = self._wrap_text(c, para.text, usable_width, font)
            for line in lines:
                if y_cursor < margin:
                    break
                c.drawString(margin, y_cursor, line)
                y_cursor -= font.size * 1.2

            y_cursor -= spacing - font.size * 1.2

    def _wrap_text(
        self, c: Any, text: str, max_width: float, font: FontSpec
    ) -> list[str]:
        font_name = self._resolve_font(font)
        words = text.split()
        if not words:
            return [""]

        lines: list[str] = []
        current_line = words[0]

        for word in words[1:]:
            test = current_line + " " + word
            w = c.stringWidth(test, font_name, font.size)
            if w <= max_width:
                current_line = test
            else:
                lines.append(current_line)
                current_line = word

        lines.append(current_line)
        return lines

    def _render_geometric(self, c: Any, page: PageDef) -> None:
        for tr in page.text_rects:
            self._apply_font(c, tr.font)
            if tr.width > 0:
                lines = self._wrap_text(c, tr.text, tr.width, tr.font)
                y = tr.y
                for line in lines:
                    if tr.alignment == "center":
                        w = c.stringWidth(
                            line, self._resolve_font(tr.font), tr.font.size
                        )
                        c.drawString(tr.x + (tr.width - w) / 2, y, line)
                    elif tr.alignment == "right":
                        w = c.stringWidth(
                            line, self._resolve_font(tr.font), tr.font.size
                        )
                        c.drawString(tr.x + tr.width - w, y, line)
                    else:
                        c.drawString(tr.x, y, line)
                    y -= tr.font.size * 1.2
            else:
                c.drawString(tr.x, tr.y, tr.text)

        for line in page.lines:
            r, g, b = _hex_to_rgb(line.color)
            c.setStrokeColorRGB(r, g, b)
            c.setLineWidth(line.width)
            c.line(line.x1, line.y1, line.x2, line.y2)

        for rect in page.rects:
            r, g, b = _hex_to_rgb(rect.stroke_color)
            c.setStrokeColorRGB(r, g, b)
            c.setLineWidth(rect.stroke_width)
            if rect.fill_color:
                fr, fg, fb = _hex_to_rgb(rect.fill_color)
                c.setFillColorRGB(fr, fg, fb)
                c.rect(
                    rect.x, rect.y, rect.width, rect.height,
                    stroke=1, fill=1,
                )
            else:
                c.rect(rect.x, rect.y, rect.width, rect.height, stroke=1)

        for img in page.images:
            from reportlab.lib.utils import ImageReader

            img_data = base64.b64decode(img.data_b64)
            c.drawImage(
                ImageReader(io.BytesIO(img_data)),
                img.x, img.y, img.width, img.height,
            )

    def _render_tables(self, c: Any, page: PageDef) -> None:
        for table in page.tables:
            num_cols = len(table.headers) if table.headers else (
                len(table.rows[0]) if table.rows else 0
            )
            if num_cols == 0:
                continue

            if table.col_widths:
                col_widths = table.col_widths
            else:
                total_w = (page.width - 2 * page.margin) * 0.9
                col_widths = [total_w / num_cols] * num_cols

            y = table.y
            rh = table.row_height

            if table.headers:
                if table.header_bg:
                    fr, fg, fb = _hex_to_rgb(table.header_bg)
                    c.setFillColorRGB(fr, fg, fb)
                    total_w = sum(col_widths)
                    c.rect(table.x, y - rh, total_w, rh, stroke=0, fill=1)

                self._apply_font(c, table.header_font)
                x_offset = table.x
                for col_idx, header in enumerate(table.headers):
                    c.drawString(x_offset + 4, y - rh + 5, header)
                    x_offset += col_widths[col_idx]
                y -= rh

            self._apply_font(c, table.cell_font)
            for row in table.rows:
                x_offset = table.x
                for col_idx, cell in enumerate(row):
                    c.drawString(x_offset + 4, y - rh + 5, str(cell))
                    if col_idx < len(col_widths):
                        x_offset += col_widths[col_idx]
                y -= rh

            # Draw grid
            r, g, b = _hex_to_rgb(table.border_color)
            c.setStrokeColorRGB(r, g, b)
            c.setLineWidth(0.5)

            total_rows = len(table.rows) + (1 if table.headers else 0)
            total_w = sum(col_widths)
            top_y = table.y
            bottom_y = top_y - total_rows * rh

            for i in range(total_rows + 1):
                row_y = top_y - i * rh
                c.line(table.x, row_y, table.x + total_w, row_y)

            x_offset = table.x
            for w in col_widths:
                c.line(x_offset, top_y, x_offset, bottom_y)
                x_offset += w
            c.line(x_offset, top_y, x_offset, bottom_y)
