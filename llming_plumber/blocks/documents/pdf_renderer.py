"""Render PDF pages to images."""

from __future__ import annotations

import base64
import io
from typing import Any, ClassVar

from pydantic import Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput


class PdfRendererInput(BlockInput):
    content: str = Field(
        title="Content",
        description="Base64-encoded PDF bytes",
        json_schema_extra={"widget": "textarea"},
    )
    pages: list[int] = Field(
        default=[],
        title="Pages",
        description="Page numbers to render (1-based). Empty list means all pages.",
    )
    dpi: int = Field(
        default=150,
        title="DPI",
        description="Resolution for rendered images",
        json_schema_extra={"min": 72, "max": 600},
    )
    format: str = Field(
        default="jpeg",
        title="Image Format",
        description="Output image format",
        json_schema_extra={"widget": "select", "options": ["jpeg", "png"]},
    )


class PdfRendererOutput(BlockOutput):
    images: list[dict[str, Any]]
    page_count: int


class PdfRendererBlock(BaseBlock[PdfRendererInput, PdfRendererOutput]):
    block_type: ClassVar[str] = "pdf_renderer"
    icon: ClassVar[str] = "tabler/photo-scan"
    categories: ClassVar[list[str]] = ["documents", "pdf", "rendering"]
    description: ClassVar[str] = "Render PDF pages to images"
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self, input: PdfRendererInput, ctx: BlockContext | None = None
    ) -> PdfRendererOutput:
        import pypdfium2 as pdfium

        raw = base64.b64decode(input.content)
        pdf = pdfium.PdfDocument(raw)
        total_pages = len(pdf)

        target_pages = input.pages if input.pages else list(range(1, total_pages + 1))
        scale = input.dpi / 72

        images: list[dict[str, Any]] = []
        for page_num in target_pages:
            if page_num < 1 or page_num > total_pages:
                continue
            page = pdf[page_num - 1]
            bitmap = page.render(scale=scale)
            pil_image = bitmap.to_pil()

            buf = io.BytesIO()
            fmt = "JPEG" if input.format == "jpeg" else "PNG"
            pil_image.save(buf, format=fmt)
            encoded = base64.b64encode(buf.getvalue()).decode("ascii")

            images.append({
                "page_number": page_num,
                "image": encoded,
                "width": pil_image.width,
                "height": pil_image.height,
            })

        pdf.close()

        return PdfRendererOutput(images=images, page_count=total_pages)
