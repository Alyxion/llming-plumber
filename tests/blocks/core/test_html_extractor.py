from __future__ import annotations

from llming_plumber.blocks.core.html_extractor import (
    HtmlExtractorBlock,
    HtmlExtractorInput,
    HtmlExtractorOutput,
)

SAMPLE_HTML = """
<html>
<body>
    <h1>Title</h1>
    <div class="content">
        <p>First paragraph.</p>
        <p>Second paragraph.</p>
    </div>
    <div class="sidebar">Sidebar text</div>
</body>
</html>
"""


async def test_extract_text_from_body() -> None:
    block = HtmlExtractorBlock()
    result = await block.execute(HtmlExtractorInput(html=SAMPLE_HTML))
    assert isinstance(result, HtmlExtractorOutput)
    assert "Title" in result.content
    assert "First paragraph." in result.content
    assert result.element_count == 1


async def test_extract_with_css_selector() -> None:
    block = HtmlExtractorBlock()
    result = await block.execute(
        HtmlExtractorInput(html=SAMPLE_HTML, selector="div.content p")
    )
    assert result.element_count == 2
    assert "First paragraph." in result.content
    assert "Second paragraph." in result.content


async def test_extract_html_mode() -> None:
    block = HtmlExtractorBlock()
    result = await block.execute(
        HtmlExtractorInput(
            html=SAMPLE_HTML, selector="h1", extract_mode="html"
        )
    )
    assert "<h1>" in result.content
    assert "Title" in result.content
    assert result.element_count == 1


async def test_no_matching_elements() -> None:
    block = HtmlExtractorBlock()
    result = await block.execute(
        HtmlExtractorInput(html=SAMPLE_HTML, selector="span.missing")
    )
    assert result.content == ""
    assert result.element_count == 0


async def test_multiple_divs() -> None:
    block = HtmlExtractorBlock()
    result = await block.execute(
        HtmlExtractorInput(html=SAMPLE_HTML, selector="div")
    )
    assert result.element_count == 2
    assert "Sidebar text" in result.content


async def test_block_metadata() -> None:
    assert HtmlExtractorBlock.block_type == "html_extractor"
    assert HtmlExtractorBlock.cache_ttl == 0
