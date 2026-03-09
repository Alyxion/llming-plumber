"""Unit tests for the web_crawler block."""

from __future__ import annotations

import json

import pytest
import respx
import httpx

from llming_plumber.blocks.base import BlockContext, Sink
from llming_plumber.blocks.web.crawler import (
    WebCrawlerBlock,
    WebCrawlerInput,
    _extract_links,
    _extract_text,
    _extract_title,
    _normalize_url,
    _slugify_domain,
    _url_to_slug,
)


# --- Helper function tests ---


def test_normalize_url_strips_fragment() -> None:
    assert _normalize_url("https://example.com/page#section") == "https://example.com/page"


def test_normalize_url_keeps_trailing_slash() -> None:
    assert _normalize_url("https://example.com/page/") == "https://example.com/page/"


def test_normalize_url_root_path() -> None:
    assert _normalize_url("https://example.com/") == "https://example.com/"
    assert _normalize_url("https://example.com") == "https://example.com/"


def test_extract_links_same_domain() -> None:
    html = """
    <a href="/about">About</a>
    <a href="https://example.com/products">Products</a>
    <a href="https://other.com/foo">External</a>
    <a href="mailto:info@example.com">Email</a>
    <a href="#top">Anchor</a>
    """
    links = _extract_links(html, "https://example.com/", "example.com")
    assert "https://example.com/about" in links
    assert "https://example.com/products" in links
    assert "https://other.com/foo" not in links
    assert len(links) == 2


def test_extract_text_removes_scripts() -> None:
    html = """
    <html><body>
    <script>alert('hi')</script>
    <style>.x { color: red }</style>
    <p>Visible content</p>
    <nav>Nav stuff</nav>
    </body></html>
    """
    text = _extract_text(html)
    assert "Visible content" in text
    assert "alert" not in text
    assert "color: red" not in text
    assert "Nav stuff" not in text


def test_extract_title() -> None:
    html = "<html><head><title>Test Page</title></head><body></body></html>"
    assert _extract_title(html) == "Test Page"


def test_extract_title_missing() -> None:
    html = "<html><body>No title</body></html>"
    assert _extract_title(html) == ""


# --- Block execution tests ---


@respx.mock
@pytest.mark.asyncio
async def test_crawl_single_page() -> None:
    html = """<html><head><title>Home</title></head>
    <body><p>Welcome</p></body></html>"""
    respx.get("https://test.com/").mock(
        return_value=httpx.Response(200, text=html, headers={"content-type": "text/html"})
    )
    block = WebCrawlerBlock()
    result = await block.execute(WebCrawlerInput(
        start_url="https://test.com/",
        max_pages=1,
        delay_seconds=0,
    ))
    assert result.page_count == 1
    assert result.domain == "test.com"
    assert result.pages[0]["title"] == "Home"
    assert result.pages[0]["status_code"] == 200
    assert "Welcome" in result.pages[0]["text"]
    assert result.pages[0]["content_hash"]  # Non-empty hash


@respx.mock
@pytest.mark.asyncio
async def test_crawl_follows_links() -> None:
    home_html = """<html><head><title>Home</title></head><body>
    <a href="/page2">Page 2</a>
    <a href="/page3">Page 3</a>
    </body></html>"""
    page2_html = """<html><head><title>Page 2</title></head><body>
    <p>Content of page 2</p>
    </body></html>"""
    page3_html = """<html><head><title>Page 3</title></head><body>
    <p>Content of page 3</p>
    </body></html>"""

    respx.get("https://test.com/").mock(
        return_value=httpx.Response(200, text=home_html, headers={"content-type": "text/html"})
    )
    respx.get("https://test.com/page2").mock(
        return_value=httpx.Response(200, text=page2_html, headers={"content-type": "text/html"})
    )
    respx.get("https://test.com/page3").mock(
        return_value=httpx.Response(200, text=page3_html, headers={"content-type": "text/html"})
    )

    block = WebCrawlerBlock()
    result = await block.execute(WebCrawlerInput(
        start_url="https://test.com/",
        max_pages=10,
        max_depth=2,
        delay_seconds=0,
    ))
    assert result.page_count == 3
    urls = [p["url"] for p in result.pages]
    assert "https://test.com/" in urls
    assert "https://test.com/page2" in urls
    assert "https://test.com/page3" in urls


@respx.mock
@pytest.mark.asyncio
async def test_crawl_respects_max_pages() -> None:
    home_html = """<html><head><title>Home</title></head><body>
    <a href="/a">A</a><a href="/b">B</a><a href="/c">C</a>
    </body></html>"""
    for path in ["/a", "/b", "/c"]:
        respx.get(f"https://test.com{path}").mock(
            return_value=httpx.Response(200, text=f"<html><body>{path}</body></html>",
                                         headers={"content-type": "text/html"})
        )
    respx.get("https://test.com/").mock(
        return_value=httpx.Response(200, text=home_html, headers={"content-type": "text/html"})
    )

    block = WebCrawlerBlock()
    result = await block.execute(WebCrawlerInput(
        start_url="https://test.com/",
        max_pages=2,
        delay_seconds=0,
    ))
    assert result.page_count == 2


@respx.mock
@pytest.mark.asyncio
async def test_crawl_respects_max_depth() -> None:
    html_depth0 = '<html><body><a href="/d1">D1</a></body></html>'
    html_depth1 = '<html><body><a href="/d2">D2</a></body></html>'
    html_depth2 = '<html><body>Deep page</body></html>'

    respx.get("https://test.com/").mock(
        return_value=httpx.Response(200, text=html_depth0, headers={"content-type": "text/html"})
    )
    respx.get("https://test.com/d1").mock(
        return_value=httpx.Response(200, text=html_depth1, headers={"content-type": "text/html"})
    )
    respx.get("https://test.com/d2").mock(
        return_value=httpx.Response(200, text=html_depth2, headers={"content-type": "text/html"})
    )

    block = WebCrawlerBlock()
    result = await block.execute(WebCrawlerInput(
        start_url="https://test.com/",
        max_pages=10,
        max_depth=1,
        delay_seconds=0,
    ))
    # Depth 0 = root, depth 1 = /d1, depth 2 = /d2 (should NOT be crawled)
    assert result.page_count == 2
    urls = [p["url"] for p in result.pages]
    assert "https://test.com/d2" not in urls


@respx.mock
@pytest.mark.asyncio
async def test_crawl_url_filter() -> None:
    html = """<html><body>
    <a href="/products/a">Prod A</a>
    <a href="/about">About</a>
    </body></html>"""
    respx.get("https://test.com/").mock(
        return_value=httpx.Response(200, text=html, headers={"content-type": "text/html"})
    )
    respx.get("https://test.com/products/a").mock(
        return_value=httpx.Response(200, text="<html><body>Product A</body></html>",
                                     headers={"content-type": "text/html"})
    )
    respx.get("https://test.com/about").mock(
        return_value=httpx.Response(200, text="<html><body>About</body></html>",
                                     headers={"content-type": "text/html"})
    )

    block = WebCrawlerBlock()
    result = await block.execute(WebCrawlerInput(
        start_url="https://test.com/",
        max_pages=10,
        delay_seconds=0,
        url_pattern="/products/",
    ))
    urls = [p["url"] for p in result.pages]
    assert "https://test.com/products/a" in urls
    assert "https://test.com/about" not in urls


@respx.mock
@pytest.mark.asyncio
async def test_crawl_handles_errors() -> None:
    respx.get("https://test.com/").mock(side_effect=httpx.ConnectError("Connection refused"))

    block = WebCrawlerBlock()
    result = await block.execute(WebCrawlerInput(
        start_url="https://test.com/",
        max_pages=1,
        delay_seconds=0,
    ))
    assert result.page_count == 0
    assert len(result.errors) == 1


@respx.mock
@pytest.mark.asyncio
async def test_crawl_skips_non_html() -> None:
    respx.get("https://test.com/").mock(
        return_value=httpx.Response(200, text="<html><body><a href='/data.json'>JSON</a></body></html>",
                                     headers={"content-type": "text/html"})
    )
    respx.get("https://test.com/data.json").mock(
        return_value=httpx.Response(200, text='{"key": "value"}',
                                     headers={"content-type": "application/json"})
    )

    block = WebCrawlerBlock()
    result = await block.execute(WebCrawlerInput(
        start_url="https://test.com/",
        max_pages=10,
        delay_seconds=0,
    ))
    assert result.page_count == 1  # Only the HTML page


# --- URL slug helpers ---


def test_slugify_domain() -> None:
    assert _slugify_domain("www.example.com") == "www_example_com"
    assert _slugify_domain("test.com") == "test_com"


def test_url_to_slug() -> None:
    assert _url_to_slug("https://example.com/") == "index"
    assert _url_to_slug("https://example.com/products/nozzles.html") == "products_nozzles_html"
    assert _url_to_slug("https://example.com/about") == "about"


# --- Sink streaming tests ---


class MemorySink(Sink):
    """In-memory sink for testing."""

    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}

    async def write(
        self,
        path: str,
        content: str | bytes,
        *,
        content_type: str = "",
        metadata: dict[str, str] | None = None,
    ) -> None:
        data = content.encode("utf-8") if isinstance(content, str) else content
        self.files[path] = data

    async def finalize(self) -> dict[str, object]:
        return {"files_written": len(self.files)}


@respx.mock
@pytest.mark.asyncio
async def test_crawl_streams_to_sink() -> None:
    """When a sink is connected, pages are written individually."""
    home = '<html><head><title>Home</title></head><body><a href="/about">About</a></body></html>'
    about = '<html><head><title>About</title></head><body>About us</body></html>'

    respx.get("https://test.com/").mock(
        return_value=httpx.Response(200, text=home, headers={"content-type": "text/html"})
    )
    respx.get("https://test.com/about").mock(
        return_value=httpx.Response(200, text=about, headers={"content-type": "text/html"})
    )

    sink = MemorySink()
    ctx = BlockContext(sink=sink)

    block = WebCrawlerBlock()
    result = await block.execute(
        WebCrawlerInput(start_url="https://test.com/", max_pages=10, delay_seconds=0),
        ctx,
    )

    assert result.page_count == 2
    # HTML and text not stored in output when sink is connected
    assert result.pages[0]["html"] == ""
    assert result.pages[0]["text"] == ""

    # Files written to sink
    html_files = [k for k in sink.files if "/html/" in k]
    text_files = [k for k in sink.files if "/text/" in k]
    assert len(html_files) == 2
    assert len(text_files) == 2

    # Manifest written
    manifest_files = [k for k in sink.files if k.endswith("content.json")]
    assert len(manifest_files) == 1
    manifest = json.loads(sink.files[manifest_files[0]])
    assert manifest["page_count"] == 2
    assert manifest["domain"] == "test.com"
    assert len(manifest["pages"]) == 2


@respx.mock
@pytest.mark.asyncio
async def test_crawl_without_sink_buffers_html() -> None:
    """Without a sink, HTML is stored in the output as before."""
    html = '<html><head><title>Home</title></head><body>Content</body></html>'
    respx.get("https://test.com/").mock(
        return_value=httpx.Response(200, text=html, headers={"content-type": "text/html"})
    )

    block = WebCrawlerBlock()
    result = await block.execute(
        WebCrawlerInput(start_url="https://test.com/", max_pages=1, delay_seconds=0),
    )

    assert result.page_count == 1
    assert result.pages[0]["html"] != ""
    assert "Content" in result.pages[0]["text"]
