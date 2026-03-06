from __future__ import annotations

import httpx
import pytest
import respx

from llming_plumber.blocks.news.rss_reader import (
    RssReaderBlock,
    RssReaderInput,
    RssReaderOutput,
)

MOCK_RSS = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Test Feed</title>
    <item>
      <title>Article One</title>
      <link>https://example.com/1</link>
      <description>Summary of article one</description>
      <pubDate>Thu, 06 Mar 2026 12:00:00 GMT</pubDate>
      <author>Alice</author>
    </item>
    <item>
      <title>Article Two</title>
      <link>https://example.com/2</link>
      <description>Summary of article two</description>
      <pubDate>Thu, 06 Mar 2026 11:00:00 GMT</pubDate>
    </item>
    <item>
      <title>Article Three</title>
      <link>https://example.com/3</link>
    </item>
  </channel>
</rss>"""


@respx.mock
async def test_rss_basic() -> None:
    respx.get("https://example.com/feed.xml").mock(
        return_value=httpx.Response(200, text=MOCK_RSS)
    )
    block = RssReaderBlock()
    result = await block.execute(
        RssReaderInput(feed_url="https://example.com/feed.xml")
    )
    assert isinstance(result, RssReaderOutput)
    assert len(result.items) == 3
    assert result.feed_title == "Test Feed"
    assert result.items[0].title == "Article One"
    assert result.items[0].link == "https://example.com/1"
    assert result.items[0].summary == "Summary of article one"
    assert result.items[0].author == "Alice"


@respx.mock
async def test_rss_max_items() -> None:
    respx.get("https://example.com/feed.xml").mock(
        return_value=httpx.Response(200, text=MOCK_RSS)
    )
    block = RssReaderBlock()
    result = await block.execute(
        RssReaderInput(feed_url="https://example.com/feed.xml", max_items=2)
    )
    assert len(result.items) == 2


@respx.mock
async def test_rss_empty_feed() -> None:
    empty_rss = """<?xml version="1.0"?>
    <rss version="2.0"><channel><title>Empty</title></channel></rss>"""
    respx.get("https://example.com/empty.xml").mock(
        return_value=httpx.Response(200, text=empty_rss)
    )
    block = RssReaderBlock()
    result = await block.execute(
        RssReaderInput(feed_url="https://example.com/empty.xml")
    )
    assert result.items == []
    assert result.feed_title == "Empty"


@respx.mock
async def test_rss_missing_optional_fields() -> None:
    respx.get("https://example.com/feed.xml").mock(
        return_value=httpx.Response(200, text=MOCK_RSS)
    )
    block = RssReaderBlock()
    result = await block.execute(
        RssReaderInput(feed_url="https://example.com/feed.xml")
    )
    # Third item has no description or author
    assert result.items[2].summary == ""
    assert result.items[2].author == ""


@respx.mock
async def test_rss_http_error() -> None:
    respx.get("https://example.com/bad.xml").mock(
        return_value=httpx.Response(404, text="Not Found")
    )
    block = RssReaderBlock()
    with pytest.raises(httpx.HTTPStatusError):
        await block.execute(
            RssReaderInput(feed_url="https://example.com/bad.xml")
        )


async def test_rss_block_metadata() -> None:
    assert RssReaderBlock.block_type == "rss_reader"
    assert RssReaderBlock.cache_ttl == 300
