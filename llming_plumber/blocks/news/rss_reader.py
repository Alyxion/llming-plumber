"""RSS/Atom feed reader block."""

from __future__ import annotations

import asyncio
from typing import Any, ClassVar

import feedparser  # type: ignore[import-untyped]  # no type stubs available
import httpx
from pydantic import BaseModel, Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput


class FeedItem(BaseModel):
    title: str
    link: str
    summary: str = ""
    published: str = ""
    author: str = ""


class RssReaderInput(BlockInput):
    feed_url: str = Field(
        title="Feed URL",
        description="URL of the RSS or Atom feed",
        json_schema_extra={"placeholder": "https://example.com/feed.xml"},
    )
    max_items: int = Field(
        default=50,
        title="Max Items",
        description="Maximum number of feed items to return",
    )


class RssReaderOutput(BlockOutput):
    items: list[FeedItem]
    feed_title: str = ""


class RssReaderBlock(BaseBlock[RssReaderInput, RssReaderOutput]):
    block_type: ClassVar[str] = "rss_reader"
    icon: ClassVar[str] = "tabler/rss"
    categories: ClassVar[list[str]] = ["news/feeds", "web"]
    description: ClassVar[str] = "Read and parse RSS/Atom feeds"
    cache_ttl: ClassVar[int] = 300

    async def execute(
        self, input: RssReaderInput, ctx: BlockContext | None = None
    ) -> RssReaderOutput:
        async with httpx.AsyncClient() as client:
            resp = await client.get(input.feed_url)
            resp.raise_for_status()
            raw_xml = resp.text

        parsed: Any = await asyncio.to_thread(feedparser.parse, raw_xml)

        items: list[FeedItem] = []
        for entry in parsed.entries[: input.max_items]:
            items.append(
                FeedItem(
                    title=str(entry.get("title", "")),
                    link=str(entry.get("link", "")),
                    summary=str(entry.get("summary", "")),
                    published=str(entry.get("published", "")),
                    author=str(entry.get("author", "")),
                )
            )

        feed_title = str(parsed.feed.get("title", "")) if parsed.feed else ""

        return RssReaderOutput(items=items, feed_title=feed_title)
