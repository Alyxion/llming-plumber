"""Tagesschau news API block (German public broadcaster, no API key needed)."""

from __future__ import annotations

from typing import Any, ClassVar

import httpx
from pydantic import BaseModel, Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput

TAGESSCHAU_API_BASE = "https://www.tagesschau.de/api2"


class TagesschauArticle(BaseModel):
    title: str
    topline: str = ""
    first_sentence: str = ""
    url: str = ""
    date: str = ""
    tags: list[str] = []


class TagesschauInput(BlockInput):
    region: int | None = Field(
        default=None,
        title="Region",
        description="Tagesschau region code (e.g. 1=BW, 2=BY, 3=BE, etc.)",
    )
    max_items: int = Field(
        default=20,
        title="Max Items",
        description="Maximum number of articles to return",
    )


class TagesschauOutput(BlockOutput):
    articles: list[TagesschauArticle]


class TagesschauBlock(BaseBlock[TagesschauInput, TagesschauOutput]):
    block_type: ClassVar[str] = "tagesschau"
    icon: ClassVar[str] = "tabler/news"
    categories: ClassVar[list[str]] = ["news/api", "government/news"]
    description: ClassVar[str] = "Headlines from Tagesschau (free, no key)"
    cache_ttl: ClassVar[int] = 300

    async def execute(
        self, input: TagesschauInput, ctx: BlockContext | None = None
    ) -> TagesschauOutput:
        params: dict[str, str] = {}
        if input.region is not None:
            params["regions"] = str(input.region)

        async with httpx.AsyncClient() as client:
            resp = await client.get(f"{TAGESSCHAU_API_BASE}/news/", params=params)
            resp.raise_for_status()
            data: dict[str, Any] = resp.json()

        articles: list[TagesschauArticle] = []
        for entry in data.get("news", [])[: input.max_items]:
            tags: list[str] = []
            for tag in entry.get("tags", []):
                if isinstance(tag, dict):
                    tag_val = tag.get("tag", "")
                    if isinstance(tag_val, str):
                        tags.append(tag_val)

            articles.append(
                TagesschauArticle(
                    title=str(entry.get("title", "")),
                    topline=str(entry.get("topline", "")),
                    first_sentence=str(entry.get("firstSentence", "")),
                    url=str(entry.get("detailsweb", "")),
                    date=str(entry.get("date", "")),
                    tags=tags,
                )
            )

        return TagesschauOutput(articles=articles)
