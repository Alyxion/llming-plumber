"""NewsAPI.org block — requires API key."""

from __future__ import annotations

import os
from typing import Any, ClassVar

import httpx
from pydantic import BaseModel, Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput


def _newsapi_key() -> str:
    return os.environ.get("NEWSAPI_KEY", "")


def _newsapi_base() -> str:
    return os.environ.get("NEWSAPI_BASE", "https://newsapi.org/v2")


class NewsArticle(BaseModel):
    source_name: str
    author: str = ""
    title: str
    description: str = ""
    url: str = ""
    published_at: str = ""
    content: str = ""


class NewsApiInput(BlockInput):
    api_key: str = Field(
        default_factory=_newsapi_key,
        title="API Key",
        description="NewsAPI.org API key (defaults to NEWSAPI_KEY env var)",
        json_schema_extra={"secret": True},
    )
    query: str | None = Field(
        default=None,
        title="Query",
        description="Search query for news articles",
        json_schema_extra={"placeholder": "climate change"},
    )
    country: str | None = Field(
        default=None,
        title="Country",
        description="Two-letter country code",
        json_schema_extra={"placeholder": "de"},
    )
    category: str | None = Field(
        default=None,
        title="Category",
        description="News category to filter by",
        json_schema_extra={
            "widget": "select",
            "options": [
                "business", "entertainment", "general", "health",
                "science", "sports", "technology",
            ],
        },
    )
    page_size: int = Field(
        default=20,
        title="Page Size",
        description="Number of articles to return",
    )
    endpoint: str = Field(
        default="top-headlines",
        title="Endpoint",
        description="NewsAPI endpoint to use",
        json_schema_extra={
            "widget": "select",
            "options": ["top-headlines", "everything"],
        },
    )
    api_base: str = Field(
        default_factory=_newsapi_base,
        title="API Base URL",
        description="NewsAPI base URL",
        json_schema_extra={"group": "advanced"},
    )


class NewsApiOutput(BlockOutput):
    articles: list[NewsArticle]
    total_results: int


class NewsApiBlock(BaseBlock[NewsApiInput, NewsApiOutput]):
    block_type: ClassVar[str] = "news_api"
    icon: ClassVar[str] = "tabler/newspaper"
    categories: ClassVar[list[str]] = ["news/api"]
    description: ClassVar[str] = "Search news via NewsAPI.org"
    cache_ttl: ClassVar[int] = 900

    async def execute(
        self, input: NewsApiInput, ctx: BlockContext | None = None
    ) -> NewsApiOutput:
        params: dict[str, str | int] = {
            "apiKey": input.api_key,
            "pageSize": input.page_size,
        }
        if input.query:
            params["q"] = input.query
        if input.country:
            params["country"] = input.country
        if input.category:
            params["category"] = input.category

        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{input.api_base}/{input.endpoint}", params=params
            )
            resp.raise_for_status()
            data: dict[str, Any] = resp.json()

        articles: list[NewsArticle] = []
        for article in data.get("articles", []):
            source = article.get("source", {})
            articles.append(
                NewsArticle(
                    source_name=str(source.get("name", "")),
                    author=str(article.get("author", "") or ""),
                    title=str(article.get("title", "")),
                    description=str(article.get("description", "") or ""),
                    url=str(article.get("url", "")),
                    published_at=str(article.get("publishedAt", "")),
                    content=str(article.get("content", "") or ""),
                )
            )

        return NewsApiOutput(
            articles=articles, total_results=int(data.get("totalResults", 0))
        )
