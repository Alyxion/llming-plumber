from __future__ import annotations

import httpx
import pytest
import respx

from llming_plumber.blocks.news.news_api import (
    NewsApiBlock,
    NewsApiInput,
    NewsApiOutput,
)

MOCK_RESPONSE = {
    "status": "ok",
    "totalResults": 2,
    "articles": [
        {
            "source": {"id": "faz", "name": "FAZ"},
            "author": "Max Mustermann",
            "title": "German Economy Grows",
            "description": "GDP increased by 0.5%...",
            "url": "https://faz.net/article-1",
            "publishedAt": "2026-03-06T10:00:00Z",
            "content": "Full content here...",
        },
        {
            "source": {"id": None, "name": "SZ"},
            "author": None,
            "title": "Weather Update",
            "description": None,
            "url": "https://sz.de/article-2",
            "publishedAt": "2026-03-06T09:00:00Z",
            "content": None,
        },
    ],
}


@respx.mock
async def test_news_api_basic() -> None:
    respx.get("https://newsapi.org/v2/top-headlines").mock(
        return_value=httpx.Response(200, json=MOCK_RESPONSE)
    )
    block = NewsApiBlock()
    result = await block.execute(
        NewsApiInput(api_key="test_key", country="de")
    )
    assert isinstance(result, NewsApiOutput)
    assert result.total_results == 2
    assert len(result.articles) == 2
    assert result.articles[0].source_name == "FAZ"
    assert result.articles[0].author == "Max Mustermann"
    assert result.articles[0].title == "German Economy Grows"


@respx.mock
async def test_news_api_null_fields() -> None:
    respx.get("https://newsapi.org/v2/top-headlines").mock(
        return_value=httpx.Response(200, json=MOCK_RESPONSE)
    )
    block = NewsApiBlock()
    result = await block.execute(NewsApiInput(api_key="k"))
    # Second article has null author, description, content
    assert result.articles[1].author == ""
    assert result.articles[1].description == ""
    assert result.articles[1].content == ""


@respx.mock
async def test_news_api_query_param() -> None:
    route = respx.get("https://newsapi.org/v2/everything").mock(
        return_value=httpx.Response(200, json={"articles": [], "totalResults": 0})
    )
    block = NewsApiBlock()
    await block.execute(
        NewsApiInput(api_key="k", query="climate", endpoint="everything")
    )
    url_str = str(route.calls[0].request.url)
    assert "q=climate" in url_str
    assert "everything" in url_str


@respx.mock
async def test_news_api_category_param() -> None:
    route = respx.get("https://newsapi.org/v2/top-headlines").mock(
        return_value=httpx.Response(200, json={"articles": [], "totalResults": 0})
    )
    block = NewsApiBlock()
    await block.execute(
        NewsApiInput(api_key="k", category="technology", country="de")
    )
    url_str = str(route.calls[0].request.url)
    assert "category=technology" in url_str
    assert "country=de" in url_str


@respx.mock
async def test_news_api_custom_base() -> None:
    respx.get("https://custom.api/v2/top-headlines").mock(
        return_value=httpx.Response(200, json={"articles": [], "totalResults": 0})
    )
    block = NewsApiBlock()
    result = await block.execute(
        NewsApiInput(api_key="k", api_base="https://custom.api/v2")
    )
    assert result.total_results == 0


@respx.mock
async def test_news_api_http_error() -> None:
    respx.get("https://newsapi.org/v2/top-headlines").mock(
        return_value=httpx.Response(429, text="Rate limited")
    )
    block = NewsApiBlock()
    with pytest.raises(httpx.HTTPStatusError):
        await block.execute(NewsApiInput(api_key="k"))


async def test_news_api_block_metadata() -> None:
    assert NewsApiBlock.block_type == "news_api"
    assert NewsApiBlock.cache_ttl == 900
