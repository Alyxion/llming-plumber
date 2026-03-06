from __future__ import annotations

import httpx
import pytest
import respx

from llming_plumber.blocks.news.tagesschau import (
    TagesschauBlock,
    TagesschauInput,
    TagesschauOutput,
)

MOCK_RESPONSE = {
    "news": [
        {
            "title": "Bundestagsdebatte zu Klimaschutz",
            "topline": "Klimaschutz",
            "firstSentence": "Der Bundestag debattiert heute...",
            "detailsweb": "https://www.tagesschau.de/inland/klima-123.html",
            "date": "2026-03-06T14:00:00.000+01:00",
            "tags": [{"tag": "Klimaschutz"}, {"tag": "Bundestag"}],
        },
        {
            "title": "Wirtschaftswachstum im Quartal",
            "topline": "Wirtschaft",
            "firstSentence": "Die deutsche Wirtschaft...",
            "detailsweb": "https://www.tagesschau.de/wirtschaft/wachstum-456.html",
            "date": "2026-03-06T12:00:00.000+01:00",
            "tags": [],
        },
    ]
}


@respx.mock
async def test_tagesschau_basic() -> None:
    respx.get("https://www.tagesschau.de/api2/news/").mock(
        return_value=httpx.Response(200, json=MOCK_RESPONSE)
    )
    block = TagesschauBlock()
    result = await block.execute(TagesschauInput())
    assert isinstance(result, TagesschauOutput)
    assert len(result.articles) == 2
    assert result.articles[0].title == "Bundestagsdebatte zu Klimaschutz"
    assert result.articles[0].topline == "Klimaschutz"
    assert result.articles[0].tags == ["Klimaschutz", "Bundestag"]
    assert "tagesschau.de" in result.articles[0].url


@respx.mock
async def test_tagesschau_max_items() -> None:
    respx.get("https://www.tagesschau.de/api2/news/").mock(
        return_value=httpx.Response(200, json=MOCK_RESPONSE)
    )
    block = TagesschauBlock()
    result = await block.execute(TagesschauInput(max_items=1))
    assert len(result.articles) == 1


@respx.mock
async def test_tagesschau_region_param() -> None:
    route = respx.get("https://www.tagesschau.de/api2/news/").mock(
        return_value=httpx.Response(200, json={"news": []})
    )
    block = TagesschauBlock()
    await block.execute(TagesschauInput(region=1))
    assert "regions=1" in str(route.calls[0].request.url)


@respx.mock
async def test_tagesschau_no_region_param() -> None:
    route = respx.get("https://www.tagesschau.de/api2/news/").mock(
        return_value=httpx.Response(200, json={"news": []})
    )
    block = TagesschauBlock()
    await block.execute(TagesschauInput())
    assert "regions" not in str(route.calls[0].request.url)


@respx.mock
async def test_tagesschau_empty_news() -> None:
    respx.get("https://www.tagesschau.de/api2/news/").mock(
        return_value=httpx.Response(200, json={"news": []})
    )
    block = TagesschauBlock()
    result = await block.execute(TagesschauInput())
    assert result.articles == []


@respx.mock
async def test_tagesschau_http_error() -> None:
    respx.get("https://www.tagesschau.de/api2/news/").mock(
        return_value=httpx.Response(503, text="Service Unavailable")
    )
    block = TagesschauBlock()
    with pytest.raises(httpx.HTTPStatusError):
        await block.execute(TagesschauInput())


@respx.mock
async def test_tagesschau_malformed_tags() -> None:
    data = {
        "news": [
            {
                "title": "Test",
                "tags": ["not-a-dict", {"tag": "valid"}, {"tag": 123}],
            }
        ]
    }
    respx.get("https://www.tagesschau.de/api2/news/").mock(
        return_value=httpx.Response(200, json=data)
    )
    block = TagesschauBlock()
    result = await block.execute(TagesschauInput())
    assert result.articles[0].tags == ["valid"]


async def test_tagesschau_block_metadata() -> None:
    assert TagesschauBlock.block_type == "tagesschau"
    assert TagesschauBlock.cache_ttl == 300
