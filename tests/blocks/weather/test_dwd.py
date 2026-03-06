from __future__ import annotations

import httpx
import pytest
import respx

from llming_plumber.blocks.weather.dwd import (
    DwdWeatherBlock,
    DwdWeatherInput,
    DwdWeatherOutput,
)

MOCK_RESPONSE = {
    "weather": {
        "temperature": 12.3,
        "relative_humidity": 65,
        "wind_speed_10": 18.7,
        "condition": "partly-cloudy",
        "icon": "partly-cloudy-day",
    },
    "sources": [
        {"station_name": "Stuttgart-Echterdingen"},
    ],
}


@respx.mock
async def test_dwd_basic() -> None:
    respx.get("https://api.brightsky.dev/current_weather").mock(
        return_value=httpx.Response(200, json=MOCK_RESPONSE)
    )
    block = DwdWeatherBlock()
    result = await block.execute(DwdWeatherInput(lat=48.69, lon=9.14))
    assert isinstance(result, DwdWeatherOutput)
    assert result.temperature == 12.3
    assert result.humidity == 65
    assert result.wind_speed == 18.7
    assert result.condition == "partly-cloudy"
    assert result.icon == "partly-cloudy-day"
    assert result.source_station == "Stuttgart-Echterdingen"


@respx.mock
async def test_dwd_passes_coordinates() -> None:
    route = respx.get("https://api.brightsky.dev/current_weather").mock(
        return_value=httpx.Response(200, json=MOCK_RESPONSE)
    )
    block = DwdWeatherBlock()
    await block.execute(DwdWeatherInput(lat=52.52, lon=13.41))
    request = route.calls[0].request
    assert "lat=52.52" in str(request.url)
    assert "lon=13.41" in str(request.url)


@respx.mock
async def test_dwd_no_sources() -> None:
    data = {**MOCK_RESPONSE, "sources": []}
    respx.get("https://api.brightsky.dev/current_weather").mock(
        return_value=httpx.Response(200, json=data)
    )
    block = DwdWeatherBlock()
    result = await block.execute(DwdWeatherInput(lat=0, lon=0))
    assert result.source_station == ""


@respx.mock
async def test_dwd_nullable_fields() -> None:
    data = {
        "weather": {
            "temperature": None,
            "relative_humidity": None,
            "wind_speed_10": None,
            "condition": "unknown",
            "icon": "",
        },
        "sources": [],
    }
    respx.get("https://api.brightsky.dev/current_weather").mock(
        return_value=httpx.Response(200, json=data)
    )
    block = DwdWeatherBlock()
    result = await block.execute(DwdWeatherInput(lat=0, lon=0))
    assert result.temperature is None
    assert result.humidity is None
    assert result.wind_speed is None


@respx.mock
async def test_dwd_http_error() -> None:
    respx.get("https://api.brightsky.dev/current_weather").mock(
        return_value=httpx.Response(500, text="Internal Server Error")
    )
    block = DwdWeatherBlock()
    with pytest.raises(httpx.HTTPStatusError):
        await block.execute(DwdWeatherInput(lat=0, lon=0))


async def test_dwd_block_metadata() -> None:
    assert DwdWeatherBlock.block_type == "dwd_weather"
    assert DwdWeatherBlock.cache_ttl == 600
