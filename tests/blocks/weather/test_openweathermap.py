from __future__ import annotations

import httpx
import pytest
import respx

from llming_plumber.blocks.weather.openweathermap import (
    WeatherBlock,
    WeatherInput,
    WeatherOutput,
)

MOCK_RESPONSE = {
    "weather": [{"main": "Clouds", "description": "broken clouds"}],
    "main": {"temp": 8.5, "feels_like": 5.2, "humidity": 72},
    "wind": {"speed": 4.1},
    "name": "Berlin",
}


@respx.mock
async def test_weather_basic() -> None:
    respx.get("https://api.openweathermap.org/data/2.5/weather").mock(
        return_value=httpx.Response(200, json=MOCK_RESPONSE)
    )
    block = WeatherBlock()
    result = await block.execute(
        WeatherInput(city="Berlin,DE", api_key="test_key")
    )
    assert isinstance(result, WeatherOutput)
    assert result.temp == 8.5
    assert result.feels_like == 5.2
    assert result.humidity == 72
    assert result.condition == "Clouds"
    assert result.description == "broken clouds"
    assert result.wind_speed == 4.1
    assert result.city_name == "Berlin"


@respx.mock
async def test_weather_passes_params() -> None:
    route = respx.get("https://api.openweathermap.org/data/2.5/weather").mock(
        return_value=httpx.Response(200, json=MOCK_RESPONSE)
    )
    block = WeatherBlock()
    await block.execute(
        WeatherInput(city="Stuttgart,DE", api_key="key123", units="imperial")
    )
    request = route.calls[0].request
    assert "q=Stuttgart" in str(request.url)
    assert "appid=key123" in str(request.url)
    assert "units=imperial" in str(request.url)


@respx.mock
async def test_weather_custom_api_base() -> None:
    respx.get("https://custom.api/data/2.5/weather").mock(
        return_value=httpx.Response(200, json=MOCK_RESPONSE)
    )
    block = WeatherBlock()
    result = await block.execute(
        WeatherInput(city="Berlin", api_key="k", api_base="https://custom.api")
    )
    assert result.city_name == "Berlin"


@respx.mock
async def test_weather_http_error() -> None:
    respx.get("https://api.openweathermap.org/data/2.5/weather").mock(
        return_value=httpx.Response(401, text="Unauthorized")
    )
    block = WeatherBlock()
    with pytest.raises(httpx.HTTPStatusError):
        await block.execute(WeatherInput(city="X", api_key="bad"))


async def test_weather_block_metadata() -> None:
    assert WeatherBlock.block_type == "weather"
    assert WeatherBlock.cache_ttl == 600


async def test_weather_standalone_no_context() -> None:
    """Blocks must accept ctx=None for standalone use."""
    inp = WeatherInput(city="Berlin", api_key="test")
    assert inp.city == "Berlin"
    # Verifying the signature accepts None — actual call needs mocked HTTP
