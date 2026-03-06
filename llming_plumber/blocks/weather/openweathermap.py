"""OpenWeatherMap current weather block."""

from __future__ import annotations

from typing import Any, ClassVar

import httpx
from pydantic import Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput


class WeatherInput(BlockInput):
    city: str = Field(
        title="City",
        description="City name, optionally with country code",
        json_schema_extra={"placeholder": "Berlin,DE"},
    )
    api_key: str = Field(
        title="API Key",
        description="OpenWeatherMap API key",
        json_schema_extra={"secret": True},
    )
    units: str = Field(
        default="metric",
        title="Units",
        description="Temperature units",
        json_schema_extra={
            "widget": "select",
            "options": ["metric", "imperial", "standard"],
        },
    )
    api_base: str = Field(
        default="https://api.openweathermap.org",
        title="API Base URL",
        description="OpenWeatherMap API base URL",
        json_schema_extra={
            "placeholder": "https://api.openweathermap.org",
            "group": "advanced",
        },
    )


class WeatherOutput(BlockOutput):
    temp: float
    feels_like: float
    humidity: int
    condition: str
    description: str
    wind_speed: float
    city_name: str


class WeatherBlock(BaseBlock[WeatherInput, WeatherOutput]):
    block_type: ClassVar[str] = "weather"
    icon: ClassVar[str] = "tabler/cloud"
    categories: ClassVar[list[str]] = ["weather"]
    description: ClassVar[str] = "Fetch current weather from OpenWeatherMap"
    cache_ttl: ClassVar[int] = 600

    async def execute(
        self, input: WeatherInput, ctx: BlockContext | None = None
    ) -> WeatherOutput:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{input.api_base}/data/2.5/weather",
                params={
                    "q": input.city,
                    "appid": input.api_key,
                    "units": input.units,
                },
            )
            resp.raise_for_status()
            data: dict[str, Any] = resp.json()

        weather_entry = data["weather"][0]
        main = data["main"]

        return WeatherOutput(
            temp=float(main["temp"]),
            feels_like=float(main["feels_like"]),
            humidity=int(main["humidity"]),
            condition=str(weather_entry["main"]),
            description=str(weather_entry["description"]),
            wind_speed=float(data["wind"]["speed"]),
            city_name=str(data["name"]),
        )
