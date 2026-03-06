"""DWD (Deutscher Wetterdienst) weather via Bright Sky API — no API key needed."""

from __future__ import annotations

from typing import Any, ClassVar

import httpx
from pydantic import Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput

BRIGHT_SKY_BASE = "https://api.brightsky.dev"


class DwdWeatherInput(BlockInput):
    lat: float = Field(
        title="Latitude",
        description="Latitude of the location",
        json_schema_extra={"placeholder": "48.78"},
    )
    lon: float = Field(
        title="Longitude",
        description="Longitude of the location",
        json_schema_extra={"placeholder": "9.18"},
    )


class DwdWeatherOutput(BlockOutput):
    temperature: float | None
    humidity: float | None
    wind_speed: float | None
    condition: str
    icon: str
    source_station: str


class DwdWeatherBlock(BaseBlock[DwdWeatherInput, DwdWeatherOutput]):
    block_type: ClassVar[str] = "dwd_weather"
    icon: ClassVar[str] = "tabler/cloud-rain"
    categories: ClassVar[list[str]] = ["weather", "government/weather"]
    description: ClassVar[str] = (
        "Current weather from Deutscher Wetterdienst (free, no key)"
    )
    cache_ttl: ClassVar[int] = 600

    async def execute(
        self, input: DwdWeatherInput, ctx: BlockContext | None = None
    ) -> DwdWeatherOutput:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{BRIGHT_SKY_BASE}/current_weather",
                params={"lat": str(input.lat), "lon": str(input.lon)},
            )
            resp.raise_for_status()
            data: dict[str, Any] = resp.json()

        weather: dict[str, Any] = data["weather"]
        sources: list[dict[str, Any]] = data.get("sources", [])
        station_name = str(sources[0]["station_name"]) if sources else ""

        return DwdWeatherOutput(
            temperature=weather.get("temperature"),
            humidity=weather.get("relative_humidity"),
            wind_speed=weather.get("wind_speed_10"),
            condition=str(weather.get("condition", "unknown")),
            icon=str(weather.get("icon", "")),
            source_station=station_name,
        )
