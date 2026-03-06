"""Pegel Online block — German waterway levels, no API key needed."""

from __future__ import annotations

from typing import Any, ClassVar

import httpx
from pydantic import BaseModel, Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput

PEGEL_API_BASE = "https://www.pegelonline.wsv.de/webservices/rest-api/v2"


class WaterLevel(BaseModel):
    station_name: str
    water_name: str
    level_cm: float | None = None
    trend: str = ""
    timestamp: str = ""


class PegelOnlineInput(BlockInput):
    station_id: str | None = Field(
        default=None,
        title="Station ID",
        description="Pegel Online station identifier",
        json_schema_extra={"placeholder": "593647aa-9fea-43ec-a7d6-6476a76ae868"},
    )
    water_name: str | None = Field(
        default=None,
        title="Water Name",
        description="Name of the waterway to filter by",
        json_schema_extra={"placeholder": "RHEIN"},
    )


class PegelOnlineOutput(BlockOutput):
    stations: list[WaterLevel]


class PegelOnlineBlock(BaseBlock[PegelOnlineInput, PegelOnlineOutput]):
    block_type: ClassVar[str] = "pegel_online"
    icon: ClassVar[str] = "tabler/ripple"
    categories: ClassVar[list[str]] = ["government/environment"]
    description: ClassVar[str] = "German waterway levels from Pegel Online"
    cache_ttl: ClassVar[int] = 600

    async def execute(
        self, input: PegelOnlineInput, ctx: BlockContext | None = None
    ) -> PegelOnlineOutput:
        async with httpx.AsyncClient() as client:
            if input.station_id:
                resp = await client.get(
                    f"{PEGEL_API_BASE}/stations/{input.station_id}.json",
                    params={
                        "includeTimeseries": "true",
                        "includeCurrentMeasurement": "true",
                    },
                )
                resp.raise_for_status()
                station_data: dict[str, Any] = resp.json()
                stations_raw = [station_data]
            else:
                params: dict[str, str] = {}
                if input.water_name:
                    params["waters"] = input.water_name
                params["includeTimeseries"] = "true"
                params["includeCurrentMeasurement"] = "true"
                resp = await client.get(
                    f"{PEGEL_API_BASE}/stations.json", params=params
                )
                resp.raise_for_status()
                stations_raw = resp.json()
                if not isinstance(stations_raw, list):
                    stations_raw = []

        stations: list[WaterLevel] = []
        for s in stations_raw:
            level: float | None = None
            trend = ""
            timestamp = ""

            timeseries: list[dict[str, Any]] = s.get("timeseries", [])
            for ts in timeseries:
                if ts.get("shortname") == "W":
                    cm = ts.get("currentMeasurement", {})
                    raw_value = cm.get("value")
                    if raw_value is not None:
                        level = float(raw_value)
                    timestamp = str(cm.get("timestamp", ""))
                    trend_val = cm.get("trend")
                    if isinstance(trend_val, (int, float)):
                        if trend_val > 0:
                            trend = "rising"
                        elif trend_val < 0:
                            trend = "falling"
                        else:
                            trend = "stable"
                    break

            water = s.get("water", {})
            stations.append(
                WaterLevel(
                    station_name=str(s.get("longname", s.get("shortname", ""))),
                    water_name=str(water.get("longname", water.get("shortname", ""))),
                    level_cm=level,
                    trend=trend,
                    timestamp=timestamp,
                )
            )

        return PegelOnlineOutput(stations=stations)
