"""German public holidays from feiertage-api.de — no API key needed."""

from __future__ import annotations

from typing import Any, ClassVar

import httpx
from pydantic import BaseModel, Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput

FEIERTAGE_API_BASE = "https://feiertage-api.de/api/"


class Holiday(BaseModel):
    name: str
    date: str
    note: str


class FeiertageInput(BlockInput):
    year: int = Field(
        title="Year",
        description="Year for which to retrieve holidays",
    )
    state: str | None = Field(
        default=None,
        title="State",
        description=(
            "German state code"
            " (BW, BY, BE, BB, HB, HH, HE, MV, NI, NW, RP, SL, SN, ST, SH, TH)"
        ),
        json_schema_extra={"placeholder": "BW"},
    )


class FeiertageOutput(BlockOutput):
    holidays: list[Holiday]


class FeiertageBlock(BaseBlock[FeiertageInput, FeiertageOutput]):
    block_type: ClassVar[str] = "feiertage"
    icon: ClassVar[str] = "tabler/calendar-event"
    categories: ClassVar[list[str]] = ["government/legal"]
    description: ClassVar[str] = "German public holidays by state and year"
    cache_ttl: ClassVar[int] = 86400

    async def execute(
        self, input: FeiertageInput, ctx: BlockContext | None = None
    ) -> FeiertageOutput:
        params: dict[str, str | int] = {"jahr": input.year}
        if input.state is not None:
            params["nur_land"] = input.state

        async with httpx.AsyncClient() as client:
            resp = await client.get(FEIERTAGE_API_BASE, params=params)
            resp.raise_for_status()
            data: dict[str, Any] = resp.json()

        holidays: list[Holiday] = []
        for name, info in data.items():
            holidays.append(
                Holiday(
                    name=name,
                    date=str(info.get("datum", "")),
                    note=str(info.get("hinweis", "")),
                )
            )

        return FeiertageOutput(holidays=holidays)
