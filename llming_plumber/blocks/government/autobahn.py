"""Autobahn API block — German highway info, no API key needed."""

from __future__ import annotations

from typing import Any, ClassVar

import httpx
from pydantic import BaseModel, Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput

AUTOBAHN_API_BASE = "https://verkehr.autobahn.de/o/autobahn"


class AutobahnEvent(BaseModel):
    title: str
    subtitle: str = ""
    description: list[str] = []
    coordinate: str = ""
    road_id: str = ""


class AutobahnInput(BlockInput):
    road_id: str = Field(
        title="Road ID",
        description="German highway identifier",
        json_schema_extra={"placeholder": "A8"},
    )
    info_type: str = Field(
        default="roadworks",
        title="Info Type",
        description="Type of highway information to retrieve",
        json_schema_extra={
            "widget": "select",
            "options": ["roadworks", "warning", "closure"],
        },
    )


class AutobahnOutput(BlockOutput):
    events: list[AutobahnEvent]
    road_id: str


class AutobahnBlock(BaseBlock[AutobahnInput, AutobahnOutput]):
    block_type: ClassVar[str] = "autobahn"
    icon: ClassVar[str] = "tabler/road"
    categories: ClassVar[list[str]] = ["government/transport"]
    description: ClassVar[str] = "German highway roadworks, warnings, and closures"
    cache_ttl: ClassVar[int] = 600

    async def execute(
        self, input: AutobahnInput, ctx: BlockContext | None = None
    ) -> AutobahnOutput:
        url = f"{AUTOBAHN_API_BASE}/{input.road_id}/services/{input.info_type}"

        async with httpx.AsyncClient() as client:
            resp = await client.get(url)
            resp.raise_for_status()
            data: dict[str, Any] = resp.json()

        raw_events: list[dict[str, Any]] = data.get(input.info_type, [])
        events: list[AutobahnEvent] = []

        for entry in raw_events:
            coord = entry.get("coordinate", {})
            coord_str = ""
            if isinstance(coord, dict) and coord.get("lat") and coord.get("long"):
                coord_str = f"{coord['lat']},{coord['long']}"

            desc_lines: list[str] = []
            for line in entry.get("description", []):
                if isinstance(line, str):
                    desc_lines.append(line)

            events.append(
                AutobahnEvent(
                    title=str(entry.get("title", "")),
                    subtitle=str(entry.get("subtitle", "")),
                    description=desc_lines,
                    coordinate=coord_str,
                    road_id=input.road_id,
                )
            )

        return AutobahnOutput(events=events, road_id=input.road_id)
