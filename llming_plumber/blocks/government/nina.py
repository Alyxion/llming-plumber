"""NINA civil protection warnings — German government API, no key needed."""

from __future__ import annotations

from typing import Any, ClassVar

import httpx
from pydantic import BaseModel, Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput

NINA_API_BASE = "https://nina.api.bund.dev/api31/warnings"


class NinaWarning(BaseModel):
    id: str
    title: str
    severity: str
    start_date: str
    warning_type: str


class NinaInput(BlockInput):
    warning_type: str = Field(
        default="mowas",
        title="Warning Type",
        description="Type of civil protection warning source",
        json_schema_extra={
            "widget": "select",
            "options": ["mowas", "katwarn", "biwapp", "dwd", "lhp", "police"],
        },
    )


class NinaOutput(BlockOutput):
    warnings: list[NinaWarning]


class NinaBlock(BaseBlock[NinaInput, NinaOutput]):
    block_type: ClassVar[str] = "nina"
    icon: ClassVar[str] = "tabler/alert-triangle"
    categories: ClassVar[list[str]] = ["government/safety"]
    description: ClassVar[str] = "German civil protection warnings (NINA)"
    cache_ttl: ClassVar[int] = 300

    async def execute(
        self, input: NinaInput, ctx: BlockContext | None = None
    ) -> NinaOutput:
        url = f"{NINA_API_BASE}/{input.warning_type}.json"

        async with httpx.AsyncClient() as client:
            resp = await client.get(url)
            resp.raise_for_status()
            data: list[dict[str, Any]] = resp.json()

        warnings: list[NinaWarning] = []
        for entry in data:
            i18n_title: dict[str, str] = entry.get("i18nTitle", {})
            title = i18n_title.get("de", "")

            warnings.append(
                NinaWarning(
                    id=str(entry.get("id", "")),
                    title=title,
                    severity=str(entry.get("severity", "")),
                    start_date=str(entry.get("startDate", "")),
                    warning_type=input.warning_type,
                )
            )

        return NinaOutput(warnings=warnings)
