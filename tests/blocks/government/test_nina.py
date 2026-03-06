from __future__ import annotations

import httpx
import pytest
import respx

from llming_plumber.blocks.government.nina import (
    NinaBlock,
    NinaInput,
    NinaOutput,
)

MOCK_WARNINGS = [
    {
        "id": "warn-001",
        "version": 1,
        "startDate": "2026-03-01T10:00:00Z",
        "severity": "Severe",
        "type": "Alert",
        "i18nTitle": {"de": "Unwetterwarnung Sturm"},
        "transKeys": {},
    },
    {
        "id": "warn-002",
        "version": 2,
        "startDate": "2026-03-02T08:00:00Z",
        "severity": "Minor",
        "type": "Update",
        "i18nTitle": {"de": "Hochwasserwarnung"},
        "transKeys": {},
    },
]


@respx.mock
async def test_fetch_mowas_warnings() -> None:
    respx.get("https://nina.api.bund.dev/api31/warnings/mowas.json").mock(
        return_value=httpx.Response(200, json=MOCK_WARNINGS)
    )
    block = NinaBlock()
    result = await block.execute(NinaInput())
    assert isinstance(result, NinaOutput)
    assert len(result.warnings) == 2
    assert result.warnings[0].id == "warn-001"
    assert result.warnings[0].title == "Unwetterwarnung Sturm"
    assert result.warnings[0].severity == "Severe"
    assert result.warnings[0].warning_type == "mowas"


@respx.mock
async def test_fetch_dwd_warnings() -> None:
    dwd_data = [
        {
            "id": "dwd-100",
            "version": 1,
            "startDate": "2026-03-05T12:00:00Z",
            "severity": "Moderate",
            "type": "Alert",
            "i18nTitle": {"de": "Hitzewarnung"},
            "transKeys": {},
        }
    ]
    respx.get("https://nina.api.bund.dev/api31/warnings/dwd.json").mock(
        return_value=httpx.Response(200, json=dwd_data)
    )
    block = NinaBlock()
    result = await block.execute(NinaInput(warning_type="dwd"))
    assert len(result.warnings) == 1
    assert result.warnings[0].warning_type == "dwd"
    assert result.warnings[0].title == "Hitzewarnung"


@respx.mock
async def test_empty_warnings() -> None:
    respx.get("https://nina.api.bund.dev/api31/warnings/katwarn.json").mock(
        return_value=httpx.Response(200, json=[])
    )
    block = NinaBlock()
    result = await block.execute(NinaInput(warning_type="katwarn"))
    assert result.warnings == []


@respx.mock
async def test_warning_start_date() -> None:
    respx.get("https://nina.api.bund.dev/api31/warnings/mowas.json").mock(
        return_value=httpx.Response(200, json=MOCK_WARNINGS)
    )
    block = NinaBlock()
    result = await block.execute(NinaInput())
    assert result.warnings[0].start_date == "2026-03-01T10:00:00Z"
    assert result.warnings[1].start_date == "2026-03-02T08:00:00Z"


@respx.mock
async def test_http_error() -> None:
    respx.get("https://nina.api.bund.dev/api31/warnings/biwapp.json").mock(
        return_value=httpx.Response(503, text="Service Unavailable")
    )
    block = NinaBlock()
    with pytest.raises(httpx.HTTPStatusError):
        await block.execute(NinaInput(warning_type="biwapp"))


async def test_block_metadata() -> None:
    assert NinaBlock.block_type == "nina"
    assert NinaBlock.cache_ttl == 300
