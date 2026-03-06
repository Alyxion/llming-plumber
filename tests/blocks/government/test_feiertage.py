from __future__ import annotations

import httpx
import pytest
import respx

from llming_plumber.blocks.government.feiertage import (
    FeiertageBlock,
    FeiertageInput,
    FeiertageOutput,
)

MOCK_HOLIDAYS = {
    "Neujahrstag": {"datum": "2026-01-01", "hinweis": ""},
    "Tag der Arbeit": {"datum": "2026-05-01", "hinweis": ""},
    "Tag der Deutschen Einheit": {"datum": "2026-10-03", "hinweis": ""},
}


@respx.mock
async def test_fetch_holidays_all_states() -> None:
    respx.get("https://feiertage-api.de/api/", params={"jahr": "2026"}).mock(
        return_value=httpx.Response(200, json=MOCK_HOLIDAYS)
    )
    block = FeiertageBlock()
    result = await block.execute(FeiertageInput(year=2026))
    assert isinstance(result, FeiertageOutput)
    assert len(result.holidays) == 3
    names = [h.name for h in result.holidays]
    assert "Neujahrstag" in names


@respx.mock
async def test_fetch_holidays_with_state() -> None:
    state_holidays = {
        "Neujahrstag": {"datum": "2026-01-01", "hinweis": ""},
        "Heilige Drei Könige": {"datum": "2026-01-06", "hinweis": ""},
    }
    respx.get(
        "https://feiertage-api.de/api/",
        params={"jahr": "2026", "nur_land": "BW"},
    ).mock(return_value=httpx.Response(200, json=state_holidays))

    block = FeiertageBlock()
    result = await block.execute(FeiertageInput(year=2026, state="BW"))
    assert len(result.holidays) == 2
    names = [h.name for h in result.holidays]
    assert "Heilige Drei Könige" in names


@respx.mock
async def test_holiday_date_and_note() -> None:
    data = {
        "Reformationstag": {
            "datum": "2026-10-31",
            "hinweis": "Nur in einigen Ländern",
        }
    }
    respx.get("https://feiertage-api.de/api/", params={"jahr": "2026"}).mock(
        return_value=httpx.Response(200, json=data)
    )
    block = FeiertageBlock()
    result = await block.execute(FeiertageInput(year=2026))
    assert result.holidays[0].date == "2026-10-31"
    assert result.holidays[0].note == "Nur in einigen Ländern"


@respx.mock
async def test_empty_response() -> None:
    respx.get("https://feiertage-api.de/api/", params={"jahr": "2026"}).mock(
        return_value=httpx.Response(200, json={})
    )
    block = FeiertageBlock()
    result = await block.execute(FeiertageInput(year=2026))
    assert result.holidays == []


@respx.mock
async def test_http_error() -> None:
    respx.get("https://feiertage-api.de/api/", params={"jahr": "1800"}).mock(
        return_value=httpx.Response(500, text="Server Error")
    )
    block = FeiertageBlock()
    with pytest.raises(httpx.HTTPStatusError):
        await block.execute(FeiertageInput(year=1800))


async def test_block_metadata() -> None:
    assert FeiertageBlock.block_type == "feiertage"
    assert FeiertageBlock.cache_ttl == 86400
