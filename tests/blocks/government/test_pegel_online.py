from __future__ import annotations

import httpx
import pytest
import respx

from llming_plumber.blocks.government.pegel_online import (
    PegelOnlineBlock,
    PegelOnlineInput,
    PegelOnlineOutput,
)

MOCK_STATIONS = [
    {
        "longname": "BONN",
        "shortname": "BONN",
        "water": {"longname": "RHEIN", "shortname": "RHEIN"},
        "timeseries": [
            {
                "shortname": "W",
                "currentMeasurement": {
                    "value": 245.0,
                    "timestamp": "2026-03-06T14:00:00+01:00",
                    "trend": -1,
                },
            }
        ],
    },
    {
        "longname": "KÖLN",
        "shortname": "KOELN",
        "water": {"longname": "RHEIN", "shortname": "RHEIN"},
        "timeseries": [
            {
                "shortname": "W",
                "currentMeasurement": {
                    "value": 312.5,
                    "timestamp": "2026-03-06T14:00:00+01:00",
                    "trend": 1,
                },
            }
        ],
    },
]

MOCK_SINGLE_STATION = {
    "longname": "MAXAU",
    "shortname": "MAXAU",
    "water": {"longname": "RHEIN", "shortname": "RHEIN"},
    "timeseries": [
        {
            "shortname": "W",
            "currentMeasurement": {
                "value": 520.0,
                "timestamp": "2026-03-06T14:00:00+01:00",
                "trend": 0,
            },
        }
    ],
}


@respx.mock
async def test_pegel_list_by_water() -> None:
    respx.get(
        "https://www.pegelonline.wsv.de/webservices/rest-api/v2/stations.json"
    ).mock(return_value=httpx.Response(200, json=MOCK_STATIONS))

    block = PegelOnlineBlock()
    result = await block.execute(PegelOnlineInput(water_name="RHEIN"))
    assert isinstance(result, PegelOnlineOutput)
    assert len(result.stations) == 2
    assert result.stations[0].station_name == "BONN"
    assert result.stations[0].water_name == "RHEIN"
    assert result.stations[0].level_cm == 245.0
    assert result.stations[0].trend == "falling"
    assert result.stations[1].level_cm == 312.5
    assert result.stations[1].trend == "rising"


@respx.mock
async def test_pegel_single_station() -> None:
    respx.get(
        "https://www.pegelonline.wsv.de/webservices/rest-api/v2/stations/abc123.json"
    ).mock(return_value=httpx.Response(200, json=MOCK_SINGLE_STATION))

    block = PegelOnlineBlock()
    result = await block.execute(PegelOnlineInput(station_id="abc123"))
    assert len(result.stations) == 1
    assert result.stations[0].station_name == "MAXAU"
    assert result.stations[0].level_cm == 520.0
    assert result.stations[0].trend == "stable"


@respx.mock
async def test_pegel_no_water_level_timeseries() -> None:
    data = [
        {
            "longname": "HAMBURG",
            "water": {"longname": "ELBE"},
            "timeseries": [{"shortname": "Q", "currentMeasurement": {"value": 100}}],
        }
    ]
    respx.get(
        "https://www.pegelonline.wsv.de/webservices/rest-api/v2/stations.json"
    ).mock(return_value=httpx.Response(200, json=data))

    block = PegelOnlineBlock()
    result = await block.execute(PegelOnlineInput())
    assert result.stations[0].level_cm is None
    assert result.stations[0].trend == ""


@respx.mock
async def test_pegel_empty_response() -> None:
    respx.get(
        "https://www.pegelonline.wsv.de/webservices/rest-api/v2/stations.json"
    ).mock(return_value=httpx.Response(200, json=[]))

    block = PegelOnlineBlock()
    result = await block.execute(PegelOnlineInput())
    assert result.stations == []


@respx.mock
async def test_pegel_http_error() -> None:
    respx.get(
        "https://www.pegelonline.wsv.de/webservices/rest-api/v2/stations.json"
    ).mock(return_value=httpx.Response(500, text="Error"))
    block = PegelOnlineBlock()
    with pytest.raises(httpx.HTTPStatusError):
        await block.execute(PegelOnlineInput())


@respx.mock
async def test_pegel_water_filter_passed() -> None:
    route = respx.get(
        "https://www.pegelonline.wsv.de/webservices/rest-api/v2/stations.json"
    ).mock(return_value=httpx.Response(200, json=[]))

    block = PegelOnlineBlock()
    await block.execute(PegelOnlineInput(water_name="DONAU"))
    assert "waters=DONAU" in str(route.calls[0].request.url)


@respx.mock
async def test_pegel_non_list_response() -> None:
    respx.get(
        "https://www.pegelonline.wsv.de/webservices/rest-api/v2/stations.json"
    ).mock(return_value=httpx.Response(200, json={"unexpected": "format"}))

    block = PegelOnlineBlock()
    result = await block.execute(PegelOnlineInput())
    assert result.stations == []


async def test_pegel_block_metadata() -> None:
    assert PegelOnlineBlock.block_type == "pegel_online"
    assert PegelOnlineBlock.cache_ttl == 600
