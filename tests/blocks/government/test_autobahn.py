from __future__ import annotations

import httpx
import pytest
import respx

from llming_plumber.blocks.government.autobahn import (
    AutobahnBlock,
    AutobahnInput,
    AutobahnOutput,
)

MOCK_ROADWORKS = {
    "roadworks": [
        {
            "title": "A8 | AS Stuttgart-Degerloch",
            "subtitle": "Stuttgart Richtung Karlsruhe",
            "description": ["Fahrbahnerneuerung", "1 Fahrstreifen gesperrt"],
            "coordinate": {"lat": "48.735", "long": "9.167"},
        },
        {
            "title": "A8 | Leonberg",
            "subtitle": "Karlsruhe Richtung Stuttgart",
            "description": ["Brückenarbeiten"],
            "coordinate": {"lat": "48.800", "long": "9.010"},
        },
    ]
}


@respx.mock
async def test_autobahn_roadworks() -> None:
    respx.get(
        "https://verkehr.autobahn.de/o/autobahn/A8/services/roadworks"
    ).mock(return_value=httpx.Response(200, json=MOCK_ROADWORKS))

    block = AutobahnBlock()
    result = await block.execute(AutobahnInput(road_id="A8"))
    assert isinstance(result, AutobahnOutput)
    assert result.road_id == "A8"
    assert len(result.events) == 2
    assert result.events[0].title == "A8 | AS Stuttgart-Degerloch"
    assert result.events[0].description == [
        "Fahrbahnerneuerung",
        "1 Fahrstreifen gesperrt",
    ]
    assert "48.735" in result.events[0].coordinate


@respx.mock
async def test_autobahn_warnings() -> None:
    respx.get(
        "https://verkehr.autobahn.de/o/autobahn/A1/services/warning"
    ).mock(
        return_value=httpx.Response(200, json={"warning": []})
    )
    block = AutobahnBlock()
    result = await block.execute(
        AutobahnInput(road_id="A1", info_type="warning")
    )
    assert result.events == []
    assert result.road_id == "A1"


@respx.mock
async def test_autobahn_closure() -> None:
    data = {
        "closure": [
            {
                "title": "A7 Vollsperrung",
                "subtitle": "Hamburg Richtung Hannover",
                "description": ["Brückenabbruch"],
                "coordinate": {"lat": "53.0", "long": "10.0"},
            }
        ]
    }
    respx.get(
        "https://verkehr.autobahn.de/o/autobahn/A7/services/closure"
    ).mock(return_value=httpx.Response(200, json=data))

    block = AutobahnBlock()
    result = await block.execute(
        AutobahnInput(road_id="A7", info_type="closure")
    )
    assert len(result.events) == 1
    assert result.events[0].title == "A7 Vollsperrung"


@respx.mock
async def test_autobahn_no_coordinate() -> None:
    data = {"roadworks": [{"title": "Test", "description": []}]}
    respx.get(
        "https://verkehr.autobahn.de/o/autobahn/A5/services/roadworks"
    ).mock(return_value=httpx.Response(200, json=data))

    block = AutobahnBlock()
    result = await block.execute(AutobahnInput(road_id="A5"))
    assert result.events[0].coordinate == ""


@respx.mock
async def test_autobahn_http_error() -> None:
    respx.get(
        "https://verkehr.autobahn.de/o/autobahn/A99/services/roadworks"
    ).mock(return_value=httpx.Response(500, text="Error"))
    block = AutobahnBlock()
    with pytest.raises(httpx.HTTPStatusError):
        await block.execute(AutobahnInput(road_id="A99"))


async def test_autobahn_block_metadata() -> None:
    assert AutobahnBlock.block_type == "autobahn"
    assert AutobahnBlock.cache_ttl == 600
