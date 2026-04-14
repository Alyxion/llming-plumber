"""Unit tests for ip_geolocation block — mocked HTTP calls."""

from __future__ import annotations

from typing import Any

import httpx
import pytest
import respx

from llming_plumber.blocks.data.ip_geolocation import (
    IPGeolocationBlock,
    IPGeolocationInput,
    IPGeolocationOutput,
    IPInfo,
    _parse_ip_info,
)

MOCK_SINGLE: dict[str, Any] = {
    "status": "success",
    "query": "8.8.8.8",
    "country": "United States",
    "countryCode": "US",
    "region": "VA",
    "regionName": "Virginia",
    "city": "Ashburn",
    "zip": "20149",
    "lat": 39.03,
    "lon": -77.5,
    "timezone": "America/New_York",
    "isp": "Google LLC",
    "org": "Google Public DNS",
    "as": "AS15169 Google LLC",
    "mobile": False,
    "proxy": False,
    "hosting": True,
}

MOCK_CURRENT_IP: dict[str, Any] = {
    "status": "success",
    "query": "203.0.113.1",
    "country": "Germany",
    "countryCode": "DE",
    "region": "BW",
    "regionName": "Baden-Württemberg",
    "city": "Stuttgart",
    "zip": "70173",
    "lat": 48.78,
    "lon": 9.18,
    "timezone": "Europe/Berlin",
    "isp": "Example ISP",
    "org": "Example Org",
    "as": "AS1234 Example",
    "mobile": False,
    "proxy": False,
    "hosting": False,
}

BASE = "https://pro.ip-api.com"


# ---------------------------------------------------------------------------
# _parse_ip_info
# ---------------------------------------------------------------------------


def test_parse_ip_info_basic() -> None:
    result = _parse_ip_info(MOCK_SINGLE)
    assert result["query"] == "8.8.8.8"
    assert result["country"] == "United States"
    assert result["country_code"] == "US"
    assert result["city"] == "Ashburn"
    assert result["lat"] == 39.03
    assert result["lon"] == -77.5
    assert result["timezone"] == "America/New_York"
    assert result["isp"] == "Google LLC"
    assert result["as_name"] == "AS15169 Google LLC"
    assert result["hosting"] is True
    assert result["proxy"] is False


def test_parse_ip_info_missing_fields() -> None:
    result = _parse_ip_info({"status": "success", "query": "1.2.3.4"})
    assert result["query"] == "1.2.3.4"
    assert result["country"] == ""
    assert result["lat"] == 0.0
    assert result["mobile"] is False


# ---------------------------------------------------------------------------
# Single IP lookup
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_single_ip_lookup() -> None:
    respx.get(f"{BASE}/json/8.8.8.8").mock(
        return_value=httpx.Response(200, json=MOCK_SINGLE)
    )
    block = IPGeolocationBlock()
    result = await block.execute(
        IPGeolocationInput(
            ip_addresses=["8.8.8.8"], api_key="testkey",
        )
    )
    assert isinstance(result, IPGeolocationOutput)
    assert result.total == 1
    assert result.results[0]["query"] == "8.8.8.8"
    assert result.results[0]["country"] == "United States"
    assert result.results[0]["hosting"] is True


@respx.mock
@pytest.mark.asyncio
async def test_single_ip_passes_params() -> None:
    route = respx.get(f"{BASE}/json/8.8.8.8").mock(
        return_value=httpx.Response(200, json=MOCK_SINGLE)
    )
    block = IPGeolocationBlock()
    await block.execute(
        IPGeolocationInput(
            ip_addresses=["8.8.8.8"], api_key="mykey", lang="de",
        )
    )
    request = route.calls[0].request
    assert "key=mykey" in str(request.url)
    assert "lang=de" in str(request.url)
    assert "fields=" in str(request.url)


# ---------------------------------------------------------------------------
# Current IP lookup (empty list)
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_current_ip_lookup() -> None:
    respx.get(f"{BASE}/json/").mock(
        return_value=httpx.Response(200, json=MOCK_CURRENT_IP)
    )
    block = IPGeolocationBlock()
    result = await block.execute(
        IPGeolocationInput(ip_addresses=[], api_key="testkey")
    )
    assert result.total == 1
    assert result.results[0]["query"] == "203.0.113.1"
    assert result.results[0]["country"] == "Germany"


# ---------------------------------------------------------------------------
# Batch lookup
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_batch_lookup() -> None:
    batch_response = [
        {**MOCK_SINGLE, "query": "8.8.8.8"},
        {**MOCK_CURRENT_IP, "query": "1.1.1.1"},
    ]
    respx.post(f"{BASE}/batch").mock(
        return_value=httpx.Response(200, json=batch_response)
    )
    block = IPGeolocationBlock()
    result = await block.execute(
        IPGeolocationInput(
            ip_addresses=["8.8.8.8", "1.1.1.1"], api_key="testkey",
        )
    )
    assert result.total == 2
    assert result.results[0]["query"] == "8.8.8.8"
    assert result.results[1]["query"] == "1.1.1.1"


@respx.mock
@pytest.mark.asyncio
async def test_batch_passes_ips_as_json() -> None:
    route = respx.post(f"{BASE}/batch").mock(
        return_value=httpx.Response(200, json=[MOCK_SINGLE, MOCK_SINGLE])
    )
    block = IPGeolocationBlock()
    await block.execute(
        IPGeolocationInput(
            ip_addresses=["8.8.8.8", "1.1.1.1"], api_key="k",
        )
    )
    request = route.calls[0].request
    import json

    body = json.loads(request.content)
    assert body == ["8.8.8.8", "1.1.1.1"]


@respx.mock
@pytest.mark.asyncio
async def test_batch_chunking() -> None:
    """Batches exceeding _MAX_BATCH (100) are split into multiple requests."""
    from llming_plumber.blocks.data.ip_geolocation import _MAX_BATCH

    ips = [f"10.0.0.{i}" for i in range(150)]
    mock_item = {**MOCK_SINGLE, "status": "success"}

    route = respx.post(f"{BASE}/batch").mock(
        side_effect=lambda request: httpx.Response(
            200, json=[mock_item] * len(request.content.decode().count(",") + 1)  # type: ignore[arg-type]
        )
    )

    # Use a simpler mock — just return enough items
    respx.reset()
    call_count = 0
    chunk_sizes: list[int] = []

    def _batch_handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        import json

        body = json.loads(request.content)
        chunk_sizes.append(len(body))
        return httpx.Response(200, json=[mock_item] * len(body))

    respx.post(f"{BASE}/batch").mock(side_effect=_batch_handler)

    block = IPGeolocationBlock()
    result = await block.execute(
        IPGeolocationInput(ip_addresses=ips, api_key="k")
    )

    assert result.total == 150
    assert call_count == 2
    assert chunk_sizes[0] == _MAX_BATCH  # 100
    assert chunk_sizes[1] == 50


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_fail_status_raises() -> None:
    respx.get(f"{BASE}/json/999.999.999.999").mock(
        return_value=httpx.Response(
            200, json={"status": "fail", "message": "invalid query", "query": "999.999.999.999"}
        )
    )
    block = IPGeolocationBlock()
    with pytest.raises(ValueError, match="ip-api lookup failed"):
        await block.execute(
            IPGeolocationInput(
                ip_addresses=["999.999.999.999"], api_key="k",
            )
        )


@respx.mock
@pytest.mark.asyncio
async def test_http_error_propagates() -> None:
    respx.get(f"{BASE}/json/8.8.8.8").mock(
        return_value=httpx.Response(429, text="Rate limited")
    )
    block = IPGeolocationBlock()
    with pytest.raises(httpx.HTTPStatusError):
        await block.execute(
            IPGeolocationInput(ip_addresses=["8.8.8.8"], api_key="k")
        )


# ---------------------------------------------------------------------------
# Custom API base
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_custom_api_base() -> None:
    respx.get("https://custom.api/json/8.8.8.8").mock(
        return_value=httpx.Response(200, json=MOCK_SINGLE)
    )
    block = IPGeolocationBlock()
    result = await block.execute(
        IPGeolocationInput(
            ip_addresses=["8.8.8.8"],
            api_key="k",
            api_base="https://custom.api",
        )
    )
    assert result.results[0]["query"] == "8.8.8.8"


# ---------------------------------------------------------------------------
# Whitespace trimming
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_strips_whitespace_from_ips() -> None:
    respx.get(f"{BASE}/json/8.8.8.8").mock(
        return_value=httpx.Response(200, json=MOCK_SINGLE)
    )
    block = IPGeolocationBlock()
    result = await block.execute(
        IPGeolocationInput(ip_addresses=["  8.8.8.8  "], api_key="k")
    )
    assert result.total == 1


@respx.mock
@pytest.mark.asyncio
async def test_empty_strings_filtered() -> None:
    respx.get(f"{BASE}/json/").mock(
        return_value=httpx.Response(200, json=MOCK_CURRENT_IP)
    )
    block = IPGeolocationBlock()
    result = await block.execute(
        IPGeolocationInput(ip_addresses=["", "  "], api_key="k")
    )
    # All empty → current IP lookup
    assert result.total == 1


# ---------------------------------------------------------------------------
# Context logging
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_ctx_logging() -> None:
    from unittest.mock import AsyncMock

    from llming_plumber.blocks.base import BlockContext

    respx.get(f"{BASE}/json/8.8.8.8").mock(
        return_value=httpx.Response(200, json=MOCK_SINGLE)
    )
    mock_console = AsyncMock()
    mock_console.write = AsyncMock()
    ctx = BlockContext(
        pipeline_id="p", run_id="r", block_id="b", console=mock_console,
    )

    block = IPGeolocationBlock()
    await block.execute(
        IPGeolocationInput(ip_addresses=["8.8.8.8"], api_key="k"),
        ctx=ctx,
    )
    assert mock_console.write.call_count >= 1


# ---------------------------------------------------------------------------
# Block metadata
# ---------------------------------------------------------------------------


def test_block_metadata() -> None:
    assert IPGeolocationBlock.block_type == "ip_geolocation"
    assert "data/network" in IPGeolocationBlock.categories
    assert IPGeolocationBlock.cache_ttl == 3600
    assert IPGeolocationBlock.fan_out_field == "results"
    assert IPGeolocationBlock.icon == "tabler/map-pin"


def test_standalone_no_ctx() -> None:
    """Input model works without context."""
    inp = IPGeolocationInput(ip_addresses=["8.8.8.8"], api_key="test")
    assert inp.ip_addresses == ["8.8.8.8"]
