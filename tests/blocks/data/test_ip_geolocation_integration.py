"""Integration tests for ip_geolocation block — real API calls.

Requires IPAPI_KEY in .env (loaded by conftest.py).
Run with: pytest -m integration tests/blocks/data/test_ip_geolocation_integration.py -v
"""

from __future__ import annotations

import os

import pytest

from llming_plumber.blocks.data.ip_geolocation import (
    IPGeolocationBlock,
    IPGeolocationInput,
)

pytestmark = pytest.mark.integration


@pytest.fixture
def api_key() -> str:
    key = os.environ.get("IPAPI_KEY", "")
    if not key:
        pytest.skip("IPAPI_KEY not set")
    return key


@pytest.mark.asyncio
async def test_current_ip(api_key: str) -> None:
    """Look up the caller's public IP."""
    block = IPGeolocationBlock()
    result = await block.execute(
        IPGeolocationInput(ip_addresses=[], api_key=api_key)
    )
    assert result.total == 1
    info = result.results[0]
    assert info["status"] == "success"
    assert info["query"]  # should be our public IP
    assert info["country"]
    assert info["city"]
    assert isinstance(info["lat"], float)
    assert isinstance(info["lon"], float)


@pytest.mark.asyncio
async def test_known_ip(api_key: str) -> None:
    """Look up Google DNS (8.8.8.8) — well-known IP with stable location."""
    block = IPGeolocationBlock()
    result = await block.execute(
        IPGeolocationInput(ip_addresses=["8.8.8.8"], api_key=api_key)
    )
    assert result.total == 1
    info = result.results[0]
    assert info["query"] == "8.8.8.8"
    assert info["status"] == "success"
    assert info["country"] == "United States"
    assert "Google" in info["isp"]


@pytest.mark.asyncio
async def test_batch(api_key: str) -> None:
    """Batch lookup of multiple IPs."""
    block = IPGeolocationBlock()
    result = await block.execute(
        IPGeolocationInput(
            ip_addresses=["8.8.8.8", "1.1.1.1"],
            api_key=api_key,
        )
    )
    assert result.total == 2
    queries = {r["query"] for r in result.results}
    assert "8.8.8.8" in queries
    assert "1.1.1.1" in queries


@pytest.mark.asyncio
async def test_language_de(api_key: str) -> None:
    """Response in German."""
    block = IPGeolocationBlock()
    result = await block.execute(
        IPGeolocationInput(
            ip_addresses=["8.8.8.8"], api_key=api_key, lang="de",
        )
    )
    info = result.results[0]
    assert info["status"] == "success"
    # Country name should be in German
    assert info["country"] == "Vereinigte Staaten"


@pytest.mark.asyncio
async def test_invalid_ip(api_key: str) -> None:
    """Invalid IP should raise ValueError."""
    block = IPGeolocationBlock()
    with pytest.raises(ValueError, match="ip-api lookup failed"):
        await block.execute(
            IPGeolocationInput(
                ip_addresses=["not-an-ip"], api_key=api_key,
            )
        )
