"""Integration tests for the guard block — real ip_geolocation API calls.

Requires IPAPI_KEY in .env (loaded by conftest.py).
Run with: pytest -m integration tests/blocks/core/test_guard_integration.py -v
"""

from __future__ import annotations

import os

import pytest

from llming_plumber.blocks.core.guard import (
    GuardAbortError,
    GuardBlock,
    GuardInput,
)

pytestmark = pytest.mark.integration


@pytest.fixture
def api_key() -> str:
    key = os.environ.get("IPAPI_KEY", "")
    if not key:
        pytest.skip("IPAPI_KEY not set")
    return key


@pytest.mark.asyncio
async def test_guard_passes_with_real_ip_check(api_key: str) -> None:
    """Guard passes when our IP is NOT the forbidden one."""
    block = GuardBlock()
    result = await block.execute(
        GuardInput(
            check_block_type="ip_geolocation",
            check_config=f'{{"api_key": "{api_key}"}}',
            condition='results[0]["query"] != "192.0.2.1"',
            abort_message="Matched forbidden IP",
        ),
    )
    assert result.passed is True
    assert result.check_output["total"] == 1
    assert result.check_output["results"][0]["status"] == "success"


@pytest.mark.asyncio
async def test_guard_aborts_when_condition_fails(api_key: str) -> None:
    """Guard aborts when condition is deliberately false."""
    block = GuardBlock()
    with pytest.raises(GuardAbortError, match="Country mismatch"):
        await block.execute(
            GuardInput(
                check_block_type="ip_geolocation",
                check_config=f'{{"ip_addresses": ["8.8.8.8"], "api_key": "{api_key}"}}',
                condition='results[0]["country"] == "Atlantis"',
                abort_message="Country mismatch: {total} result(s)",
            ),
        )


@pytest.mark.asyncio
async def test_guard_check_output_available(api_key: str) -> None:
    """Check output dict is populated in the guard result."""
    block = GuardBlock()
    result = await block.execute(
        GuardInput(
            check_block_type="ip_geolocation",
            check_config=f'{{"ip_addresses": ["8.8.8.8"], "api_key": "{api_key}"}}',
            condition='results[0]["country"] == "United States"',
            abort_message="Expected US",
        ),
    )
    assert result.passed is True
    info = result.check_output["results"][0]
    assert info["query"] == "8.8.8.8"
    assert "Google" in info["isp"]
