"""Integration tests for the periodic guard block — real ip_geolocation API calls.

Requires IPAPI_KEY in .env (loaded by conftest.py).
Run with: pytest -m integration tests/blocks/core/test_periodic_guard_integration.py -v
"""

from __future__ import annotations

import os

import pytest

from llming_plumber.blocks.core.guard import GuardAbortError
from llming_plumber.blocks.core.periodic_guard import (
    PeriodicGuardBlock,
    PeriodicGuardInput,
)

pytestmark = pytest.mark.integration


@pytest.fixture
def api_key() -> str:
    key = os.environ.get("IPAPI_KEY", "")
    if not key:
        pytest.skip("IPAPI_KEY not set")
    return key


@pytest.mark.asyncio
async def test_initial_check_passes(api_key: str) -> None:
    """Periodic guard initial check passes when IP doesn't match."""
    block = PeriodicGuardBlock()
    result = await block.execute(
        PeriodicGuardInput(
            check_block_type="ip_geolocation",
            check_config=f'{{"api_key": "{api_key}"}}',
            condition='results[0]["query"] != "192.0.2.1"',
            interval_seconds=60,
        ),
    )
    assert result.passed is True
    assert result.check_output["total"] == 1


@pytest.mark.asyncio
async def test_initial_check_aborts(api_key: str) -> None:
    """Periodic guard initial check aborts when condition fails."""
    block = PeriodicGuardBlock()
    with pytest.raises(GuardAbortError, match="Wrong country"):
        await block.execute(
            PeriodicGuardInput(
                check_block_type="ip_geolocation",
                check_config=f'{{"ip_addresses": ["8.8.8.8"], "api_key": "{api_key}"}}',
                condition='results[0]["country"] == "Atlantis"',
                interval_seconds=60,
                pause_message="Wrong country",
            ),
        )
