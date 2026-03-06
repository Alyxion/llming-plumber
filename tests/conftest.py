from __future__ import annotations

import pytest
from dotenv import load_dotenv

load_dotenv()


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"
