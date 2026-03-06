from __future__ import annotations

import httpx

from llming_plumber.main import app


async def test_root() -> None:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/")
    assert resp.status_code == 200
    assert resp.json() == {"message": "hello world"}
