from __future__ import annotations

import httpx
import respx

from llming_plumber.blocks.core.http_request import (
    HttpRequestBlock,
    HttpRequestInput,
    HttpRequestOutput,
)


@respx.mock
async def test_get_request() -> None:
    respx.get("https://example.com/api/data").mock(
        return_value=httpx.Response(200, text='{"ok": true}')
    )
    block = HttpRequestBlock()
    result = await block.execute(HttpRequestInput(url="https://example.com/api/data"))
    assert isinstance(result, HttpRequestOutput)
    assert result.status_code == 200
    assert result.body == '{"ok": true}'


@respx.mock
async def test_post_request_with_body() -> None:
    respx.post("https://example.com/api/submit").mock(
        return_value=httpx.Response(201, text="created")
    )
    block = HttpRequestBlock()
    result = await block.execute(
        HttpRequestInput(
            url="https://example.com/api/submit",
            method="POST",
            body='{"name": "test"}',
            headers={"Content-Type": "application/json"},
        )
    )
    assert result.status_code == 201
    assert result.body == "created"


@respx.mock
async def test_custom_headers_passed() -> None:
    route = respx.get("https://example.com/api").mock(
        return_value=httpx.Response(200, text="ok")
    )
    block = HttpRequestBlock()
    await block.execute(
        HttpRequestInput(
            url="https://example.com/api",
            headers={"X-Custom": "value123"},
        )
    )
    assert route.called
    assert route.calls[0].request.headers["X-Custom"] == "value123"


@respx.mock
async def test_non_200_returned_not_raised() -> None:
    respx.get("https://example.com/404").mock(
        return_value=httpx.Response(404, text="not found")
    )
    block = HttpRequestBlock()
    result = await block.execute(
        HttpRequestInput(url="https://example.com/404")
    )
    assert result.status_code == 404
    assert result.body == "not found"


@respx.mock
async def test_response_headers_captured() -> None:
    respx.get("https://example.com/h").mock(
        return_value=httpx.Response(
            200, text="ok", headers={"X-Rate-Limit": "100"}
        )
    )
    block = HttpRequestBlock()
    result = await block.execute(
        HttpRequestInput(url="https://example.com/h")
    )
    assert "x-rate-limit" in result.headers


async def test_block_type() -> None:
    assert HttpRequestBlock.block_type == "http_request"
    assert HttpRequestBlock.cache_ttl == 0
