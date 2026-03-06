"""Generic HTTP request block."""

from __future__ import annotations

from typing import ClassVar

import httpx
from pydantic import Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput


class HttpRequestInput(BlockInput):
    url: str = Field(
        title="URL",
        description="The URL to send the request to",
        json_schema_extra={"placeholder": "https://api.example.com/data"},
    )
    method: str = Field(
        default="GET",
        title="Method",
        description="HTTP method",
        json_schema_extra={
            "widget": "select",
            "options": ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"],
        },
    )
    headers: dict[str, str] = Field(
        default={},
        title="Headers",
        description="HTTP headers as key-value pairs",
        json_schema_extra={"widget": "code"},
    )
    body: str | None = Field(
        default=None,
        title="Body",
        description="Request body content",
        json_schema_extra={"widget": "textarea"},
    )
    timeout: float = Field(
        default=30.0,
        title="Timeout",
        description="Request timeout in seconds",
    )


class HttpRequestOutput(BlockOutput):
    status_code: int
    headers: dict[str, str]
    body: str


class HttpRequestBlock(BaseBlock[HttpRequestInput, HttpRequestOutput]):
    block_type: ClassVar[str] = "http_request"
    icon: ClassVar[str] = "tabler/world-www"
    categories: ClassVar[list[str]] = ["core", "business/api"]
    description: ClassVar[str] = "Make HTTP requests to any URL"
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self, input: HttpRequestInput, ctx: BlockContext | None = None
    ) -> HttpRequestOutput:
        async with httpx.AsyncClient() as client:
            resp = await client.request(
                method=input.method,
                url=input.url,
                headers=input.headers,
                content=input.body,
                timeout=input.timeout,
            )

        return HttpRequestOutput(
            status_code=resp.status_code,
            headers=dict(resp.headers),
            body=resp.text,
        )
