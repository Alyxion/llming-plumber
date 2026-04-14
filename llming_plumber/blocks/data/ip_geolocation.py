"""IP geolocation block — lookup IP address details via ip-api.com.

Supports three modes:
- No IPs → looks up the caller's public IP
- One IP → single lookup
- Multiple IPs → batch lookup (up to 100 per request)

Requires a pro API key for HTTPS access (env: ``IPAPI_KEY``).
"""

from __future__ import annotations

import os
from typing import Any, ClassVar

import httpx
from pydantic import BaseModel, Field

from llming_plumber.blocks.base import (
    BaseBlock,
    BlockContext,
    BlockInput,
    BlockOutput,
)
from llming_plumber.blocks.limits import MAX_FAN_OUT_ITEMS, check_list_size

_MAX_BATCH = 100


def _ipapi_key() -> str:
    return os.environ.get("IPAPI_KEY", "")


def _ipapi_base() -> str:
    return os.environ.get("IPAPI_BASE", "https://pro.ip-api.com")


class IPInfo(BaseModel):
    """Geolocation and network info for a single IP address."""

    query: str = ""
    status: str = ""
    country: str = ""
    country_code: str = ""
    region: str = ""
    region_name: str = ""
    city: str = ""
    zip: str = ""
    lat: float = 0.0
    lon: float = 0.0
    timezone: str = ""
    isp: str = ""
    org: str = ""
    as_name: str = ""
    mobile: bool = False
    proxy: bool = False
    hosting: bool = False


class IPGeolocationInput(BlockInput):
    ip_addresses: list[str] = Field(
        default=[],
        title="IP Addresses",
        description=(
            "IP addresses to look up. "
            "Leave empty to look up the caller's public IP."
        ),
        json_schema_extra={"placeholder": "8.8.8.8"},
    )
    api_key: str = Field(
        default_factory=_ipapi_key,
        title="API Key",
        description="ip-api.com pro API key (defaults to IPAPI_KEY env var)",
        json_schema_extra={"secret": True},
    )
    lang: str = Field(
        default="en",
        title="Language",
        description="Response language",
        json_schema_extra={
            "widget": "select",
            "options": ["en", "de", "es", "fr", "ja", "ru", "pt-BR", "zh-CN"],
        },
    )
    api_base: str = Field(
        default_factory=_ipapi_base,
        title="API Base URL",
        description="ip-api.com base URL",
        json_schema_extra={"group": "advanced"},
    )


class IPGeolocationOutput(BlockOutput):
    results: list[dict[str, Any]] = []
    total: int = 0


_FIELDS = (
    "status,message,query,country,countryCode,region,regionName,"
    "city,zip,lat,lon,timezone,isp,org,as,mobile,proxy,hosting"
)


def _parse_ip_info(data: dict[str, Any]) -> dict[str, Any]:
    """Convert a raw API response dict into an IPInfo-compatible dict."""
    return dict(IPInfo(
        query=str(data.get("query", "")),
        status=str(data.get("status", "")),
        country=str(data.get("country", "")),
        country_code=str(data.get("countryCode", "")),
        region=str(data.get("region", "")),
        region_name=str(data.get("regionName", "")),
        city=str(data.get("city", "")),
        zip=str(data.get("zip", "")),
        lat=float(data.get("lat", 0)),
        lon=float(data.get("lon", 0)),
        timezone=str(data.get("timezone", "")),
        isp=str(data.get("isp", "")),
        org=str(data.get("org", "")),
        as_name=str(data.get("as", "")),
        mobile=bool(data.get("mobile", False)),
        proxy=bool(data.get("proxy", False)),
        hosting=bool(data.get("hosting", False)),
    ).model_dump())


class IPGeolocationBlock(
    BaseBlock[IPGeolocationInput, IPGeolocationOutput],
):
    block_type: ClassVar[str] = "ip_geolocation"
    icon: ClassVar[str] = "tabler/map-pin"
    categories: ClassVar[list[str]] = ["data/network"]
    description: ClassVar[str] = (
        "Look up geolocation and network info for IP addresses via ip-api.com"
    )
    cache_ttl: ClassVar[int] = 3600
    fan_out_field: ClassVar[str | None] = "results"

    async def execute(
        self,
        input: IPGeolocationInput,
        ctx: BlockContext | None = None,
    ) -> IPGeolocationOutput:
        ips = [ip.strip() for ip in input.ip_addresses if ip.strip()]

        params: dict[str, str] = {"fields": _FIELDS, "lang": input.lang}
        if input.api_key:
            params["key"] = input.api_key

        results: list[dict[str, Any]] = []

        async with httpx.AsyncClient() as client:
            if len(ips) <= 1:
                # Single lookup (or current IP if empty)
                target = ips[0] if ips else ""
                resp = await client.get(
                    f"{input.api_base}/json/{target}",
                    params=params,
                )
                resp.raise_for_status()
                data: dict[str, Any] = resp.json()
                if data.get("status") == "fail":
                    msg = data.get("message", "Unknown error")
                    raise ValueError(
                        f"ip-api lookup failed for '{target or 'current IP'}': {msg}"
                    )
                results.append(_parse_ip_info(data))
            else:
                # Batch lookup — split into chunks of _MAX_BATCH
                check_list_size(
                    ips,
                    limit=MAX_FAN_OUT_ITEMS,
                    label="IP addresses",
                )
                for i in range(0, len(ips), _MAX_BATCH):
                    chunk = ips[i : i + _MAX_BATCH]
                    resp = await client.post(
                        f"{input.api_base}/batch",
                        params=params,
                        json=chunk,
                    )
                    resp.raise_for_status()
                    batch_data: list[dict[str, Any]] = resp.json()
                    for item in batch_data:
                        results.append(_parse_ip_info(item))

        if ctx:
            await ctx.log(f"Looked up {len(results)} IP address(es)")

        return IPGeolocationOutput(results=results, total=len(results))
