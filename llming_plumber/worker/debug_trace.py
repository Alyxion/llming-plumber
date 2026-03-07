"""Debug trace — store intermediate pipeline results in Redis.

When a run has ``debug=True``, every block execution writes a lightweight
summary to Redis so users can inspect data flow after the fact.

Redis key layout::

    plumber:debug:{run_id}:order          → JSON list of block UIDs (exec order)
    plumber:debug:{run_id}:{block_uid}    → JSON block summary
    plumber:debug:{run_id}:{block_uid}:g  → JSON list of item glimpses
    plumber:debug:{run_id}:{block_uid}:p:{index} → JSON parcel detail

All keys auto-expire via ``DEBUG_TTL_SECONDS``.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any

from llming_plumber.blocks.limits import (
    DEBUG_MAX_GLIMPSES,
    DEBUG_MAX_PARCEL_BYTES,
    DEBUG_MAX_PARCELS,
    DEBUG_TTL_SECONDS,
)

logger = logging.getLogger(__name__)

# Fields we look for to build a human-readable "glimpse" label.
_LABEL_FIELDS: tuple[str, ...] = (
    "name", "filename", "file_name", "title", "label",
    "blob_name", "path", "url", "key", "id", "subject",
    "sheet_name", "container", "bucket",
)


def _glimpse_label(fields: dict[str, Any], index: int) -> str:
    """Extract a short human-readable label from parcel fields."""
    for key in _LABEL_FIELDS:
        val = fields.get(key)
        if val and isinstance(val, str):
            if len(val) > 120:
                return val[:117] + "..."
            return val
    # Fall back to first short string value
    for val in fields.values():
        if isinstance(val, str) and 0 < len(val) <= 120:
            return val
    return f"item-{index}"


def _truncate_parcel(fields: dict[str, Any], max_bytes: int) -> dict[str, Any]:
    """Truncate large field values so the JSON stays under *max_bytes*."""
    encoded = json.dumps(fields, default=str)
    if len(encoded.encode()) <= max_bytes:
        return fields

    # Shrink the largest string fields first
    result = dict(fields)
    for key in sorted(result, key=lambda k: len(str(result[k])), reverse=True):
        val = result[key]
        if isinstance(val, str) and len(val) > 200:
            result[key] = val[:200] + f"... ({len(val)} chars total)"
        elif isinstance(val, list) and len(val) > 20:
            result[key] = val[:20]
            result[f"_{key}_truncated"] = f"{len(val)} items total"
        elif isinstance(val, dict) and len(str(val)) > 500:
            result[key] = {"_truncated": True, "_keys": list(val.keys())[:30]}

        check = json.dumps(result, default=str)
        if len(check.encode()) <= max_bytes:
            break

    return result


class DebugTracer:
    """Writes debug trace data to Redis during a pipeline run.

    Create one per run.  If *redis* is ``None`` or debug is disabled,
    all methods silently no-op.
    """

    def __init__(
        self,
        redis: Any,
        run_id: str,
        *,
        enabled: bool = False,
        ttl: int = DEBUG_TTL_SECONDS,
        max_glimpses: int = DEBUG_MAX_GLIMPSES,
        max_parcels: int = DEBUG_MAX_PARCELS,
        max_parcel_bytes: int = DEBUG_MAX_PARCEL_BYTES,
    ) -> None:
        self._redis = redis
        self._run_id = run_id
        self._enabled = enabled and redis is not None
        self._ttl = ttl
        self._max_glimpses = max_glimpses
        self._max_parcels = max_parcels
        self._max_parcel_bytes = max_parcel_bytes
        self._prefix = f"plumber:debug:{run_id}"

    @property
    def enabled(self) -> bool:
        return self._enabled

    async def _setex(self, key: str, value: str) -> None:
        try:
            await self._redis.setex(key, self._ttl, value)
        except Exception:
            logger.debug("Debug trace write failed for %s", key, exc_info=True)

    async def record_order(self, block_uids: list[str]) -> None:
        """Store the execution order of blocks."""
        if not self._enabled:
            return
        await self._setex(
            f"{self._prefix}:order",
            json.dumps(block_uids),
        )

    async def record_block(
        self,
        block_uid: str,
        block_type: str,
        *,
        duration_ms: float,
        parcel_count: int,
        status: str = "completed",
        error: str | None = None,
    ) -> None:
        """Store a per-block execution summary."""
        if not self._enabled:
            return
        summary = {
            "block_uid": block_uid,
            "block_type": block_type,
            "ran_at": datetime.now(UTC).isoformat(),
            "duration_ms": round(duration_ms, 2),
            "parcel_count": parcel_count,
            "status": status,
        }
        if error:
            summary["error"] = error[:500]

        await self._setex(
            f"{self._prefix}:{block_uid}",
            json.dumps(summary),
        )

    async def record_parcels(
        self,
        block_uid: str,
        parcels_fields: list[dict[str, Any]],
    ) -> None:
        """Store glimpses for all parcels and full detail for the first N.

        *parcels_fields* is a list of ``parcel.fields`` dicts — one per
        parcel produced by this block.
        """
        if not self._enabled:
            return

        # Glimpses: short labels for browsing
        glimpses: list[dict[str, Any]] = []
        for i, fields in enumerate(parcels_fields[: self._max_glimpses]):
            glimpses.append({
                "index": i,
                "label": _glimpse_label(fields, i),
                "field_count": len(fields),
            })

        total = len(parcels_fields)
        if total > self._max_glimpses:
            glimpses.append({
                "index": -1,
                "label": f"... and {total - self._max_glimpses} more",
                "field_count": 0,
            })

        await self._setex(
            f"{self._prefix}:{block_uid}:g",
            json.dumps(glimpses),
        )

        # Full parcel detail for first N parcels
        for i, fields in enumerate(parcels_fields[: self._max_parcels]):
            truncated = _truncate_parcel(fields, self._max_parcel_bytes)
            await self._setex(
                f"{self._prefix}:{block_uid}:p:{i}",
                json.dumps(truncated, default=str),
            )


async def get_debug_trace(redis: Any, run_id: str) -> dict[str, Any]:
    """Read the full debug trace for a run from Redis.

    Returns a dict with execution order, block summaries, and glimpses.
    Returns an empty dict if no trace exists.
    """
    prefix = f"plumber:debug:{run_id}"

    order_raw = await redis.get(f"{prefix}:order")
    if not order_raw:
        return {}

    order: list[str] = json.loads(order_raw)
    blocks: dict[str, Any] = {}

    for block_uid in order:
        summary_raw = await redis.get(f"{prefix}:{block_uid}")
        if not summary_raw:
            continue

        entry: dict[str, Any] = json.loads(summary_raw)

        glimpses_raw = await redis.get(f"{prefix}:{block_uid}:g")
        if glimpses_raw:
            entry["glimpses"] = json.loads(glimpses_raw)

        blocks[block_uid] = entry

    return {"run_id": run_id, "order": order, "blocks": blocks}


async def get_debug_parcel(
    redis: Any,
    run_id: str,
    block_uid: str,
    index: int,
) -> dict[str, Any] | None:
    """Read a single parcel's full detail from the debug trace."""
    key = f"plumber:debug:{run_id}:{block_uid}:p:{index}"
    raw = await redis.get(key)
    if raw is None:
        return None
    return json.loads(raw)


async def search_debug_parcels(
    redis: Any,
    run_id: str,
    block_uid: str,
    *,
    label_contains: str | None = None,
    max_results: int = 20,
) -> list[dict[str, Any]]:
    """Search glimpses for a block, optionally filtering by label substring.

    Returns matching glimpses with their indices so the caller can fetch
    full detail via ``get_debug_parcel``.
    """
    prefix = f"plumber:debug:{run_id}"
    glimpses_raw = await redis.get(f"{prefix}:{block_uid}:g")
    if not glimpses_raw:
        return []

    glimpses: list[dict[str, Any]] = json.loads(glimpses_raw)

    if label_contains:
        needle = label_contains.lower()
        glimpses = [g for g in glimpses if needle in g.get("label", "").lower()]

    return glimpses[:max_results]
