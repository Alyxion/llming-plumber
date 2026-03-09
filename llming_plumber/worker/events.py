"""Run event publisher — pushes block-level progress to Redis pub/sub.

Every pipeline run publishes events to the channel
``plumber:run:{run_id}:events`` so that SSE endpoints can stream them
to the UI in real time.

Event types match the inline-run SSE protocol:
    start, block_start, block_done, error, done
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any

logger = logging.getLogger(__name__)


class RunEventPublisher:
    """Publishes run progress events to a Redis pub/sub channel.

    If *redis* is ``None`` all publishes silently no-op.
    """

    def __init__(self, redis: Any, run_id: str, pipeline_id: str) -> None:
        self._redis = redis
        self._run_id = run_id
        self._pipeline_id = pipeline_id
        self._run_channel = f"plumber:run:{run_id}:events"
        self._pipeline_channel = f"plumber:pipeline:{pipeline_id}:events"

    async def publish(self, event: str, data: dict[str, Any]) -> None:
        """Publish to both the run channel and the pipeline channel."""
        if self._redis is None:
            return
        msg = json.dumps({
            "event": event,
            "data": {**data, "run_id": self._run_id},
            "ts": datetime.now(UTC).isoformat(),
        })
        try:
            pipe = self._redis.pipeline()
            pipe.publish(self._run_channel, msg)
            pipe.publish(self._pipeline_channel, msg)
            await pipe.execute()
        except Exception:
            logger.debug(
                "Event publish failed for run %s", self._run_id,
                exc_info=True,
            )

    async def start(self, order: list[str], total: int) -> None:
        await self.publish("start", {
            "run_id": self._run_id,
            "pipeline_id": self._pipeline_id,
            "blocks": order,
            "total": total,
        })

    async def block_start(
        self,
        block_uid: str,
        block_type: str,
        label: str,
        index: int,
    ) -> None:
        await self.publish("block_start", {
            "block_uid": block_uid,
            "block_type": block_type,
            "label": label,
            "index": index,
        })

    async def block_done(
        self,
        block_uid: str,
        block_type: str,
        label: str,
        *,
        duration_ms: float,
        parcel_count: int,
        status: str = "completed",
        output: dict[str, Any] | None = None,
        error: str | None = None,
        error_fields: list[dict[str, str]] | None = None,
    ) -> None:
        data: dict[str, Any] = {
            "block_uid": block_uid,
            "block_type": block_type,
            "label": label,
            "duration_ms": round(duration_ms, 1),
            "parcel_count": parcel_count,
            "status": status,
        }
        if output:
            data["output"] = _truncate_output(output)
        if error:
            data["error"] = error
        if error_fields:
            data["error_fields"] = error_fields
        await self.publish("block_done", data)

    async def block_progress(
        self,
        block_uid: str,
        message: str,
    ) -> None:
        await self.publish("block_progress", {
            "block_uid": block_uid,
            "message": message,
        })

    async def error(
        self,
        block_uid: str,
        message: str,
        **extra: Any,
    ) -> None:
        await self.publish("error", {
            "block_uid": block_uid,
            "message": message,
            **extra,
        })

    async def done(
        self,
        *,
        total_ms: float,
        blocks_run: int,
        status: str,
        output: dict[str, Any] | None = None,
    ) -> None:
        await self.publish("done", {
            "run_id": self._run_id,
            "total_ms": round(total_ms, 1),
            "blocks_run": blocks_run,
            "status": status,
            "output": _truncate_output(output) if output else {},
        })


def _truncate_output(d: dict[str, Any], max_str: int = 500) -> dict[str, Any]:
    """Truncate large values for event preview."""
    result: dict[str, Any] = {}
    for k, v in d.items():
        if isinstance(v, str) and len(v) > max_str:
            result[k] = v[:max_str] + f"... ({len(v)} chars)"
        elif isinstance(v, list) and len(v) > 10:
            result[k] = v[:10]
            result[f"_{k}_total"] = len(v)
        elif isinstance(v, dict) and len(str(v)) > max_str:
            result[k] = {kk: "..." for kk in list(v.keys())[:10]}
        else:
            result[k] = v
    return result
