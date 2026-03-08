"""SSE endpoints for live pipeline run events.

Two levels of subscription:

- ``GET /runs/{run_id}/events`` — events for a single run (closes on done)
- ``GET /pipelines/{pipeline_id}/events`` — events for ALL runs of a
  pipeline (stays open, shows scheduler-triggered runs too)

Events are published by the executor via Redis pub/sub.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import AsyncGenerator

from fastapi import APIRouter, HTTPException
from starlette.responses import StreamingResponse

from llming_plumber.db import get_database, get_redis

logger = logging.getLogger(__name__)
router = APIRouter()


def _sse(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data, default=str)}\n\n"


async def _subscribe_channel(
    channel: str, *, close_on_done: bool = True,
) -> AsyncGenerator[str, None]:
    """Subscribe to a Redis pub/sub channel and yield SSE events."""
    redis = get_redis()
    pubsub = redis.pubsub()
    await pubsub.subscribe(channel)

    try:
        yield f"event: connected\ndata: {json.dumps({'channel': channel})}\n\n"

        while True:
            msg = await pubsub.get_message(
                ignore_subscribe_messages=True, timeout=1.0,
            )
            if msg and msg["type"] == "message":
                payload = json.loads(msg["data"])
                event = payload.get("event", "update")
                data = payload.get("data", payload)
                yield _sse(event, data)

                if close_on_done and event == "done":
                    break
            else:
                yield ": keepalive\n\n"
                await asyncio.sleep(0.1)
    except asyncio.CancelledError:
        pass
    finally:
        await pubsub.unsubscribe(channel)
        await pubsub.aclose()


def _streaming(gen: AsyncGenerator) -> StreamingResponse:
    return StreamingResponse(
        gen,
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/runs/{run_id}/events")
async def run_events(run_id: str) -> StreamingResponse:
    """Stream live events for a single run. Closes when the run finishes."""
    from bson import ObjectId

    db = get_database()
    run_doc = await db["runs"].find_one({"_id": ObjectId(run_id)})
    if run_doc is None:
        raise HTTPException(status_code=404, detail="Run not found")

    channel = f"plumber:run:{run_id}:events"
    return _streaming(_subscribe_channel(channel, close_on_done=True))


@router.get("/pipelines/{pipeline_id}/events")
async def pipeline_events(pipeline_id: str) -> StreamingResponse:
    """Stream live events for ALL runs of a pipeline.

    Stays open indefinitely — the editor subscribes on load and
    sees every scheduler-triggered or manually-triggered run.
    """
    from bson import ObjectId

    db = get_database()
    doc = await db["pipelines"].find_one({"_id": ObjectId(pipeline_id)})
    if doc is None:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    channel = f"plumber:pipeline:{pipeline_id}:events"
    return _streaming(_subscribe_channel(channel, close_on_done=False))
