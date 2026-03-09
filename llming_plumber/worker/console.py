"""Run console — Redis-backed append-only log stream per pipeline run.

Every run gets a virtual console that blocks can write to via
``ctx.log("message")``.  Console entries are stored as a Redis list
with auto-expiry so they clean up automatically.

Redis key: ``plumber:console:{run_id}``

Reading the console is available to anyone with the run_id — the API
layer should enforce access control.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any

from llming_plumber.blocks.limits import (
    CONSOLE_MAX_ENTRIES,
    CONSOLE_TTL_SECONDS,
)

logger = logging.getLogger(__name__)


class RunConsole:
    """Append-only console log backed by a Redis list.

    Create one per run.  If *redis* is ``None`` all writes silently
    no-op so blocks can call ``ctx.log()`` without worrying about
    the execution context.
    """

    def __init__(
        self,
        redis: Any,
        run_id: str,
        *,
        ttl: int = CONSOLE_TTL_SECONDS,
        max_entries: int = CONSOLE_MAX_ENTRIES,
        events: Any = None,
    ) -> None:
        self._redis = redis
        self._run_id = run_id
        self._ttl = ttl
        self._max = max_entries
        self._key = f"plumber:console:{run_id}"
        self._events = events  # RunEventPublisher for progress events

    async def write(
        self,
        block_id: str,
        message: str,
        *,
        level: str = "info",
    ) -> None:
        """Append a console entry.  No-ops when Redis is unavailable."""
        if self._redis is None:
            return
        entry = json.dumps({
            "ts": datetime.now(UTC).isoformat(),
            "block_id": block_id,
            "level": level,
            "msg": message[:2000],
        })
        try:
            pipe = self._redis.pipeline()
            pipe.rpush(self._key, entry)
            pipe.ltrim(self._key, -self._max, -1)
            pipe.expire(self._key, self._ttl)
            await pipe.execute()
        except Exception:
            logger.debug(
                "Console write failed for run %s", self._run_id,
                exc_info=True,
            )

        # Publish progress event so the UI shows intermediate status
        if self._events is not None:
            try:
                await self._events.block_progress(block_id, message[:200])
            except Exception:
                pass


async def read_console(
    redis: Any,
    run_id: str,
    *,
    offset: int = 0,
    limit: int = 200,
) -> list[dict[str, Any]]:
    """Read console entries for a run.

    Returns a list of dicts with ``ts``, ``block_id``, ``level``, ``msg``.
    """
    key = f"plumber:console:{run_id}"
    raw_list = await redis.lrange(key, offset, offset + limit - 1)
    return [json.loads(entry) for entry in raw_list]
