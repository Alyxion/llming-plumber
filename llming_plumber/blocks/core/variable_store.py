"""Atomic variable store backed by Redis.

Scopes
------
- ``gl_`` — global, shared across all pipelines (requires admin grant)
- ``pl_`` — pipeline-scoped, isolated per pipeline_id
- ``job_`` — run-scoped, isolated per run_id (persisted in run log)
- no prefix — ephemeral, lives only in the current block execution

All ``gl_`` and ``pl_`` operations are atomic via Redis.
"""

from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)

# Redis key prefixes
_GL_PREFIX = "plumber:var:gl:"
_PL_PREFIX = "plumber:var:pl:"
_JOB_PREFIX = "plumber:var:job:"
_GRANT_KEY = "plumber:var:grants"


class VariableStore:
    """Atomic variable operations backed by Redis."""

    def __init__(
        self,
        redis: Any,
        pipeline_id: str,
        run_id: str,
    ) -> None:
        self._redis = redis
        self._pipeline_id = pipeline_id
        self._run_id = run_id
        self._local: dict[str, Any] = {}

    # --- key mapping ---

    def _redis_key(self, name: str) -> tuple[str, str]:
        """Return (redis_key, scope) for a variable name."""
        if name.startswith("gl_"):
            return f"{_GL_PREFIX}{name[3:]}", "global"
        if name.startswith("pl_"):
            return f"{_PL_PREFIX}{self._pipeline_id}:{name[3:]}", "pipeline"
        if name.startswith("job_"):
            return f"{_JOB_PREFIX}{self._run_id}:{name[4:]}", "job"
        return "", "local"

    # --- access control ---

    async def check_global_access(self, var_name: str) -> bool:
        """Check if this pipeline has been granted access to a global var."""
        if self._redis is None:
            return False
        granted = await self._redis.sismember(
            f"{_GRANT_KEY}:{var_name}",
            self._pipeline_id,
        )
        return bool(granted)

    @staticmethod
    async def grant_global_access(
        redis: Any,
        var_name: str,
        pipeline_id: str,
    ) -> None:
        """Admin: grant a pipeline access to a global variable."""
        await redis.sadd(f"{_GRANT_KEY}:{var_name}", pipeline_id)

    @staticmethod
    async def revoke_global_access(
        redis: Any,
        var_name: str,
        pipeline_id: str,
    ) -> None:
        """Admin: revoke a pipeline's access to a global variable."""
        await redis.srem(f"{_GRANT_KEY}:{var_name}", pipeline_id)

    # --- core operations ---

    async def get(self, name: str) -> Any:
        """Fetch a variable value. Returns None if not set."""
        key, scope = self._redis_key(name)
        if scope == "local":
            return self._local.get(name)
        if self._redis is None:
            return None
        if scope == "global":
            if not await self.check_global_access(name[3:]):
                msg = f"Pipeline not granted access to global variable '{name}'"
                raise PermissionError(msg)
        raw = await self._redis.get(key)
        if raw is None:
            return None
        return _deserialize(raw)

    async def set(self, name: str, value: Any) -> None:
        """Set a variable to a value (atomic for Redis-backed scopes)."""
        key, scope = self._redis_key(name)
        if scope == "local":
            self._local[name] = value
            return
        if self._redis is None:
            return
        if scope == "global":
            if not await self.check_global_access(name[3:]):
                msg = f"Pipeline not granted access to global variable '{name}'"
                raise PermissionError(msg)
        await self._redis.set(key, _serialize(value))

    async def delete(self, name: str) -> None:
        """Delete a variable."""
        key, scope = self._redis_key(name)
        if scope == "local":
            self._local.pop(name, None)
            return
        if self._redis is None:
            return
        await self._redis.delete(key)

    async def incr(self, name: str, amount: float = 1) -> float:
        """Atomic increment (works for int and float). Returns new value."""
        key, scope = self._redis_key(name)
        if scope == "local":
            cur = self._local.get(name, 0)
            new = float(cur) + amount
            self._local[name] = new
            return new
        if self._redis is None:
            return amount
        if scope == "global":
            if not await self.check_global_access(name[3:]):
                msg = f"Pipeline not granted access to global variable '{name}'"
                raise PermissionError(msg)
        if isinstance(amount, float) and not amount.is_integer():
            result = await self._redis.incrbyfloat(key, amount)
            return float(result)
        result = await self._redis.incrbyfloat(key, amount)
        return float(result)

    async def decr(self, name: str, amount: float = 1) -> float:
        """Atomic decrement. Returns new value."""
        return await self.incr(name, -amount)

    async def append(self, name: str, suffix: str) -> str:
        """Atomic string append. Returns new value."""
        key, scope = self._redis_key(name)
        if scope == "local":
            cur = str(self._local.get(name, ""))
            new = cur + suffix
            self._local[name] = new
            return new
        if self._redis is None:
            return suffix
        if scope == "global":
            if not await self.check_global_access(name[3:]):
                msg = f"Pipeline not granted access to global variable '{name}'"
                raise PermissionError(msg)
        await self._redis.append(key, suffix)
        raw = await self._redis.get(key)
        return raw or suffix

    async def fetch(self, name: str) -> Any:
        """Alias for get — atomic read."""
        return await self.get(name)

    def get_local_vars(self) -> dict[str, Any]:
        """Return all local (ephemeral) variables."""
        return dict(self._local)

    async def get_job_vars(self) -> dict[str, Any]:
        """Return all job-scoped variables (for persisting in run log)."""
        if self._redis is None:
            return {}
        pattern = f"{_JOB_PREFIX}{self._run_id}:*"
        result: dict[str, Any] = {}
        async for key in self._redis.scan_iter(match=pattern, count=100):
            short_name = key.split(":", 4)[-1] if ":" in key else key
            raw = await self._redis.get(key)
            if raw is not None:
                result[f"job_{short_name}"] = _deserialize(raw)
        return result


def _serialize(value: Any) -> str:
    """Serialize a value to a Redis-compatible string."""
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return value
    return json.dumps(value)


def _deserialize(raw: str) -> Any:
    """Deserialize a Redis string back to a Python value."""
    # Try numeric
    try:
        if "." in raw:
            return float(raw)
        return int(raw)
    except (ValueError, TypeError):
        pass
    # Try JSON
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        pass
    return raw
