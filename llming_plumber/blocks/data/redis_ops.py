"""Redis operation blocks — direct key/value, list, hash, pub/sub, and utility operations.

These blocks provide low-level Redis access for pipelines that need direct
control over Redis data structures. For higher-level caching with scoping and
TTL, see the cache blocks in ``core/cache.py``.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, ClassVar, Literal

from pydantic import Field

from llming_plumber.blocks.base import (
    BaseBlock,
    BlockContext,
    BlockInput,
    BlockOutput,
)

logger = logging.getLogger(__name__)


def _get_redis() -> Any:
    """Get Redis connection, returns None on failure."""
    try:
        from llming_plumber.db import get_redis
        return get_redis()
    except Exception:
        return None


def _parse_list(raw: str) -> list[str]:
    """Parse a string as JSON array or newline-separated values."""
    stripped = raw.strip()
    if stripped.startswith("["):
        try:
            parsed = json.loads(stripped)
            if isinstance(parsed, list):
                return [str(v) for v in parsed]
        except json.JSONDecodeError:
            pass
    return [line.strip() for line in raw.splitlines() if line.strip()]


def _parse_comma_separated(raw: str) -> list[str]:
    """Split a comma-separated string into trimmed, non-empty parts."""
    return [part.strip() for part in raw.split(",") if part.strip()]


# ------------------------------------------------------------------
# RedisGet
# ------------------------------------------------------------------


class RedisGetInput(BlockInput):
    key: str = Field(
        title="Key",
        description="Redis key to retrieve",
    )


class RedisGetOutput(BlockOutput):
    value: str = ""
    found: bool = False


class RedisGetBlock(BaseBlock[RedisGetInput, RedisGetOutput]):
    block_type: ClassVar[str] = "redis_get"
    icon: ClassVar[str] = "tabler/database-import"
    categories: ClassVar[list[str]] = ["data/redis"]
    description: ClassVar[str] = "Get a key's value from Redis"

    async def execute(
        self,
        input: RedisGetInput,
        ctx: BlockContext | None = None,
    ) -> RedisGetOutput:
        redis = _get_redis()
        if redis is None:
            return RedisGetOutput()

        raw = await redis.get(input.key)
        if raw is None:
            return RedisGetOutput(found=False)

        return RedisGetOutput(value=raw, found=True)


# ------------------------------------------------------------------
# RedisSet
# ------------------------------------------------------------------


class RedisSetInput(BlockInput):
    key: str = Field(
        title="Key",
        description="Redis key to set",
    )
    value: str = Field(
        title="Value",
        description="Value to store",
        json_schema_extra={"widget": "textarea", "rows": 3},
    )
    ttl_seconds: int = Field(
        default=0,
        title="TTL (seconds)",
        description="Time to live. 0 = no expiry.",
        json_schema_extra={"min": 0},
    )


class RedisSetOutput(BlockOutput):
    success: bool = False
    key: str = ""


class RedisSetBlock(BaseBlock[RedisSetInput, RedisSetOutput]):
    block_type: ClassVar[str] = "redis_set"
    icon: ClassVar[str] = "tabler/database-export"
    categories: ClassVar[list[str]] = ["data/redis"]
    description: ClassVar[str] = "Set a key with optional TTL in Redis"

    async def execute(
        self,
        input: RedisSetInput,
        ctx: BlockContext | None = None,
    ) -> RedisSetOutput:
        redis = _get_redis()
        if redis is None:
            return RedisSetOutput(key=input.key)

        if input.ttl_seconds > 0:
            await redis.setex(input.key, input.ttl_seconds, input.value)
        else:
            await redis.set(input.key, input.value)

        return RedisSetOutput(success=True, key=input.key)


# ------------------------------------------------------------------
# RedisDelete
# ------------------------------------------------------------------


class RedisDeleteInput(BlockInput):
    keys: str = Field(
        title="Keys",
        description="Comma-separated list of keys to delete",
        json_schema_extra={"placeholder": "key1, key2, key3"},
    )


class RedisDeleteOutput(BlockOutput):
    deleted_count: int = 0


class RedisDeleteBlock(BaseBlock[RedisDeleteInput, RedisDeleteOutput]):
    block_type: ClassVar[str] = "redis_delete"
    icon: ClassVar[str] = "tabler/database-x"
    categories: ClassVar[list[str]] = ["data/redis"]
    description: ClassVar[str] = "Delete one or more keys from Redis"

    async def execute(
        self,
        input: RedisDeleteInput,
        ctx: BlockContext | None = None,
    ) -> RedisDeleteOutput:
        redis = _get_redis()
        if redis is None:
            return RedisDeleteOutput()

        key_list = _parse_comma_separated(input.keys)
        if not key_list:
            return RedisDeleteOutput()

        count = await redis.delete(*key_list)
        return RedisDeleteOutput(deleted_count=count)


# ------------------------------------------------------------------
# RedisListPush
# ------------------------------------------------------------------


class RedisListPushInput(BlockInput):
    key: str = Field(
        title="Key",
        description="Redis list key",
    )
    values: str = Field(
        title="Values",
        description="Values to push (one per line or JSON array)",
        json_schema_extra={"widget": "textarea", "rows": 4},
    )
    direction: Literal["left", "right"] = Field(
        default="right",
        title="Direction",
        description="Push to the left (head) or right (tail) of the list",
        json_schema_extra={"widget": "select", "options": ["left", "right"]},
    )


class RedisListPushOutput(BlockOutput):
    list_length: int = 0
    key: str = ""


class RedisListPushBlock(BaseBlock[RedisListPushInput, RedisListPushOutput]):
    block_type: ClassVar[str] = "redis_list_push"
    icon: ClassVar[str] = "tabler/list-details"
    categories: ClassVar[list[str]] = ["data/redis"]
    description: ClassVar[str] = "Push values to a Redis list (LPUSH or RPUSH)"

    async def execute(
        self,
        input: RedisListPushInput,
        ctx: BlockContext | None = None,
    ) -> RedisListPushOutput:
        redis = _get_redis()
        if redis is None:
            return RedisListPushOutput(key=input.key)

        items = _parse_list(input.values)
        if not items:
            return RedisListPushOutput(key=input.key)

        if input.direction == "left":
            length = await redis.lpush(input.key, *items)
        else:
            length = await redis.rpush(input.key, *items)

        return RedisListPushOutput(list_length=length, key=input.key)


# ------------------------------------------------------------------
# RedisListPop
# ------------------------------------------------------------------


class RedisListPopInput(BlockInput):
    key: str = Field(
        title="Key",
        description="Redis list key",
    )
    count: int = Field(
        default=1,
        title="Count",
        description="Number of elements to pop",
        json_schema_extra={"min": 1},
    )
    direction: Literal["left", "right"] = Field(
        default="left",
        title="Direction",
        description="Pop from the left (head) or right (tail) of the list",
        json_schema_extra={"widget": "select", "options": ["left", "right"]},
    )
    block_seconds: int = Field(
        default=0,
        title="Block (seconds)",
        description="Blocking wait time. 0 = non-blocking.",
        json_schema_extra={"min": 0},
    )


class RedisListPopOutput(BlockOutput):
    values: list[str] = []
    key: str = ""


class RedisListPopBlock(BaseBlock[RedisListPopInput, RedisListPopOutput]):
    block_type: ClassVar[str] = "redis_list_pop"
    icon: ClassVar[str] = "tabler/list-check"
    categories: ClassVar[list[str]] = ["data/redis"]
    description: ClassVar[str] = "Pop values from a Redis list (LPOP or RPOP)"

    async def execute(
        self,
        input: RedisListPopInput,
        ctx: BlockContext | None = None,
    ) -> RedisListPopOutput:
        redis = _get_redis()
        if redis is None:
            return RedisListPopOutput(key=input.key)

        results: list[str] = []

        if input.block_seconds > 0:
            # Blocking pop — returns (key, value) or None
            for _ in range(input.count):
                if input.direction == "left":
                    result = await redis.blpop(input.key, timeout=input.block_seconds)
                else:
                    result = await redis.brpop(input.key, timeout=input.block_seconds)
                if result is None:
                    break
                # blpop/brpop return (key, value) tuple
                results.append(result[1] if isinstance(result, (list, tuple)) else str(result))
        else:
            # Non-blocking pop
            for _ in range(input.count):
                if input.direction == "left":
                    val = await redis.lpop(input.key)
                else:
                    val = await redis.rpop(input.key)
                if val is None:
                    break
                results.append(val)

        return RedisListPopOutput(values=results, key=input.key)


# ------------------------------------------------------------------
# RedisListRange
# ------------------------------------------------------------------


class RedisListRangeInput(BlockInput):
    key: str = Field(
        title="Key",
        description="Redis list key",
    )
    start: int = Field(
        default=0,
        title="Start",
        description="Start index (0-based, negative counts from end)",
    )
    stop: int = Field(
        default=-1,
        title="Stop",
        description="Stop index (-1 = end of list)",
    )


class RedisListRangeOutput(BlockOutput):
    values: list[str] = []
    count: int = 0


class RedisListRangeBlock(BaseBlock[RedisListRangeInput, RedisListRangeOutput]):
    block_type: ClassVar[str] = "redis_list_range"
    icon: ClassVar[str] = "tabler/list-numbers"
    categories: ClassVar[list[str]] = ["data/redis"]
    description: ClassVar[str] = "Get a range of elements from a Redis list"

    async def execute(
        self,
        input: RedisListRangeInput,
        ctx: BlockContext | None = None,
    ) -> RedisListRangeOutput:
        redis = _get_redis()
        if redis is None:
            return RedisListRangeOutput()

        items = await redis.lrange(input.key, input.start, input.stop)
        return RedisListRangeOutput(values=items, count=len(items))


# ------------------------------------------------------------------
# RedisPublish
# ------------------------------------------------------------------


class RedisPublishInput(BlockInput):
    channel: str = Field(
        title="Channel",
        description="Redis pub/sub channel name",
    )
    message: str = Field(
        title="Message",
        description="Message to publish",
        json_schema_extra={"widget": "textarea", "rows": 3},
    )


class RedisPublishOutput(BlockOutput):
    receivers: int = 0
    channel: str = ""


class RedisPublishBlock(BaseBlock[RedisPublishInput, RedisPublishOutput]):
    block_type: ClassVar[str] = "redis_publish"
    icon: ClassVar[str] = "tabler/broadcast"
    categories: ClassVar[list[str]] = ["data/redis"]
    description: ClassVar[str] = "Publish a message to a Redis channel"

    async def execute(
        self,
        input: RedisPublishInput,
        ctx: BlockContext | None = None,
    ) -> RedisPublishOutput:
        redis = _get_redis()
        if redis is None:
            return RedisPublishOutput(channel=input.channel)

        count = await redis.publish(input.channel, input.message)
        return RedisPublishOutput(receivers=count, channel=input.channel)


# ------------------------------------------------------------------
# RedisSubscribe
# ------------------------------------------------------------------


class RedisSubscribeInput(BlockInput):
    channel: str = Field(
        title="Channel",
        description="Redis pub/sub channel to subscribe to",
    )
    max_messages: int = Field(
        default=10,
        title="Max Messages",
        description="Maximum number of messages to collect",
        json_schema_extra={"min": 1},
    )
    timeout_seconds: int = Field(
        default=30,
        title="Timeout (seconds)",
        description="Stop listening after this many seconds",
        json_schema_extra={"min": 1},
    )


class RedisSubscribeOutput(BlockOutput):
    messages: list[str] = []
    count: int = 0


class RedisSubscribeBlock(BaseBlock[RedisSubscribeInput, RedisSubscribeOutput]):
    block_type: ClassVar[str] = "redis_subscribe"
    icon: ClassVar[str] = "tabler/antenna"
    categories: ClassVar[list[str]] = ["data/redis"]
    description: ClassVar[str] = "Subscribe to a Redis channel and collect messages"
    fan_out_field: ClassVar[str | None] = "messages"

    async def execute(
        self,
        input: RedisSubscribeInput,
        ctx: BlockContext | None = None,
    ) -> RedisSubscribeOutput:
        redis = _get_redis()
        if redis is None:
            return RedisSubscribeOutput()

        pubsub = redis.pubsub()
        await pubsub.subscribe(input.channel)

        messages: list[str] = []
        try:
            deadline = asyncio.get_event_loop().time() + input.timeout_seconds
            while len(messages) < input.max_messages:
                remaining = deadline - asyncio.get_event_loop().time()
                if remaining <= 0:
                    break
                msg = await asyncio.wait_for(
                    pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0),
                    timeout=remaining,
                )
                if msg is not None and msg.get("type") == "message":
                    data = msg.get("data", "")
                    messages.append(data if isinstance(data, str) else data.decode("utf-8", errors="replace"))
        except (asyncio.TimeoutError, Exception):
            pass
        finally:
            await pubsub.unsubscribe(input.channel)
            await pubsub.close()

        return RedisSubscribeOutput(messages=messages, count=len(messages))


# ------------------------------------------------------------------
# RedisHashGet
# ------------------------------------------------------------------


class RedisHashGetInput(BlockInput):
    key: str = Field(
        title="Key",
        description="Redis hash key",
    )
    fields: str = Field(
        default="",
        title="Fields",
        description="Comma-separated field names. Leave empty to get all fields.",
        json_schema_extra={"placeholder": "field1, field2"},
    )


class RedisHashGetOutput(BlockOutput):
    data: dict[str, str] = {}
    found: bool = False


class RedisHashGetBlock(BaseBlock[RedisHashGetInput, RedisHashGetOutput]):
    block_type: ClassVar[str] = "redis_hash_get"
    icon: ClassVar[str] = "tabler/braces"
    categories: ClassVar[list[str]] = ["data/redis"]
    description: ClassVar[str] = "Get field(s) from a Redis hash"

    async def execute(
        self,
        input: RedisHashGetInput,
        ctx: BlockContext | None = None,
    ) -> RedisHashGetOutput:
        redis = _get_redis()
        if redis is None:
            return RedisHashGetOutput()

        field_list = _parse_comma_separated(input.fields) if input.fields.strip() else []

        if not field_list:
            # Get all fields
            data = await redis.hgetall(input.key)
        else:
            # Get specific fields
            values = await redis.hmget(input.key, *field_list)
            data = {
                f: v
                for f, v in zip(field_list, values)
                if v is not None
            }

        return RedisHashGetOutput(
            data=data,
            found=bool(data),
        )


# ------------------------------------------------------------------
# RedisHashSet
# ------------------------------------------------------------------


class RedisHashSetInput(BlockInput):
    key: str = Field(
        title="Key",
        description="Redis hash key",
    )
    data: str = Field(
        title="Data",
        description="JSON object of field-value pairs to set",
        json_schema_extra={"widget": "code", "rows": 4, "placeholder": '{"field1": "value1"}'},
    )


class RedisHashSetOutput(BlockOutput):
    fields_set: int = 0
    key: str = ""


class RedisHashSetBlock(BaseBlock[RedisHashSetInput, RedisHashSetOutput]):
    block_type: ClassVar[str] = "redis_hash_set"
    icon: ClassVar[str] = "tabler/braces"
    categories: ClassVar[list[str]] = ["data/redis"]
    description: ClassVar[str] = "Set field(s) in a Redis hash"

    async def execute(
        self,
        input: RedisHashSetInput,
        ctx: BlockContext | None = None,
    ) -> RedisHashSetOutput:
        redis = _get_redis()
        if redis is None:
            return RedisHashSetOutput(key=input.key)

        try:
            mapping = json.loads(input.data)
        except json.JSONDecodeError:
            return RedisHashSetOutput(key=input.key)

        if not isinstance(mapping, dict) or not mapping:
            return RedisHashSetOutput(key=input.key)

        # hset with mapping returns the number of new fields added
        count = await redis.hset(input.key, mapping=mapping)
        return RedisHashSetOutput(fields_set=count, key=input.key)


# ------------------------------------------------------------------
# RedisKeys
# ------------------------------------------------------------------


class RedisKeysInput(BlockInput):
    pattern: str = Field(
        default="*",
        title="Pattern",
        description="Glob-style pattern to match keys",
        json_schema_extra={"placeholder": "user:*"},
    )


class RedisKeysOutput(BlockOutput):
    keys: list[str] = []
    count: int = 0


class RedisKeysBlock(BaseBlock[RedisKeysInput, RedisKeysOutput]):
    block_type: ClassVar[str] = "redis_keys"
    icon: ClassVar[str] = "tabler/key"
    categories: ClassVar[list[str]] = ["data/redis"]
    description: ClassVar[str] = "List keys matching a pattern in Redis"

    async def execute(
        self,
        input: RedisKeysInput,
        ctx: BlockContext | None = None,
    ) -> RedisKeysOutput:
        redis = _get_redis()
        if redis is None:
            return RedisKeysOutput()

        keys = await redis.keys(input.pattern)
        return RedisKeysOutput(keys=keys, count=len(keys))


# ------------------------------------------------------------------
# RedisIncr
# ------------------------------------------------------------------


class RedisIncrInput(BlockInput):
    key: str = Field(
        title="Key",
        description="Redis key to increment",
    )
    amount: int = Field(
        default=1,
        title="Amount",
        description="Increment amount (can be negative to decrement)",
    )


class RedisIncrOutput(BlockOutput):
    value: int = 0
    key: str = ""


class RedisIncrBlock(BaseBlock[RedisIncrInput, RedisIncrOutput]):
    block_type: ClassVar[str] = "redis_incr"
    icon: ClassVar[str] = "tabler/plus"
    categories: ClassVar[list[str]] = ["data/redis"]
    description: ClassVar[str] = "Increment a key's numeric value in Redis"

    async def execute(
        self,
        input: RedisIncrInput,
        ctx: BlockContext | None = None,
    ) -> RedisIncrOutput:
        redis = _get_redis()
        if redis is None:
            return RedisIncrOutput(key=input.key)

        result = await redis.incrby(input.key, input.amount)
        return RedisIncrOutput(value=result, key=input.key)


# ------------------------------------------------------------------
# RedisFileStore — store a FileRef in Redis
# ------------------------------------------------------------------


class RedisFileStoreInput(BlockInput):
    key: str = Field(
        title="Key",
        description="Redis key to store the file under",
    )
    file_ref: dict[str, Any] = Field(
        default_factory=dict,
        title="File Reference",
        description="FileRef object to store",
    )
    ttl_seconds: int = Field(
        default=0,
        title="TTL (seconds)",
        description="Time to live. 0 = no expiry.",
        json_schema_extra={"min": 0},
    )


class RedisFileStoreOutput(BlockOutput):
    success: bool = False
    key: str = ""
    filename: str = ""
    size_bytes: int = 0


class RedisFileStoreBlock(BaseBlock[RedisFileStoreInput, RedisFileStoreOutput]):
    block_type: ClassVar[str] = "redis_file_store"
    icon: ClassVar[str] = "tabler/file-database"
    categories: ClassVar[list[str]] = ["data/redis"]
    description: ClassVar[str] = "Store a file (FileRef) in Redis"

    async def execute(
        self,
        input: RedisFileStoreInput,
        ctx: BlockContext | None = None,
    ) -> RedisFileStoreOutput:
        redis = _get_redis()
        if redis is None:
            return RedisFileStoreOutput(key=input.key)

        from llming_plumber.models.file_ref import FileRef

        ref = FileRef(**input.file_ref) if isinstance(input.file_ref, dict) else input.file_ref
        payload = json.dumps(ref.model_dump())

        if input.ttl_seconds > 0:
            await redis.setex(input.key, input.ttl_seconds, payload)
        else:
            await redis.set(input.key, payload)

        return RedisFileStoreOutput(
            success=True,
            key=input.key,
            filename=ref.filename,
            size_bytes=ref.size_bytes,
        )


# ------------------------------------------------------------------
# RedisFileLoad — load a FileRef from Redis
# ------------------------------------------------------------------


class RedisFileLoadInput(BlockInput):
    key: str = Field(
        title="Key",
        description="Redis key to retrieve the file from",
    )


class RedisFileLoadOutput(BlockOutput):
    file_ref: dict[str, Any] = Field(default_factory=dict)
    found: bool = False
    filename: str = ""


class RedisFileLoadBlock(BaseBlock[RedisFileLoadInput, RedisFileLoadOutput]):
    block_type: ClassVar[str] = "redis_file_load"
    icon: ClassVar[str] = "tabler/file-database"
    categories: ClassVar[list[str]] = ["data/redis"]
    description: ClassVar[str] = "Load a file (FileRef) from Redis"

    async def execute(
        self,
        input: RedisFileLoadInput,
        ctx: BlockContext | None = None,
    ) -> RedisFileLoadOutput:
        redis = _get_redis()
        if redis is None:
            return RedisFileLoadOutput()

        raw = await redis.get(input.key)
        if raw is None:
            return RedisFileLoadOutput(found=False)

        from llming_plumber.models.file_ref import FileRef

        data = json.loads(raw)
        ref = FileRef(**data)
        return RedisFileLoadOutput(
            file_ref=ref.model_dump(),
            found=True,
            filename=ref.filename,
        )
