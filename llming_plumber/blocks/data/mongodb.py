"""MongoDB operation blocks — query, insert, update, delete, aggregate, and watch.

All blocks use the shared Motor database obtained via ``get_database()``.
JSON string inputs are parsed with ``json.loads()`` so they can be wired
from upstream text outputs or typed directly in the UI.

Usage:
    MongoFindBlock    — query documents with filter, projection, sort, skip/limit
    MongoFindOneBlock — find a single document by filter
    MongoInsertBlock  — insert one or many documents
    MongoUpdateBlock  — update one or many documents
    MongoDeleteBlock  — delete one or many documents
    MongoAggregateBlock — run an aggregation pipeline
    MongoCountBlock   — count documents matching a filter
    MongoWatchBlock   — watch a collection change stream
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, ClassVar

from pydantic import Field

from llming_plumber.blocks.base import (
    BaseBlock,
    BlockContext,
    BlockInput,
    BlockOutput,
)

logger = logging.getLogger(__name__)


def _get_database() -> Any:
    """Get MongoDB database, returns None on failure."""
    try:
        from llming_plumber.db import get_database

        return get_database()
    except Exception:
        return None


def _parse_json(raw: str, *, label: str = "JSON") -> Any:
    """Parse a JSON string, raising ValueError with a clear message."""
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        msg = f"Invalid {label}: {exc}"
        raise ValueError(msg) from exc


# ------------------------------------------------------------------
# MongoFind
# ------------------------------------------------------------------


class MongoFindInput(BlockInput):
    collection: str = Field(
        title="Collection",
        description="MongoDB collection name",
    )
    query: str = Field(
        title="Query",
        description="Filter query as JSON object",
        json_schema_extra={"widget": "code", "placeholder": '{"status": "active"}'},
    )
    projection: str = Field(
        default="",
        title="Projection",
        description="Fields to include/exclude as JSON (optional)",
        json_schema_extra={"widget": "code", "placeholder": '{"_id": 0, "name": 1}'},
    )
    sort: str = Field(
        default="",
        title="Sort",
        description="Sort specification as JSON (optional)",
        json_schema_extra={"widget": "code", "placeholder": '{"created_at": -1}'},
    )
    limit: int = Field(
        default=100,
        title="Limit",
        description="Maximum number of documents to return",
        json_schema_extra={"min": 1, "max": 100_000},
    )
    skip: int = Field(
        default=0,
        title="Skip",
        description="Number of documents to skip",
        json_schema_extra={"min": 0},
    )


class MongoFindOutput(BlockOutput):
    documents: list[dict[str, Any]] = Field(default_factory=list)
    count: int = 0


class MongoFindBlock(BaseBlock[MongoFindInput, MongoFindOutput]):
    block_type: ClassVar[str] = "mongo_find"
    icon: ClassVar[str] = "tabler/database-search"
    categories: ClassVar[list[str]] = ["data/mongodb"]
    description: ClassVar[str] = "Query documents from a MongoDB collection"
    fan_out_field: ClassVar[str | None] = "documents"

    async def execute(
        self,
        input: MongoFindInput,
        ctx: BlockContext | None = None,
    ) -> MongoFindOutput:
        db = _get_database()
        if db is None:
            logger.error("MongoDB not available")
            return MongoFindOutput()

        try:
            query = _parse_json(input.query, label="query")
            projection = (
                _parse_json(input.projection, label="projection")
                if input.projection
                else None
            )
            sort_spec = _parse_json(input.sort, label="sort") if input.sort else None

            cursor = db[input.collection].find(query, projection)
            if sort_spec:
                cursor = cursor.sort(list(sort_spec.items()))
            cursor = cursor.skip(input.skip).limit(input.limit)

            documents = await cursor.to_list(length=input.limit)
            # Convert ObjectId and other BSON types to strings
            for doc in documents:
                if "_id" in doc:
                    doc["_id"] = str(doc["_id"])

            return MongoFindOutput(documents=documents, count=len(documents))
        except Exception:
            logger.exception("mongo_find failed on %s", input.collection)
            return MongoFindOutput()


# ------------------------------------------------------------------
# MongoFindOne
# ------------------------------------------------------------------


class MongoFindOneInput(BlockInput):
    collection: str = Field(
        title="Collection",
        description="MongoDB collection name",
    )
    query: str = Field(
        title="Query",
        description="Filter query as JSON object",
        json_schema_extra={"widget": "code", "placeholder": '{"_id": "abc123"}'},
    )


class MongoFindOneOutput(BlockOutput):
    document: dict[str, Any] = Field(default_factory=dict)
    found: bool = False


class MongoFindOneBlock(BaseBlock[MongoFindOneInput, MongoFindOneOutput]):
    block_type: ClassVar[str] = "mongo_find_one"
    icon: ClassVar[str] = "tabler/database-search"
    categories: ClassVar[list[str]] = ["data/mongodb"]
    description: ClassVar[str] = "Find a single document in a MongoDB collection"

    async def execute(
        self,
        input: MongoFindOneInput,
        ctx: BlockContext | None = None,
    ) -> MongoFindOneOutput:
        db = _get_database()
        if db is None:
            logger.error("MongoDB not available")
            return MongoFindOneOutput()

        try:
            query = _parse_json(input.query, label="query")
            doc = await db[input.collection].find_one(query)

            if doc is None:
                return MongoFindOneOutput(found=False)

            if "_id" in doc:
                doc["_id"] = str(doc["_id"])

            return MongoFindOneOutput(document=doc, found=True)
        except Exception:
            logger.exception("mongo_find_one failed on %s", input.collection)
            return MongoFindOneOutput()


# ------------------------------------------------------------------
# MongoInsert
# ------------------------------------------------------------------


class MongoInsertInput(BlockInput):
    collection: str = Field(
        title="Collection",
        description="MongoDB collection name",
    )
    document: str = Field(
        title="Document(s)",
        description="Document or array of documents as JSON",
        json_schema_extra={
            "widget": "code",
            "rows": 6,
            "placeholder": '{"name": "Alice"}',
        },
    )


class MongoInsertOutput(BlockOutput):
    inserted_ids: list[str] = Field(default_factory=list)
    count: int = 0


class MongoInsertBlock(BaseBlock[MongoInsertInput, MongoInsertOutput]):
    block_type: ClassVar[str] = "mongo_insert"
    icon: ClassVar[str] = "tabler/database-plus"
    categories: ClassVar[list[str]] = ["data/mongodb"]
    description: ClassVar[str] = (
        "Insert one or more documents into a MongoDB collection"
    )

    async def execute(
        self,
        input: MongoInsertInput,
        ctx: BlockContext | None = None,
    ) -> MongoInsertOutput:
        db = _get_database()
        if db is None:
            logger.error("MongoDB not available")
            return MongoInsertOutput()

        try:
            parsed = _parse_json(input.document, label="document")
            coll = db[input.collection]

            if isinstance(parsed, list):
                if not parsed:
                    return MongoInsertOutput()
                result = await coll.insert_many(parsed)
                ids = [str(oid) for oid in result.inserted_ids]
                return MongoInsertOutput(inserted_ids=ids, count=len(ids))
            else:
                result = await coll.insert_one(parsed)
                return MongoInsertOutput(
                    inserted_ids=[str(result.inserted_id)],
                    count=1,
                )
        except Exception:
            logger.exception("mongo_insert failed on %s", input.collection)
            return MongoInsertOutput()


# ------------------------------------------------------------------
# MongoUpdate
# ------------------------------------------------------------------


class MongoUpdateInput(BlockInput):
    collection: str = Field(
        title="Collection",
        description="MongoDB collection name",
    )
    query: str = Field(
        title="Query",
        description="Filter query as JSON object",
        json_schema_extra={"widget": "code", "placeholder": '{"status": "pending"}'},
    )
    update: str = Field(
        title="Update",
        description="Update operations as JSON",
        json_schema_extra={
            "widget": "code",
            "placeholder": '{"$set": {"status": "done"}}',
        },
    )
    upsert: bool = Field(
        default=False,
        title="Upsert",
        description="Insert if no document matches the query",
    )
    update_many: bool = Field(
        default=False,
        title="Update Many",
        description="Update all matching documents instead of just one",
    )


class MongoUpdateOutput(BlockOutput):
    matched_count: int = 0
    modified_count: int = 0
    upserted_id: str = ""


class MongoUpdateBlock(BaseBlock[MongoUpdateInput, MongoUpdateOutput]):
    block_type: ClassVar[str] = "mongo_update"
    icon: ClassVar[str] = "tabler/database-edit"
    categories: ClassVar[list[str]] = ["data/mongodb"]
    description: ClassVar[str] = "Update documents in a MongoDB collection"

    async def execute(
        self,
        input: MongoUpdateInput,
        ctx: BlockContext | None = None,
    ) -> MongoUpdateOutput:
        db = _get_database()
        if db is None:
            logger.error("MongoDB not available")
            return MongoUpdateOutput()

        try:
            query = _parse_json(input.query, label="query")
            update = _parse_json(input.update, label="update")
            coll = db[input.collection]

            if input.update_many:
                result = await coll.update_many(query, update, upsert=input.upsert)
            else:
                result = await coll.update_one(query, update, upsert=input.upsert)

            upserted_id = str(result.upserted_id) if result.upserted_id else ""
            return MongoUpdateOutput(
                matched_count=result.matched_count,
                modified_count=result.modified_count,
                upserted_id=upserted_id,
            )
        except Exception:
            logger.exception("mongo_update failed on %s", input.collection)
            return MongoUpdateOutput()


# ------------------------------------------------------------------
# MongoDelete
# ------------------------------------------------------------------


class MongoDeleteInput(BlockInput):
    collection: str = Field(
        title="Collection",
        description="MongoDB collection name",
    )
    query: str = Field(
        title="Query",
        description="Filter query as JSON object",
        json_schema_extra={"widget": "code", "placeholder": '{"status": "archived"}'},
    )
    delete_many: bool = Field(
        default=False,
        title="Delete Many",
        description="Delete all matching documents instead of just one",
    )


class MongoDeleteOutput(BlockOutput):
    deleted_count: int = 0


class MongoDeleteBlock(BaseBlock[MongoDeleteInput, MongoDeleteOutput]):
    block_type: ClassVar[str] = "mongo_delete"
    icon: ClassVar[str] = "tabler/database-minus"
    categories: ClassVar[list[str]] = ["data/mongodb"]
    description: ClassVar[str] = "Delete documents from a MongoDB collection"

    async def execute(
        self,
        input: MongoDeleteInput,
        ctx: BlockContext | None = None,
    ) -> MongoDeleteOutput:
        db = _get_database()
        if db is None:
            logger.error("MongoDB not available")
            return MongoDeleteOutput()

        try:
            query = _parse_json(input.query, label="query")
            coll = db[input.collection]

            if input.delete_many:
                result = await coll.delete_many(query)
            else:
                result = await coll.delete_one(query)

            return MongoDeleteOutput(deleted_count=result.deleted_count)
        except Exception:
            logger.exception("mongo_delete failed on %s", input.collection)
            return MongoDeleteOutput()


# ------------------------------------------------------------------
# MongoAggregate
# ------------------------------------------------------------------


class MongoAggregateInput(BlockInput):
    collection: str = Field(
        title="Collection",
        description="MongoDB collection name",
    )
    pipeline: str = Field(
        title="Pipeline",
        description="Aggregation pipeline as JSON array",
        json_schema_extra={
            "widget": "code",
            "rows": 8,
            "placeholder": (
                '[{"$match": {"status": "active"}},'
                ' {"$group": {"_id": "$category",'
                ' "total": {"$sum": 1}}}]'
            ),
        },
    )


class MongoAggregateOutput(BlockOutput):
    documents: list[dict[str, Any]] = Field(default_factory=list)
    count: int = 0


class MongoAggregateBlock(BaseBlock[MongoAggregateInput, MongoAggregateOutput]):
    block_type: ClassVar[str] = "mongo_aggregate"
    icon: ClassVar[str] = "tabler/database-cog"
    categories: ClassVar[list[str]] = ["data/mongodb"]
    description: ClassVar[str] = "Run an aggregation pipeline on a MongoDB collection"
    fan_out_field: ClassVar[str | None] = "documents"

    async def execute(
        self,
        input: MongoAggregateInput,
        ctx: BlockContext | None = None,
    ) -> MongoAggregateOutput:
        db = _get_database()
        if db is None:
            logger.error("MongoDB not available")
            return MongoAggregateOutput()

        try:
            pipeline = _parse_json(input.pipeline, label="pipeline")
            if not isinstance(pipeline, list):
                msg = "Aggregation pipeline must be a JSON array"
                raise ValueError(msg)

            cursor = db[input.collection].aggregate(pipeline)
            documents = await cursor.to_list(length=None)

            for doc in documents:
                if "_id" in doc:
                    doc["_id"] = str(doc["_id"])

            return MongoAggregateOutput(documents=documents, count=len(documents))
        except Exception:
            logger.exception("mongo_aggregate failed on %s", input.collection)
            return MongoAggregateOutput()


# ------------------------------------------------------------------
# MongoCount
# ------------------------------------------------------------------


class MongoCountInput(BlockInput):
    collection: str = Field(
        title="Collection",
        description="MongoDB collection name",
    )
    query: str = Field(
        default="{}",
        title="Query",
        description="Filter query as JSON object",
        json_schema_extra={"widget": "code", "placeholder": '{"status": "active"}'},
    )


class MongoCountOutput(BlockOutput):
    count: int = 0


class MongoCountBlock(BaseBlock[MongoCountInput, MongoCountOutput]):
    block_type: ClassVar[str] = "mongo_count"
    icon: ClassVar[str] = "tabler/database"
    categories: ClassVar[list[str]] = ["data/mongodb"]
    description: ClassVar[str] = "Count documents in a MongoDB collection"

    async def execute(
        self,
        input: MongoCountInput,
        ctx: BlockContext | None = None,
    ) -> MongoCountOutput:
        db = _get_database()
        if db is None:
            logger.error("MongoDB not available")
            return MongoCountOutput()

        try:
            query = _parse_json(input.query, label="query")
            count = await db[input.collection].count_documents(query)
            return MongoCountOutput(count=count)
        except Exception:
            logger.exception("mongo_count failed on %s", input.collection)
            return MongoCountOutput()


# ------------------------------------------------------------------
# MongoWatch
# ------------------------------------------------------------------


class MongoWatchInput(BlockInput):
    collection: str = Field(
        title="Collection",
        description="MongoDB collection name",
    )
    pipeline: str = Field(
        default="",
        title="Pipeline Filter",
        description="Change stream filter pipeline as JSON array (optional)",
        json_schema_extra={
            "widget": "code",
            "rows": 4,
            "placeholder": '[{"$match": {"operationType": "insert"}}]',
        },
    )
    max_events: int = Field(
        default=10,
        title="Max Events",
        description="Maximum number of change events to collect",
        json_schema_extra={"min": 1, "max": 1000},
    )
    timeout_seconds: int = Field(
        default=30,
        title="Timeout (seconds)",
        description="Stop watching after this many seconds",
        json_schema_extra={"min": 1, "max": 300},
    )


class MongoWatchOutput(BlockOutput):
    events: list[dict[str, Any]] = Field(default_factory=list)
    count: int = 0


class MongoWatchBlock(BaseBlock[MongoWatchInput, MongoWatchOutput]):
    block_type: ClassVar[str] = "mongo_watch"
    icon: ClassVar[str] = "tabler/database-heart"
    categories: ClassVar[list[str]] = ["data/mongodb"]
    description: ClassVar[str] = (
        "Watch a MongoDB collection for changes via change stream"
    )
    fan_out_field: ClassVar[str | None] = "events"

    async def execute(
        self,
        input: MongoWatchInput,
        ctx: BlockContext | None = None,
    ) -> MongoWatchOutput:
        db = _get_database()
        if db is None:
            logger.error("MongoDB not available")
            return MongoWatchOutput()

        try:
            pipeline = (
                _parse_json(input.pipeline, label="pipeline")
                if input.pipeline
                else []
            )
            if not isinstance(pipeline, list):
                msg = "Change stream pipeline must be a JSON array"
                raise ValueError(msg)

            coll = db[input.collection]
            events: list[dict[str, Any]] = []

            async with coll.watch(pipeline) as stream:
                deadline = asyncio.get_event_loop().time() + input.timeout_seconds
                while len(events) < input.max_events:
                    remaining = deadline - asyncio.get_event_loop().time()
                    if remaining <= 0:
                        break
                    try:
                        change = await asyncio.wait_for(
                            stream.next(),
                            timeout=remaining,
                        )
                    except (TimeoutError, StopAsyncIteration):
                        break

                    # Serialize BSON types to strings
                    _serialize_bson(change)
                    events.append(change)

            return MongoWatchOutput(events=events, count=len(events))
        except Exception:
            logger.exception("mongo_watch failed on %s", input.collection)
            return MongoWatchOutput()


def _serialize_bson(doc: dict[str, Any]) -> None:
    """Recursively convert non-JSON-serializable BSON values to strings."""
    for key, value in doc.items():
        if isinstance(value, dict):
            _serialize_bson(value)
        elif isinstance(value, list):
            for i, item in enumerate(value):
                if isinstance(item, dict):
                    _serialize_bson(item)
                elif not isinstance(item, (str, int, float, bool, type(None))):
                    value[i] = str(item)
        elif not isinstance(value, (str, int, float, bool, type(None))):
            doc[key] = str(value)
