"""Integration tests for MongoDB operation blocks — requires a running MongoDB server."""

from __future__ import annotations

import pytest


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

TEST_COLLECTION = "plumber_test_integration"


@pytest.fixture
async def mongo_db(monkeypatch: pytest.MonkeyPatch):
    try:
        from motor.motor_asyncio import AsyncIOMotorClient

        client = AsyncIOMotorClient("mongodb://localhost:27017", serverSelectionTimeoutMS=2000)
        await client.admin.command("ping")
        db = client["plumber_test"]
    except Exception as exc:
        pytest.skip(f"MongoDB not available: {exc}")

    monkeypatch.setattr("llming_plumber.blocks.data.mongodb._get_database", lambda: db)

    yield db

    # Cleanup
    await db[TEST_COLLECTION].drop()


async def test_insert_and_find(mongo_db) -> None:
    from llming_plumber.blocks.data.mongodb import (
        MongoFindBlock,
        MongoFindInput,
        MongoInsertBlock,
        MongoInsertInput,
    )

    # Insert
    result = await MongoInsertBlock().execute(
        MongoInsertInput(
            collection=TEST_COLLECTION,
            document='[{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]',
        ),
    )
    assert result.count == 2

    # Find all
    result = await MongoFindBlock().execute(
        MongoFindInput(collection=TEST_COLLECTION, query="{}"),
    )
    assert result.count == 2


async def test_find_one(mongo_db) -> None:
    from llming_plumber.blocks.data.mongodb import (
        MongoFindOneBlock,
        MongoFindOneInput,
        MongoInsertBlock,
        MongoInsertInput,
    )

    await MongoInsertBlock().execute(
        MongoInsertInput(
            collection=TEST_COLLECTION,
            document='{"name": "Charlie", "email": "c@test.com"}',
        ),
    )

    result = await MongoFindOneBlock().execute(
        MongoFindOneInput(collection=TEST_COLLECTION, query='{"email": "c@test.com"}'),
    )
    assert result.found is True
    assert result.document["name"] == "Charlie"


async def test_update(mongo_db) -> None:
    from llming_plumber.blocks.data.mongodb import (
        MongoInsertBlock,
        MongoInsertInput,
        MongoUpdateBlock,
        MongoUpdateInput,
    )

    await MongoInsertBlock().execute(
        MongoInsertInput(collection=TEST_COLLECTION, document='{"name": "Dana", "score": 10}'),
    )

    result = await MongoUpdateBlock().execute(
        MongoUpdateInput(
            collection=TEST_COLLECTION,
            query='{"name": "Dana"}',
            update='{"$set": {"score": 20}}',
        ),
    )
    assert result.matched_count == 1
    assert result.modified_count == 1


async def test_delete(mongo_db) -> None:
    from llming_plumber.blocks.data.mongodb import (
        MongoDeleteBlock,
        MongoDeleteInput,
        MongoInsertBlock,
        MongoInsertInput,
    )

    await MongoInsertBlock().execute(
        MongoInsertInput(
            collection=TEST_COLLECTION,
            document='[{"x": 1}, {"x": 2}, {"x": 3}]',
        ),
    )

    result = await MongoDeleteBlock().execute(
        MongoDeleteInput(collection=TEST_COLLECTION, query='{"x": {"$gte": 2}}', delete_many=True),
    )
    assert result.deleted_count == 2


async def test_aggregate(mongo_db) -> None:
    from llming_plumber.blocks.data.mongodb import (
        MongoAggregateBlock,
        MongoAggregateInput,
        MongoInsertBlock,
        MongoInsertInput,
    )

    await MongoInsertBlock().execute(
        MongoInsertInput(
            collection=TEST_COLLECTION,
            document='[{"cat": "A", "v": 10}, {"cat": "A", "v": 20}, {"cat": "B", "v": 5}]',
        ),
    )

    result = await MongoAggregateBlock().execute(
        MongoAggregateInput(
            collection=TEST_COLLECTION,
            pipeline='[{"$group": {"_id": "$cat", "total": {"$sum": "$v"}}}]',
        ),
    )
    assert result.count == 2
    totals = {d["_id"]: d["total"] for d in result.documents}
    assert totals["A"] == 30
    assert totals["B"] == 5


async def test_count(mongo_db) -> None:
    from llming_plumber.blocks.data.mongodb import (
        MongoCountBlock,
        MongoCountInput,
        MongoInsertBlock,
        MongoInsertInput,
    )

    await MongoInsertBlock().execute(
        MongoInsertInput(
            collection=TEST_COLLECTION,
            document='[{"status": "active"}, {"status": "active"}, {"status": "inactive"}]',
        ),
    )

    result = await MongoCountBlock().execute(
        MongoCountInput(collection=TEST_COLLECTION, query='{"status": "active"}'),
    )
    assert result.count == 2
