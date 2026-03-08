"""Unit tests for MongoDB operation blocks — mocked Motor."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from llming_plumber.blocks.base import BlockContext


def _ctx(pipeline_id: str = "pl1") -> BlockContext:
    return BlockContext(pipeline_id=pipeline_id, run_id="run1")


def _mock_cursor(docs: list | None = None) -> MagicMock:
    """Create a chainable cursor mock (sort/skip/limit return self, to_list is async)."""
    cursor = MagicMock()
    cursor.sort = MagicMock(return_value=cursor)
    cursor.skip = MagicMock(return_value=cursor)
    cursor.limit = MagicMock(return_value=cursor)
    cursor.to_list = AsyncMock(return_value=docs or [])
    return cursor


def _mock_collection() -> MagicMock:
    """Create a mock MongoDB collection."""
    col = MagicMock()
    col.find = MagicMock(return_value=_mock_cursor())
    col.find_one = AsyncMock(return_value=None)
    col.insert_one = AsyncMock()
    col.insert_many = AsyncMock()
    col.update_one = AsyncMock()
    col.update_many = AsyncMock()
    col.delete_one = AsyncMock()
    col.delete_many = AsyncMock()
    col.aggregate = MagicMock(return_value=_mock_cursor())
    col.count_documents = AsyncMock(return_value=0)
    return col


def _mock_db(collection: MagicMock | None = None) -> MagicMock:
    db = MagicMock()
    col = collection or _mock_collection()
    db.__getitem__ = MagicMock(return_value=col)
    return db


# ── MongoFindBlock ──


@pytest.mark.asyncio
async def test_find_returns_documents(monkeypatch: pytest.MonkeyPatch) -> None:
    from llming_plumber.blocks.data.mongodb import MongoFindBlock, MongoFindInput

    docs = [{"_id": "1", "name": "a"}, {"_id": "2", "name": "b"}]
    col = _mock_collection()
    col.find = MagicMock(return_value=_mock_cursor(docs))
    db = _mock_db(col)
    monkeypatch.setattr("llming_plumber.blocks.data.mongodb._get_database", lambda: db)

    block = MongoFindBlock()
    result = await block.execute(
        MongoFindInput(collection="users", query='{"name": "a"}', limit=10),
    )
    assert result.count == 2
    assert len(result.documents) == 2
    col.find.assert_called_once()


@pytest.mark.asyncio
async def test_find_empty_query(monkeypatch: pytest.MonkeyPatch) -> None:
    from llming_plumber.blocks.data.mongodb import MongoFindBlock, MongoFindInput

    col = _mock_collection()
    col.find = MagicMock(return_value=_mock_cursor([]))
    db = _mock_db(col)
    monkeypatch.setattr("llming_plumber.blocks.data.mongodb._get_database", lambda: db)

    block = MongoFindBlock()
    result = await block.execute(MongoFindInput(collection="users", query="{}"))
    assert result.count == 0
    assert result.documents == []


@pytest.mark.asyncio
async def test_find_with_sort_and_skip(monkeypatch: pytest.MonkeyPatch) -> None:
    from llming_plumber.blocks.data.mongodb import MongoFindBlock, MongoFindInput

    col = _mock_collection()
    col.find = MagicMock(return_value=_mock_cursor([{"_id": "1", "x": 1}]))
    db = _mock_db(col)
    monkeypatch.setattr("llming_plumber.blocks.data.mongodb._get_database", lambda: db)

    block = MongoFindBlock()
    result = await block.execute(
        MongoFindInput(collection="items", query="{}", sort='{"created": -1}', skip=5, limit=10),
    )
    assert result.count == 1


@pytest.mark.asyncio
async def test_find_with_projection(monkeypatch: pytest.MonkeyPatch) -> None:
    from llming_plumber.blocks.data.mongodb import MongoFindBlock, MongoFindInput

    col = _mock_collection()
    col.find = MagicMock(return_value=_mock_cursor([{"name": "a"}]))
    db = _mock_db(col)
    monkeypatch.setattr("llming_plumber.blocks.data.mongodb._get_database", lambda: db)

    block = MongoFindBlock()
    result = await block.execute(
        MongoFindInput(collection="users", query="{}", projection='{"_id": 0, "name": 1}'),
    )
    assert result.count == 1
    col.find.assert_called_once_with({}, {"_id": 0, "name": 1})


# ── MongoFindOneBlock ──


@pytest.mark.asyncio
async def test_find_one_found(monkeypatch: pytest.MonkeyPatch) -> None:
    from llming_plumber.blocks.data.mongodb import MongoFindOneBlock, MongoFindOneInput

    col = _mock_collection()
    col.find_one = AsyncMock(return_value={"_id": "abc", "email": "x@y.com"})
    db = _mock_db(col)
    monkeypatch.setattr("llming_plumber.blocks.data.mongodb._get_database", lambda: db)

    block = MongoFindOneBlock()
    result = await block.execute(MongoFindOneInput(collection="users", query='{"email": "x@y.com"}'))
    assert result.found is True
    assert result.document["email"] == "x@y.com"


@pytest.mark.asyncio
async def test_find_one_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    from llming_plumber.blocks.data.mongodb import MongoFindOneBlock, MongoFindOneInput

    col = _mock_collection()
    col.find_one = AsyncMock(return_value=None)
    db = _mock_db(col)
    monkeypatch.setattr("llming_plumber.blocks.data.mongodb._get_database", lambda: db)

    block = MongoFindOneBlock()
    result = await block.execute(MongoFindOneInput(collection="users", query='{"x": 1}'))
    assert result.found is False


# ── MongoInsertBlock ──


@pytest.mark.asyncio
async def test_insert_single(monkeypatch: pytest.MonkeyPatch) -> None:
    from llming_plumber.blocks.data.mongodb import MongoInsertBlock, MongoInsertInput

    col = _mock_collection()
    mock_result = MagicMock()
    mock_result.inserted_id = "abc123"
    col.insert_one = AsyncMock(return_value=mock_result)
    db = _mock_db(col)
    monkeypatch.setattr("llming_plumber.blocks.data.mongodb._get_database", lambda: db)

    block = MongoInsertBlock()
    result = await block.execute(
        MongoInsertInput(collection="users", document='{"name": "Alice"}'),
    )
    assert result.count == 1
    assert len(result.inserted_ids) == 1


@pytest.mark.asyncio
async def test_insert_many(monkeypatch: pytest.MonkeyPatch) -> None:
    from llming_plumber.blocks.data.mongodb import MongoInsertBlock, MongoInsertInput

    col = _mock_collection()
    mock_result = MagicMock()
    mock_result.inserted_ids = ["id1", "id2"]
    col.insert_many = AsyncMock(return_value=mock_result)
    db = _mock_db(col)
    monkeypatch.setattr("llming_plumber.blocks.data.mongodb._get_database", lambda: db)

    block = MongoInsertBlock()
    result = await block.execute(
        MongoInsertInput(collection="users", document='[{"name": "A"}, {"name": "B"}]'),
    )
    assert result.count == 2


# ── MongoUpdateBlock ──


@pytest.mark.asyncio
async def test_update_one(monkeypatch: pytest.MonkeyPatch) -> None:
    from llming_plumber.blocks.data.mongodb import MongoUpdateBlock, MongoUpdateInput

    col = _mock_collection()
    mock_result = MagicMock()
    mock_result.matched_count = 1
    mock_result.modified_count = 1
    mock_result.upserted_id = None
    col.update_one = AsyncMock(return_value=mock_result)
    db = _mock_db(col)
    monkeypatch.setattr("llming_plumber.blocks.data.mongodb._get_database", lambda: db)

    block = MongoUpdateBlock()
    result = await block.execute(
        MongoUpdateInput(
            collection="users",
            query='{"name": "Alice"}',
            update='{"$set": {"age": 30}}',
        ),
    )
    assert result.matched_count == 1
    assert result.modified_count == 1


@pytest.mark.asyncio
async def test_update_with_upsert(monkeypatch: pytest.MonkeyPatch) -> None:
    from llming_plumber.blocks.data.mongodb import MongoUpdateBlock, MongoUpdateInput

    col = _mock_collection()
    mock_result = MagicMock()
    mock_result.matched_count = 0
    mock_result.modified_count = 0
    mock_result.upserted_id = "new123"
    col.update_one = AsyncMock(return_value=mock_result)
    db = _mock_db(col)
    monkeypatch.setattr("llming_plumber.blocks.data.mongodb._get_database", lambda: db)

    block = MongoUpdateBlock()
    result = await block.execute(
        MongoUpdateInput(
            collection="users",
            query='{"name": "Bob"}',
            update='{"$set": {"age": 25}}',
            upsert=True,
        ),
    )
    assert result.upserted_id == "new123"


@pytest.mark.asyncio
async def test_update_many(monkeypatch: pytest.MonkeyPatch) -> None:
    from llming_plumber.blocks.data.mongodb import MongoUpdateBlock, MongoUpdateInput

    col = _mock_collection()
    mock_result = MagicMock()
    mock_result.matched_count = 10
    mock_result.modified_count = 10
    mock_result.upserted_id = None
    col.update_many = AsyncMock(return_value=mock_result)
    db = _mock_db(col)
    monkeypatch.setattr("llming_plumber.blocks.data.mongodb._get_database", lambda: db)

    block = MongoUpdateBlock()
    result = await block.execute(
        MongoUpdateInput(
            collection="users",
            query='{"active": false}',
            update='{"$set": {"active": true}}',
            update_many=True,
        ),
    )
    assert result.matched_count == 10


# ── MongoDeleteBlock ──


@pytest.mark.asyncio
async def test_delete_one(monkeypatch: pytest.MonkeyPatch) -> None:
    from llming_plumber.blocks.data.mongodb import MongoDeleteBlock, MongoDeleteInput

    col = _mock_collection()
    mock_result = MagicMock()
    mock_result.deleted_count = 1
    col.delete_one = AsyncMock(return_value=mock_result)
    db = _mock_db(col)
    monkeypatch.setattr("llming_plumber.blocks.data.mongodb._get_database", lambda: db)

    block = MongoDeleteBlock()
    result = await block.execute(
        MongoDeleteInput(collection="users", query='{"name": "Alice"}'),
    )
    assert result.deleted_count == 1


@pytest.mark.asyncio
async def test_delete_many(monkeypatch: pytest.MonkeyPatch) -> None:
    from llming_plumber.blocks.data.mongodb import MongoDeleteBlock, MongoDeleteInput

    col = _mock_collection()
    mock_result = MagicMock()
    mock_result.deleted_count = 5
    col.delete_many = AsyncMock(return_value=mock_result)
    db = _mock_db(col)
    monkeypatch.setattr("llming_plumber.blocks.data.mongodb._get_database", lambda: db)

    block = MongoDeleteBlock()
    result = await block.execute(
        MongoDeleteInput(collection="users", query='{"active": false}', delete_many=True),
    )
    assert result.deleted_count == 5


# ── MongoAggregateBlock ──


@pytest.mark.asyncio
async def test_aggregate(monkeypatch: pytest.MonkeyPatch) -> None:
    from llming_plumber.blocks.data.mongodb import MongoAggregateBlock, MongoAggregateInput

    results = [{"_id": "group1", "total": 100}]
    col = _mock_collection()
    cursor = _mock_cursor(results)
    col.aggregate = MagicMock(return_value=cursor)
    db = _mock_db(col)
    monkeypatch.setattr("llming_plumber.blocks.data.mongodb._get_database", lambda: db)

    block = MongoAggregateBlock()
    result = await block.execute(
        MongoAggregateInput(
            collection="orders",
            pipeline='[{"$group": {"_id": "$category", "total": {"$sum": "$amount"}}}]',
        ),
    )
    assert result.count == 1
    assert result.documents[0]["total"] == 100


# ── MongoCountBlock ──


@pytest.mark.asyncio
async def test_count(monkeypatch: pytest.MonkeyPatch) -> None:
    from llming_plumber.blocks.data.mongodb import MongoCountBlock, MongoCountInput

    col = _mock_collection()
    col.count_documents = AsyncMock(return_value=42)
    db = _mock_db(col)
    monkeypatch.setattr("llming_plumber.blocks.data.mongodb._get_database", lambda: db)

    block = MongoCountBlock()
    result = await block.execute(MongoCountInput(collection="users", query="{}"))
    assert result.count == 42


@pytest.mark.asyncio
async def test_count_with_filter(monkeypatch: pytest.MonkeyPatch) -> None:
    from llming_plumber.blocks.data.mongodb import MongoCountBlock, MongoCountInput

    col = _mock_collection()
    col.count_documents = AsyncMock(return_value=7)
    db = _mock_db(col)
    monkeypatch.setattr("llming_plumber.blocks.data.mongodb._get_database", lambda: db)

    block = MongoCountBlock()
    result = await block.execute(
        MongoCountInput(collection="users", query='{"active": true}'),
    )
    assert result.count == 7
    col.count_documents.assert_called_once_with({"active": True})


# ── No database available ──


@pytest.mark.asyncio
async def test_find_no_db(monkeypatch: pytest.MonkeyPatch) -> None:
    from llming_plumber.blocks.data.mongodb import MongoFindBlock, MongoFindInput

    monkeypatch.setattr("llming_plumber.blocks.data.mongodb._get_database", lambda: None)

    block = MongoFindBlock()
    result = await block.execute(MongoFindInput(collection="users", query="{}"))
    assert result.count == 0
    assert result.documents == []


# ── Registry ──


def test_mongo_blocks_in_registry() -> None:
    from llming_plumber.blocks.registry import BlockRegistry

    BlockRegistry.reset()
    BlockRegistry.discover()
    for bt in [
        "mongo_find", "mongo_find_one", "mongo_insert", "mongo_update",
        "mongo_delete", "mongo_aggregate", "mongo_count",
    ]:
        assert bt in BlockRegistry._registry, f"{bt} not registered"
