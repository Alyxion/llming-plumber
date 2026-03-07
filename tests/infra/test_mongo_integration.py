"""Integration tests for MongoDB — real local instance.

Covers: CRUD, indexing, atomic operations, querying, nested
document storage (pipeline graphs), and bulk operations.

Requires a running MongoDB on localhost:27017.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import pytest
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient

from llming_plumber.db import ensure_indexes
from llming_plumber.models.mongo_helpers import doc_to_model, model_to_doc
from llming_plumber.models.pipeline import (
    BlockDefinition,
    BlockPosition,
    PipeDefinition,
    PipelineDefinition,
)
from llming_plumber.models.run import Run, RunStatus

pytestmark = pytest.mark.integration

TEST_DB = "plumber_integration_test"


@pytest.fixture()
async def db():
    """Provide a clean test database, dropped after the test."""
    client: AsyncIOMotorClient = AsyncIOMotorClient(  # type: ignore[type-arg]
        "mongodb://localhost:27017",
    )
    database = client[TEST_DB]
    yield database
    await client.drop_database(TEST_DB)
    client.close()


# -------------------------------------------------------------------
# Basic connectivity
# -------------------------------------------------------------------


async def test_ping(db: Any) -> None:
    result = await db.command("ping")
    assert result["ok"] == 1.0


# -------------------------------------------------------------------
# Pipeline CRUD — full graph storage
# -------------------------------------------------------------------


async def test_insert_and_retrieve_pipeline(db: Any) -> None:
    """Insert a full pipeline graph and read it back."""
    pipeline = PipelineDefinition(
        name="Integration Test Pipeline",
        description="Two-block pipeline for testing",
        owner_id="user-1",
        tags=["test", "integration"],
        blocks=[
            BlockDefinition(
                uid="fetch",
                block_type="rss_reader",
                label="Fetch",
                config={"url": "https://example.com/feed", "max_items": 3},
                position=BlockPosition(x=100, y=200),
            ),
            BlockDefinition(
                uid="summarize",
                block_type="llm_summarizer",
                label="Summarize",
                config={"provider": "openai", "model": "gpt-5-nano"},
                position=BlockPosition(x=400, y=200),
            ),
        ],
        pipes=[
            PipeDefinition(
                uid="p1",
                source_block_uid="fetch",
                source_fitting_uid="output",
                target_block_uid="summarize",
                target_fitting_uid="input",
                field_mapping={"entries": "text"},
            ),
        ],
    )

    doc = model_to_doc(pipeline)
    result = await db["pipelines"].insert_one(doc)
    oid = result.inserted_id
    assert isinstance(oid, ObjectId)

    # Read back
    stored = await db["pipelines"].find_one({"_id": oid})
    assert stored is not None
    restored = doc_to_model(stored, PipelineDefinition)

    assert restored.name == "Integration Test Pipeline"
    assert len(restored.blocks) == 2
    assert len(restored.pipes) == 1
    assert restored.blocks[0].position.x == 100
    assert restored.pipes[0].field_mapping == {"entries": "text"}
    assert restored.tags == ["test", "integration"]
    assert restored.id == str(oid)


async def test_update_pipeline(db: Any) -> None:
    """Update a pipeline and verify version bump."""
    pipeline = PipelineDefinition(
        name="Versioned",
        blocks=[
            BlockDefinition(
                uid="b1", block_type="filter", label="B1",
            ),
        ],
    )
    doc = model_to_doc(pipeline)
    res = await db["pipelines"].insert_one(doc)
    oid = res.inserted_id

    # Add a second block and bump version
    await db["pipelines"].update_one(
        {"_id": oid},
        {
            "$push": {
                "blocks": {
                    "uid": "b2",
                    "block_type": "aggregate",
                    "label": "B2",
                    "config": {},
                    "position": {"x": 300, "y": 200},
                    "notes": "",
                },
            },
            "$set": {"version": 2, "updated_at": datetime.now(UTC)},
        },
    )

    updated = await db["pipelines"].find_one({"_id": oid})
    assert updated is not None
    assert updated["version"] == 2
    assert len(updated["blocks"]) == 2


async def test_delete_pipeline(db: Any) -> None:
    doc = model_to_doc(
        PipelineDefinition(name="ToDelete"),
    )
    res = await db["pipelines"].insert_one(doc)
    oid = res.inserted_id

    result = await db["pipelines"].delete_one({"_id": oid})
    assert result.deleted_count == 1

    gone = await db["pipelines"].find_one({"_id": oid})
    assert gone is None


# -------------------------------------------------------------------
# Query and filter
# -------------------------------------------------------------------


async def test_query_by_owner_and_tag(db: Any) -> None:
    """Query pipelines by owner_id and tags."""
    for i, (owner, tags) in enumerate([
        ("alice", ["prod"]),
        ("alice", ["dev"]),
        ("bob", ["prod"]),
    ]):
        doc = model_to_doc(
            PipelineDefinition(
                name=f"P{i}",
                owner_id=owner,
                tags=tags,
            ),
        )
        await db["pipelines"].insert_one(doc)

    # Alice's pipelines
    alice_docs = await db["pipelines"].find(
        {"owner_id": "alice"},
    ).to_list(length=100)
    assert len(alice_docs) == 2

    # Prod pipelines
    prod_docs = await db["pipelines"].find(
        {"tags": "prod"},
    ).to_list(length=100)
    assert len(prod_docs) == 2

    # Alice + prod
    both = await db["pipelines"].find(
        {"owner_id": "alice", "tags": "prod"},
    ).to_list(length=100)
    assert len(both) == 1


# -------------------------------------------------------------------
# Indexes
# -------------------------------------------------------------------


async def test_ensure_indexes(db: Any) -> None:
    """Verify that ensure_indexes creates the expected indexes."""
    await ensure_indexes(db)

    run_indexes = await db["runs"].index_information()
    # Should have compound indexes beyond _id
    assert len(run_indexes) > 1

    schedule_indexes = await db["schedules"].index_information()
    assert len(schedule_indexes) > 1

    pipeline_indexes = await db["pipelines"].index_information()
    # name unique index
    name_idx = [
        v for v in pipeline_indexes.values()
        if any(k == "name" for k, _ in v.get("key", []))
    ]
    assert len(name_idx) == 1
    assert name_idx[0].get("unique") is True


async def test_unique_pipeline_name(db: Any) -> None:
    """Duplicate pipeline names are rejected after indexes."""
    await ensure_indexes(db)

    doc1 = model_to_doc(PipelineDefinition(name="Unique"))
    doc2 = model_to_doc(PipelineDefinition(name="Unique"))
    await db["pipelines"].insert_one(doc1)

    from pymongo.errors import DuplicateKeyError

    with pytest.raises(DuplicateKeyError):
        await db["pipelines"].insert_one(doc2)


# -------------------------------------------------------------------
# Atomic operations (run claiming)
# -------------------------------------------------------------------


async def test_atomic_run_claim(db: Any) -> None:
    """Only one lemming can claim a queued run."""
    from pymongo import ReturnDocument

    run_doc = {
        "pipeline_id": ObjectId(),
        "status": "queued",
        "created_at": datetime.now(UTC),
    }
    res = await db["runs"].insert_one(run_doc)
    run_id = res.inserted_id

    # Simulate two lemmings racing to claim
    async def claim(lemming_id: str) -> dict[str, Any] | None:
        return await db["runs"].find_one_and_update(
            {"_id": run_id, "status": "queued"},
            {"$set": {
                "status": "running",
                "lemming_id": lemming_id,
            }},
            return_document=ReturnDocument.AFTER,
        )

    r1, r2 = await asyncio.gather(claim("lem-1"), claim("lem-2"))

    # Exactly one should succeed
    winners = [r for r in [r1, r2] if r is not None]
    assert len(winners) == 1
    assert winners[0]["status"] == "running"


# -------------------------------------------------------------------
# Run lifecycle
# -------------------------------------------------------------------


async def test_run_lifecycle(db: Any) -> None:
    """Create, start, complete a run — verify all state transitions."""
    run = Run(
        pipeline_id="pipe-1",
        status=RunStatus.queued,
        tags=["e2e"],
    )
    doc = model_to_doc(run)
    doc.pop("_id", None)
    res = await db["runs"].insert_one(doc)
    run_id = res.inserted_id

    # Verify queued
    stored = await db["runs"].find_one({"_id": run_id})
    assert stored["status"] == "queued"

    # Transition to running
    await db["runs"].update_one(
        {"_id": run_id},
        {"$set": {
            "status": "running",
            "lemming_id": "lem-1",
            "started_at": datetime.now(UTC),
        }},
    )

    # Transition to completed
    now = datetime.now(UTC)
    await db["runs"].update_one(
        {"_id": run_id},
        {"$set": {
            "status": "completed",
            "finished_at": now,
            "output": {"summary": "Done"},
            "block_states.b1.status": "completed",
            "block_states.b1.duration_ms": 123.4,
        }},
    )

    final = await db["runs"].find_one({"_id": run_id})
    assert final["status"] == "completed"
    assert final["block_states"]["b1"]["status"] == "completed"
    assert final["output"]["summary"] == "Done"


async def test_query_runs_by_status(db: Any) -> None:
    """Filter runs by status."""
    for status in ["queued", "running", "completed", "failed"]:
        await db["runs"].insert_one({
            "pipeline_id": "p1",
            "status": status,
            "created_at": datetime.now(UTC),
        })

    queued = await db["runs"].find(
        {"status": "queued"},
    ).to_list(length=100)
    assert len(queued) == 1

    active = await db["runs"].find(
        {"status": {"$in": ["queued", "running"]}},
    ).to_list(length=100)
    assert len(active) == 2


# -------------------------------------------------------------------
# Run logs (append-only)
# -------------------------------------------------------------------


async def test_run_logs(db: Any) -> None:
    """Insert and query run log entries."""
    run_id = "run-abc"
    logs = [
        {
            "run_id": run_id,
            "block_id": f"b{i}",
            "block_type": "filter",
            "ts": datetime.now(UTC),
            "level": "info",
            "msg": f"Block b{i} completed",
            "duration_ms": 50.0 + i * 10,
        }
        for i in range(5)
    ]
    await db["run_logs"].insert_many(logs)

    stored = await db["run_logs"].find(
        {"run_id": run_id},
    ).sort("ts", 1).to_list(length=100)

    assert len(stored) == 5
    assert stored[0]["block_id"] == "b0"
    assert stored[4]["block_id"] == "b4"


# -------------------------------------------------------------------
# Schedules
# -------------------------------------------------------------------


async def test_schedule_due_query(db: Any) -> None:
    """Find schedules that are due for execution."""
    now = datetime.now(UTC)
    past = datetime(2025, 1, 1, tzinfo=UTC)
    future = datetime(2030, 1, 1, tzinfo=UTC)

    await db["schedules"].insert_many([
        {
            "pipeline_id": "p1",
            "enabled": True,
            "cron_expression": "* * * * *",
            "next_run_at": past,
        },
        {
            "pipeline_id": "p2",
            "enabled": True,
            "cron_expression": "0 0 * * *",
            "next_run_at": future,
        },
        {
            "pipeline_id": "p3",
            "enabled": False,
            "cron_expression": "* * * * *",
            "next_run_at": past,
        },
    ])

    due = await db["schedules"].find({
        "enabled": True,
        "next_run_at": {"$lte": now},
    }).to_list(length=100)

    assert len(due) == 1
    assert due[0]["pipeline_id"] == "p1"


# -------------------------------------------------------------------
# Bulk operations
# -------------------------------------------------------------------


async def test_bulk_insert_and_count(db: Any) -> None:
    """Insert many pipelines and verify count."""
    docs = [
        model_to_doc(PipelineDefinition(name=f"Bulk-{i}"))
        for i in range(50)
    ]
    result = await db["pipelines"].insert_many(docs)
    assert len(result.inserted_ids) == 50

    count = await db["pipelines"].count_documents({})
    assert count == 50


# -------------------------------------------------------------------
# Nested document updates (block states)
# -------------------------------------------------------------------


async def test_nested_block_state_updates(db: Any) -> None:
    """Update individual block states using dot notation."""
    run_id = (await db["runs"].insert_one({
        "status": "running",
        "block_states": {},
    })).inserted_id

    # Set block states one at a time
    for uid in ["fetch", "transform", "store"]:
        await db["runs"].update_one(
            {"_id": run_id},
            {"$set": {
                f"block_states.{uid}.status": "completed",
                f"block_states.{uid}.duration_ms": 42.0,
                f"block_states.{uid}.output": {"ok": True},
            }},
        )

    doc = await db["runs"].find_one({"_id": run_id})
    assert len(doc["block_states"]) == 3
    for uid in ["fetch", "transform", "store"]:
        assert doc["block_states"][uid]["status"] == "completed"
