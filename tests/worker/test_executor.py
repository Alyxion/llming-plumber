"""Tests for the pipeline executor — topological sort and block execution."""

from __future__ import annotations

from typing import Any, ClassVar
from unittest.mock import patch

import pytest
from bson import ObjectId
from mongomock_motor import AsyncMongoMockClient

from llming_plumber.blocks.base import (
    BaseBlock,
    BlockContext,
    BlockInput,
    BlockOutput,
)
from llming_plumber.models.pipeline import (
    BlockDefinition,
    PipeDefinition,
    PipelineDefinition,
)
from llming_plumber.worker.executor import (
    execute_pipeline,
    run_blocks,
    topological_sort,
)

# ---------------------------------------------------------------------------
# Test blocks defined inline
# ---------------------------------------------------------------------------


class ProducerInput(BlockInput):
    value: int = 42


class ProducerOutput(BlockOutput):
    value: int


class ProducerBlock(BaseBlock[ProducerInput, ProducerOutput]):
    block_type: ClassVar[str] = "test_producer"

    async def execute(
        self, input: ProducerInput, ctx: BlockContext | None = None
    ) -> ProducerOutput:
        return ProducerOutput(value=input.value)


class ConsumerInput(BlockInput):
    value: int = 0


class ConsumerOutput(BlockOutput):
    doubled: int


class ConsumerBlock(BaseBlock[ConsumerInput, ConsumerOutput]):
    block_type: ClassVar[str] = "test_consumer"

    async def execute(
        self, input: ConsumerInput, ctx: BlockContext | None = None
    ) -> ConsumerOutput:
        return ConsumerOutput(doubled=input.value * 2)


class FailingInput(BlockInput):
    pass


class FailingOutput(BlockOutput):
    pass


class FailingBlock(BaseBlock[FailingInput, FailingOutput]):
    block_type: ClassVar[str] = "test_failing"

    async def execute(
        self, input: FailingInput, ctx: BlockContext | None = None
    ) -> FailingOutput:
        msg = "Block exploded!"
        raise RuntimeError(msg)


# ---------------------------------------------------------------------------
# Topological sort tests
# ---------------------------------------------------------------------------


class TestTopologicalSort:
    def test_linear(self) -> None:
        """A -> B -> C produces [A, B, C]."""
        blocks = [
            BlockDefinition(uid="A", block_type="x", label="A"),
            BlockDefinition(uid="B", block_type="x", label="B"),
            BlockDefinition(uid="C", block_type="x", label="C"),
        ]
        pipes = [
            PipeDefinition(
                uid="p1",
                source_block_uid="A",
                source_fitting_uid="out",
                target_block_uid="B",
                target_fitting_uid="in",
            ),
            PipeDefinition(
                uid="p2",
                source_block_uid="B",
                source_fitting_uid="out",
                target_block_uid="C",
                target_fitting_uid="in",
            ),
        ]
        result = topological_sort(blocks, pipes)
        assert result == ["A", "B", "C"]

    def test_diamond(self) -> None:
        """A -> B, A -> C, B -> D, C -> D."""
        blocks = [
            BlockDefinition(uid="A", block_type="x", label="A"),
            BlockDefinition(uid="B", block_type="x", label="B"),
            BlockDefinition(uid="C", block_type="x", label="C"),
            BlockDefinition(uid="D", block_type="x", label="D"),
        ]
        pipes = [
            PipeDefinition(
                uid="p1",
                source_block_uid="A",
                source_fitting_uid="out",
                target_block_uid="B",
                target_fitting_uid="in",
            ),
            PipeDefinition(
                uid="p2",
                source_block_uid="A",
                source_fitting_uid="out",
                target_block_uid="C",
                target_fitting_uid="in",
            ),
            PipeDefinition(
                uid="p3",
                source_block_uid="B",
                source_fitting_uid="out",
                target_block_uid="D",
                target_fitting_uid="in",
            ),
            PipeDefinition(
                uid="p4",
                source_block_uid="C",
                source_fitting_uid="out",
                target_block_uid="D",
                target_fitting_uid="in",
            ),
        ]
        result = topological_sort(blocks, pipes)
        assert result[0] == "A"
        assert result[-1] == "D"
        assert set(result) == {"A", "B", "C", "D"}

    def test_no_pipes(self) -> None:
        """Blocks with no connections: all can run in any deterministic order."""
        blocks = [
            BlockDefinition(uid="C", block_type="x", label="C"),
            BlockDefinition(uid="A", block_type="x", label="A"),
            BlockDefinition(uid="B", block_type="x", label="B"),
        ]
        result = topological_sort(blocks, [])
        # All blocks should appear, sorted deterministically
        assert set(result) == {"A", "B", "C"}
        assert len(result) == 3

    def test_cycle_raises(self) -> None:
        """A -> B -> A raises ValueError."""
        blocks = [
            BlockDefinition(uid="A", block_type="x", label="A"),
            BlockDefinition(uid="B", block_type="x", label="B"),
        ]
        pipes = [
            PipeDefinition(
                uid="p1",
                source_block_uid="A",
                source_fitting_uid="out",
                target_block_uid="B",
                target_fitting_uid="in",
            ),
            PipeDefinition(
                uid="p2",
                source_block_uid="B",
                source_fitting_uid="out",
                target_block_uid="A",
                target_fitting_uid="in",
            ),
        ]
        with pytest.raises(ValueError, match="cycle"):
            topological_sort(blocks, pipes)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_db():
    """Create a mongomock-motor async database."""
    client = AsyncMongoMockClient()
    return client["test_plumber"]


def _register_test_blocks() -> None:
    """Ensure our inline test blocks are in the registry."""
    from llming_plumber.blocks.registry import BlockRegistry

    BlockRegistry._registry["test_producer"] = ProducerBlock  # type: ignore[assignment]
    BlockRegistry._registry["test_consumer"] = ConsumerBlock  # type: ignore[assignment]
    BlockRegistry._registry["test_failing"] = FailingBlock  # type: ignore[assignment]
    BlockRegistry._discovered = True


# ---------------------------------------------------------------------------
# run_blocks tests
# ---------------------------------------------------------------------------


class TestRunBlocks:
    async def test_simple_two_block_pipeline(self) -> None:
        """Producer(42) -> Consumer -> {doubled: 84}."""
        _register_test_blocks()
        db = _mock_db()

        pipeline = PipelineDefinition(
            id="pipe1",
            name="test",
            blocks=[
                BlockDefinition(
                    uid="producer",
                    block_type="test_producer",
                    label="Producer",
                    config={"value": 42},
                ),
                BlockDefinition(
                    uid="consumer",
                    block_type="test_consumer",
                    label="Consumer",
                ),
            ],
            pipes=[
                PipeDefinition(
                    uid="p1",
                    source_block_uid="producer",
                    source_fitting_uid="out",
                    target_block_uid="consumer",
                    target_fitting_uid="in",
                ),
            ],
        )

        # Create a run doc so update_one calls succeed
        run_oid = ObjectId()
        await db["runs"].insert_one({"_id": run_oid, "status": "running"})

        result = await run_blocks(pipeline, str(run_oid), db, "test-lemming")

        assert result == {"doubled": 84}

        # Verify logs were written
        logs = await db["run_logs"].find({}).to_list(length=100)
        assert len(logs) == 2
        assert logs[0]["block_id"] == "producer"
        assert logs[1]["block_id"] == "consumer"


# ---------------------------------------------------------------------------
# execute_pipeline tests
# ---------------------------------------------------------------------------


class TestExecutePipeline:
    async def test_claims_run(self) -> None:
        """Verify atomic claim via find_one_and_update."""
        _register_test_blocks()
        db = _mock_db()

        pipeline_oid = ObjectId()
        run_oid = ObjectId()

        # Insert pipeline
        await db["pipelines"].insert_one({
            "_id": pipeline_oid,
            "name": "test",
            "blocks": [
                {
                    "uid": "producer", "block_type": "test_producer",
                    "label": "P", "config": {"value": 10},
                },
            ],
            "pipes": [],
            "version": 1,
        })

        # Insert run as queued
        await db["runs"].insert_one({
            "_id": run_oid,
            "pipeline_id": pipeline_oid,
            "status": "queued",
        })

        ctx: dict[str, Any] = {"db": db, "lemming_id": "test-lemming-1"}

        # Patch _publish_status to avoid needing real Redis
        with patch("llming_plumber.worker.executor._publish_status"):
            result = await execute_pipeline(ctx, run_id=str(run_oid))

        assert result == {"value": 10}

        # Verify run was marked completed
        run_doc = await db["runs"].find_one({"_id": run_oid})
        assert run_doc is not None
        assert run_doc["status"] == "completed"
        assert run_doc["lemming_id"] == "test-lemming-1"

    async def test_skips_already_claimed(self) -> None:
        """If run is not queued, execute_pipeline returns skipped."""
        db = _mock_db()
        run_oid = ObjectId()

        await db["runs"].insert_one({
            "_id": run_oid,
            "status": "running",  # already claimed
        })

        ctx: dict[str, Any] = {"db": db, "lemming_id": "lemming-2"}

        result = await execute_pipeline(ctx, run_id=str(run_oid))
        assert result == {"skipped": True}

    async def test_marks_failed(self) -> None:
        """When a block raises, run status becomes failed."""
        _register_test_blocks()
        db = _mock_db()

        pipeline_oid = ObjectId()
        run_oid = ObjectId()

        await db["pipelines"].insert_one({
            "_id": pipeline_oid,
            "name": "fail-test",
            "blocks": [
                {
                    "uid": "boom", "block_type": "test_failing",
                    "label": "Boom", "config": {},
                },
            ],
            "pipes": [],
            "version": 1,
        })

        await db["runs"].insert_one({
            "_id": run_oid,
            "pipeline_id": pipeline_oid,
            "status": "queued",
        })

        ctx: dict[str, Any] = {"db": db, "lemming_id": "test-lemming-3"}

        with patch("llming_plumber.worker.executor._publish_status"):
            with pytest.raises(RuntimeError, match="Block exploded"):
                await execute_pipeline(ctx, run_id=str(run_oid))

        run_doc = await db["runs"].find_one({"_id": run_oid})
        assert run_doc is not None
        assert run_doc["status"] == "failed"
        assert "Block exploded" in run_doc["error"]
