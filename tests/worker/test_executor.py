"""Tests for the pipeline executor — topological sort and block execution."""

from __future__ import annotations

from collections import defaultdict
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
from llming_plumber.blocks.registry import BlockRegistry
from llming_plumber.models.pipeline import (
    BlockDefinition,
    PipeDefinition,
    PipelineDefinition,
)
from llming_plumber.worker.executor import (
    _can_continue_after_failure,
    _execute_fan_in,
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


# ---------------------------------------------------------------------------
# Test blocks for failure tolerance
# ---------------------------------------------------------------------------


class TolerantCollectInput(BlockInput):
    items: list[dict[str, Any]] = []


class TolerantCollectOutput(BlockOutput):
    items: list[dict[str, Any]]
    count: int


class TolerantCollectBlock(BaseBlock[TolerantCollectInput, TolerantCollectOutput]):
    """Fan-in block that tolerates upstream errors."""

    block_type: ClassVar[str] = "test_tolerant_collect"
    fan_in: ClassVar[bool] = True
    tolerate_upstream_errors: ClassVar[bool] = True

    async def execute(
        self, input: TolerantCollectInput, ctx: BlockContext | None = None
    ) -> TolerantCollectOutput:
        return TolerantCollectOutput(items=input.items, count=len(input.items))


class StrictCollectInput(BlockInput):
    items: list[dict[str, Any]] = []


class StrictCollectOutput(BlockOutput):
    items: list[dict[str, Any]]
    count: int


class StrictCollectBlock(BaseBlock[StrictCollectInput, StrictCollectOutput]):
    """Fan-in block that does NOT tolerate upstream errors."""

    block_type: ClassVar[str] = "test_strict_collect"
    fan_in: ClassVar[bool] = True
    tolerate_upstream_errors: ClassVar[bool] = False

    async def execute(
        self, input: StrictCollectInput, ctx: BlockContext | None = None
    ) -> StrictCollectOutput:
        return StrictCollectOutput(items=input.items, count=len(input.items))


class TolerantConsumerInput(BlockInput):
    value: int = 0


class TolerantConsumerOutput(BlockOutput):
    doubled: int


class TolerantConsumerBlock(BaseBlock[TolerantConsumerInput, TolerantConsumerOutput]):
    """Non-fan-in block that tolerates upstream errors."""

    block_type: ClassVar[str] = "test_tolerant_consumer"
    tolerate_upstream_errors: ClassVar[bool] = True

    async def execute(
        self, input: TolerantConsumerInput, ctx: BlockContext | None = None
    ) -> TolerantConsumerOutput:
        return TolerantConsumerOutput(doubled=input.value * 2)


def _register_all_test_blocks() -> None:
    """Register all test blocks including failure tolerance blocks."""
    BlockRegistry._registry["test_producer"] = ProducerBlock  # type: ignore[assignment]
    BlockRegistry._registry["test_consumer"] = ConsumerBlock  # type: ignore[assignment]
    BlockRegistry._registry["test_failing"] = FailingBlock  # type: ignore[assignment]
    BlockRegistry._registry["test_tolerant_collect"] = TolerantCollectBlock  # type: ignore[assignment]
    BlockRegistry._registry["test_strict_collect"] = StrictCollectBlock  # type: ignore[assignment]
    BlockRegistry._registry["test_tolerant_consumer"] = TolerantConsumerBlock  # type: ignore[assignment]
    BlockRegistry._discovered = True


# ---------------------------------------------------------------------------
# Pipe/block helpers
# ---------------------------------------------------------------------------


def _pipe(
    uid: str,
    src: str,
    tgt: str,
    *,
    field_mapping: dict[str, str] | None = None,
) -> PipeDefinition:
    return PipeDefinition(
        uid=uid,
        source_block_uid=src,
        source_fitting_uid="out",
        target_block_uid=tgt,
        target_fitting_uid="in",
        field_mapping=field_mapping,
    )


def _block(uid: str, block_type: str, **config: Any) -> BlockDefinition:
    return BlockDefinition(
        uid=uid, block_type=block_type, label=uid, config=config,
    )


# ---------------------------------------------------------------------------
# _can_continue_after_failure tests
# ---------------------------------------------------------------------------


class TestCanContinueAfterFailure:
    """Tests for _can_continue_after_failure() — checks whether all
    downstream blocks tolerate upstream errors."""

    def setup_method(self) -> None:
        _register_all_test_blocks()

    def test_returns_true_when_all_downstream_tolerate(self) -> None:
        """All downstream blocks have tolerate_upstream_errors = True."""
        outgoing_pipes: dict[str, list[PipeDefinition]] = defaultdict(list)
        outgoing_pipes["failed_block"] = [
            _pipe("p1", "failed_block", "tolerant"),
        ]
        block_map = {
            "failed_block": _block("failed_block", "test_failing"),
            "tolerant": _block("tolerant", "test_tolerant_collect"),
        }
        assert _can_continue_after_failure(
            "failed_block", outgoing_pipes, block_map,
        ) is True

    def test_returns_true_multiple_tolerant_downstream(self) -> None:
        """Multiple downstream blocks, all tolerant."""
        outgoing_pipes: dict[str, list[PipeDefinition]] = defaultdict(list)
        outgoing_pipes["failed_block"] = [
            _pipe("p1", "failed_block", "tolerant_a"),
            _pipe("p2", "failed_block", "tolerant_b"),
        ]
        block_map = {
            "failed_block": _block("failed_block", "test_failing"),
            "tolerant_a": _block("tolerant_a", "test_tolerant_collect"),
            "tolerant_b": _block("tolerant_b", "test_tolerant_consumer"),
        }
        assert _can_continue_after_failure(
            "failed_block", outgoing_pipes, block_map,
        ) is True

    def test_returns_false_when_any_downstream_strict(self) -> None:
        """One downstream block doesn't tolerate errors -> False."""
        outgoing_pipes: dict[str, list[PipeDefinition]] = defaultdict(list)
        outgoing_pipes["failed_block"] = [
            _pipe("p1", "failed_block", "tolerant"),
            _pipe("p2", "failed_block", "strict"),
        ]
        block_map = {
            "failed_block": _block("failed_block", "test_failing"),
            "tolerant": _block("tolerant", "test_tolerant_collect"),
            "strict": _block("strict", "test_strict_collect"),
        }
        assert _can_continue_after_failure(
            "failed_block", outgoing_pipes, block_map,
        ) is False

    def test_returns_false_when_no_downstream(self) -> None:
        """No outgoing pipes from failed block -> False (normal abort)."""
        outgoing_pipes: dict[str, list[PipeDefinition]] = defaultdict(list)
        # No entries for "failed_block"
        block_map = {
            "failed_block": _block("failed_block", "test_failing"),
        }
        assert _can_continue_after_failure(
            "failed_block", outgoing_pipes, block_map,
        ) is False

    def test_returns_false_when_downstream_not_in_registry(self) -> None:
        """Downstream block_type not in registry -> KeyError -> False."""
        outgoing_pipes: dict[str, list[PipeDefinition]] = defaultdict(list)
        outgoing_pipes["failed_block"] = [
            _pipe("p1", "failed_block", "unknown"),
        ]
        block_map = {
            "failed_block": _block("failed_block", "test_failing"),
            "unknown": _block("unknown", "nonexistent_block_type"),
        }
        # BlockRegistry.get raises KeyError for unknown types,
        # so _can_continue_after_failure should propagate that
        with pytest.raises(KeyError):
            _can_continue_after_failure(
                "failed_block", outgoing_pipes, block_map,
            )

    def test_returns_false_when_downstream_missing_from_block_map(self) -> None:
        """target_block_uid not in block_map -> False."""
        outgoing_pipes: dict[str, list[PipeDefinition]] = defaultdict(list)
        outgoing_pipes["failed_block"] = [
            _pipe("p1", "failed_block", "ghost"),
        ]
        block_map = {
            "failed_block": _block("failed_block", "test_failing"),
            # "ghost" intentionally missing
        }
        assert _can_continue_after_failure(
            "failed_block", outgoing_pipes, block_map,
        ) is False

    def test_returns_false_single_non_tolerant_downstream(self) -> None:
        """Single downstream block without tolerate_upstream_errors -> False."""
        outgoing_pipes: dict[str, list[PipeDefinition]] = defaultdict(list)
        outgoing_pipes["failed_block"] = [
            _pipe("p1", "failed_block", "consumer"),
        ]
        block_map = {
            "failed_block": _block("failed_block", "test_failing"),
            "consumer": _block("consumer", "test_consumer"),
        }
        assert _can_continue_after_failure(
            "failed_block", outgoing_pipes, block_map,
        ) is False


# ---------------------------------------------------------------------------
# Fan-in with error parcels tests
# ---------------------------------------------------------------------------


class TestFanInErrorParcels:
    """Tests for _execute_fan_in() with failed_blocks parameter."""

    def setup_method(self) -> None:
        _register_all_test_blocks()

    async def test_tolerant_fan_in_receives_error_markers(self) -> None:
        """When failed_blocks is provided, error markers appear as items."""
        from llming_plumber.models.parcel import Parcel

        block = TolerantCollectBlock()
        incoming_pipes: dict[str, list[PipeDefinition]] = defaultdict(list)
        incoming_pipes["collector"] = [
            _pipe("p1", "ok_block", "collector"),
            _pipe("p2", "bad_block", "collector"),
        ]
        parcels: dict[str, list[Parcel]] = {
            "ok_block": [Parcel(uid="ok_block", fields={"value": 42})],
            "bad_block": [],  # no parcels because it failed
        }
        failed_blocks = {"bad_block": "Something went wrong"}

        ctx = BlockContext(run_id="r1", pipeline_id="p1", block_id="collector")
        block_def = _block("collector", "test_tolerant_collect")

        result = await _execute_fan_in(
            block, TolerantCollectInput,
            block_def, "collector",
            incoming_pipes, parcels, ctx,
            failed_blocks=failed_blocks,
        )

        assert result["count"] == 2
        items = result["items"]

        # First item: normal parcel from ok_block
        assert items[0] == {"value": 42}

        # Second item: error marker from bad_block
        assert items[1]["_error"] is True
        assert items[1]["_block_uid"] == "bad_block"
        assert items[1]["_message"] == "Something went wrong"

    async def test_strict_fan_in_does_not_receive_error_markers(self) -> None:
        """When failed_blocks is None, no error markers are included."""
        from llming_plumber.models.parcel import Parcel

        block = StrictCollectBlock()
        incoming_pipes: dict[str, list[PipeDefinition]] = defaultdict(list)
        incoming_pipes["collector"] = [
            _pipe("p1", "ok_block", "collector"),
            _pipe("p2", "bad_block", "collector"),
        ]
        parcels: dict[str, list[Parcel]] = {
            "ok_block": [Parcel(uid="ok_block", fields={"value": 42})],
            "bad_block": [],  # no parcels
        }

        ctx = BlockContext(run_id="r1", pipeline_id="p1", block_id="collector")
        block_def = _block("collector", "test_strict_collect")

        # failed_blocks=None means error markers are not included
        result = await _execute_fan_in(
            block, StrictCollectInput,
            block_def, "collector",
            incoming_pipes, parcels, ctx,
            failed_blocks=None,
        )

        assert result["count"] == 1
        items = result["items"]
        # Only the successful parcel
        assert items[0] == {"value": 42}

    async def test_fan_in_multiple_failed_blocks(self) -> None:
        """Multiple upstream failures produce multiple error markers."""
        from llming_plumber.models.parcel import Parcel

        block = TolerantCollectBlock()
        incoming_pipes: dict[str, list[PipeDefinition]] = defaultdict(list)
        incoming_pipes["collector"] = [
            _pipe("p1", "fail_a", "collector"),
            _pipe("p2", "fail_b", "collector"),
            _pipe("p3", "ok_block", "collector"),
        ]
        parcels: dict[str, list[Parcel]] = {
            "fail_a": [],
            "fail_b": [],
            "ok_block": [Parcel(uid="ok_block", fields={"result": "good"})],
        }
        failed_blocks = {
            "fail_a": "Error A",
            "fail_b": "Error B",
        }

        ctx = BlockContext(run_id="r1", pipeline_id="p1", block_id="collector")
        block_def = _block("collector", "test_tolerant_collect")

        result = await _execute_fan_in(
            block, TolerantCollectInput,
            block_def, "collector",
            incoming_pipes, parcels, ctx,
            failed_blocks=failed_blocks,
        )

        assert result["count"] == 3
        error_items = [i for i in result["items"] if i.get("_error")]
        ok_items = [i for i in result["items"] if not i.get("_error")]
        assert len(error_items) == 2
        assert len(ok_items) == 1
        assert ok_items[0] == {"result": "good"}
        error_uids = {i["_block_uid"] for i in error_items}
        assert error_uids == {"fail_a", "fail_b"}

    async def test_fan_in_no_failures_no_markers(self) -> None:
        """When failed_blocks is provided but empty, no error markers."""
        from llming_plumber.models.parcel import Parcel

        block = TolerantCollectBlock()
        incoming_pipes: dict[str, list[PipeDefinition]] = defaultdict(list)
        incoming_pipes["collector"] = [
            _pipe("p1", "block_a", "collector"),
            _pipe("p2", "block_b", "collector"),
        ]
        parcels: dict[str, list[Parcel]] = {
            "block_a": [Parcel(uid="block_a", fields={"x": 1})],
            "block_b": [Parcel(uid="block_b", fields={"y": 2})],
        }

        ctx = BlockContext(run_id="r1", pipeline_id="p1", block_id="collector")
        block_def = _block("collector", "test_tolerant_collect")

        result = await _execute_fan_in(
            block, TolerantCollectInput,
            block_def, "collector",
            incoming_pipes, parcels, ctx,
            failed_blocks={},
        )

        assert result["count"] == 2
        assert all(not i.get("_error") for i in result["items"])

    async def test_fan_in_all_upstream_failed(self) -> None:
        """All upstream blocks failed — all items are error markers."""
        from llming_plumber.models.parcel import Parcel

        block = TolerantCollectBlock()
        incoming_pipes: dict[str, list[PipeDefinition]] = defaultdict(list)
        incoming_pipes["collector"] = [
            _pipe("p1", "fail_a", "collector"),
            _pipe("p2", "fail_b", "collector"),
        ]
        parcels: dict[str, list[Parcel]] = {
            "fail_a": [],
            "fail_b": [],
        }
        failed_blocks = {
            "fail_a": "Crash A",
            "fail_b": "Crash B",
        }

        ctx = BlockContext(run_id="r1", pipeline_id="p1", block_id="collector")
        block_def = _block("collector", "test_tolerant_collect")

        result = await _execute_fan_in(
            block, TolerantCollectInput,
            block_def, "collector",
            incoming_pipes, parcels, ctx,
            failed_blocks=failed_blocks,
        )

        assert result["count"] == 2
        assert all(i["_error"] is True for i in result["items"])
        messages = {i["_message"] for i in result["items"]}
        assert messages == {"Crash A", "Crash B"}


# ---------------------------------------------------------------------------
# End-to-end failure tolerance in run_blocks
# ---------------------------------------------------------------------------


class TestFailureToleranceEndToEnd:
    """Integration tests: run_blocks continues past failures when downstream
    blocks tolerate errors, and aborts when they don't."""

    def setup_method(self) -> None:
        _register_all_test_blocks()

    async def test_pipeline_continues_when_downstream_tolerates(self) -> None:
        """Failing block -> tolerant collect: pipeline completes."""
        db = _mock_db()
        run_oid = ObjectId()
        await db["runs"].insert_one({"_id": run_oid, "status": "running"})

        pipeline = PipelineDefinition(
            id="tol1", name="tolerant-test",
            blocks=[
                _block("ok", "test_producer", value=10),
                _block("boom", "test_failing"),
                _block("coll", "test_tolerant_collect"),
            ],
            pipes=[
                _pipe("p1", "ok", "coll"),
                _pipe("p2", "boom", "coll"),
            ],
        )

        result = await run_blocks(pipeline, str(run_oid), db, "lem")

        # Pipeline completed — tolerant collect received error marker
        assert result["count"] == 2
        error_items = [i for i in result["items"] if i.get("_error")]
        ok_items = [i for i in result["items"] if not i.get("_error")]
        assert len(error_items) == 1
        assert error_items[0]["_block_uid"] == "boom"
        assert "Block exploded" in error_items[0]["_message"]
        assert len(ok_items) == 1
        assert ok_items[0]["value"] == 10

    async def test_pipeline_aborts_when_downstream_strict(self) -> None:
        """Failing block -> strict collect: pipeline aborts."""
        db = _mock_db()
        run_oid = ObjectId()
        await db["runs"].insert_one({"_id": run_oid, "status": "running"})

        pipeline = PipelineDefinition(
            id="strict1", name="strict-test",
            blocks=[
                _block("ok", "test_producer", value=10),
                _block("boom", "test_failing"),
                _block("coll", "test_strict_collect"),
            ],
            pipes=[
                _pipe("p1", "ok", "coll"),
                _pipe("p2", "boom", "coll"),
            ],
        )

        with pytest.raises(RuntimeError, match="Block exploded"):
            await run_blocks(pipeline, str(run_oid), db, "lem")

    async def test_pipeline_aborts_when_mixed_downstream(self) -> None:
        """Failing block feeds both tolerant and strict blocks: aborts."""
        db = _mock_db()
        run_oid = ObjectId()
        await db["runs"].insert_one({"_id": run_oid, "status": "running"})

        pipeline = PipelineDefinition(
            id="mixed1", name="mixed-test",
            blocks=[
                _block("boom", "test_failing"),
                _block("tolerant", "test_tolerant_collect"),
                _block("strict", "test_strict_collect"),
            ],
            pipes=[
                _pipe("p1", "boom", "tolerant"),
                _pipe("p2", "boom", "strict"),
            ],
        )

        with pytest.raises(RuntimeError, match="Block exploded"):
            await run_blocks(pipeline, str(run_oid), db, "lem")

    async def test_failed_block_recorded_in_block_states(self) -> None:
        """When a failure is tolerated, the failed block's state is still
        recorded as 'failed' in the run document."""
        db = _mock_db()
        run_oid = ObjectId()
        await db["runs"].insert_one({"_id": run_oid, "status": "running"})

        pipeline = PipelineDefinition(
            id="state1", name="state-test",
            blocks=[
                _block("boom", "test_failing"),
                _block("coll", "test_tolerant_collect"),
            ],
            pipes=[
                _pipe("p1", "boom", "coll"),
            ],
        )

        await run_blocks(pipeline, str(run_oid), db, "lem")

        run_doc = await db["runs"].find_one({"_id": run_oid})
        assert run_doc["block_states"]["boom"]["status"] == "failed"
        assert "Block exploded" in run_doc["block_states"]["boom"]["error"]
        assert run_doc["block_states"]["coll"]["status"] == "completed"


# ---------------------------------------------------------------------------
# Resume / checkpoint tests
# ---------------------------------------------------------------------------


class TestResumeFromCheckpoint:
    """Tests for executor resume: skipping completed blocks and restoring
    their parcels from saved block_states."""

    def setup_method(self) -> None:
        _register_all_test_blocks()

    async def test_resume_skips_completed_blocks(self) -> None:
        """Blocks with saved status='completed' and output are skipped."""
        db = _mock_db()
        run_oid = ObjectId()

        # Pre-populate run doc with producer already completed
        await db["runs"].insert_one({
            "_id": run_oid,
            "status": "running",
            "block_states": {
                "producer": {
                    "status": "completed",
                    "output": {"value": 99},
                    "duration_ms": 10,
                },
            },
        })

        pipeline = PipelineDefinition(
            id="resume1", name="resume-test",
            blocks=[
                _block("producer", "test_producer", value=42),
                _block("consumer", "test_consumer"),
            ],
            pipes=[_pipe("p1", "producer", "consumer")],
        )

        result = await run_blocks(pipeline, str(run_oid), db, "lem")

        # Consumer should use the saved output (99), not the config value (42)
        assert result == {"doubled": 198}

        # Check the block log has "resumed" status for producer
        run_doc = await db["runs"].find_one({"_id": run_oid})
        log = run_doc.get("log", [])
        producer_log = next(e for e in log if e["uid"] == "producer")
        assert producer_log["status"] == "resumed"
        consumer_log = next(e for e in log if e["uid"] == "consumer")
        assert consumer_log["status"] == "completed"

    async def test_resume_re_executes_cleared_failed_blocks(self) -> None:
        """Blocks with cleared status (empty string) are re-executed."""
        db = _mock_db()
        run_oid = ObjectId()

        # Simulate a resumed run: producer completed, consumer was failed
        # but its status was cleared by the resume endpoint
        await db["runs"].insert_one({
            "_id": run_oid,
            "status": "running",
            "block_states": {
                "producer": {
                    "status": "completed",
                    "output": {"value": 50},
                    "duration_ms": 5,
                },
                "consumer": {
                    "status": "",  # cleared by resume endpoint
                    "error": None,
                },
            },
        })

        pipeline = PipelineDefinition(
            id="resume2", name="resume-cleared",
            blocks=[
                _block("producer", "test_producer", value=42),
                _block("consumer", "test_consumer"),
            ],
            pipes=[_pipe("p1", "producer", "consumer")],
        )

        result = await run_blocks(pipeline, str(run_oid), db, "lem")

        # Consumer uses producer's saved output (50) and executes
        assert result == {"doubled": 100}

    async def test_fresh_run_no_skip(self) -> None:
        """A fresh run with empty block_states executes all blocks."""
        db = _mock_db()
        run_oid = ObjectId()

        await db["runs"].insert_one({
            "_id": run_oid,
            "status": "running",
        })

        pipeline = PipelineDefinition(
            id="fresh1", name="fresh-test",
            blocks=[
                _block("producer", "test_producer", value=7),
                _block("consumer", "test_consumer"),
            ],
            pipes=[_pipe("p1", "producer", "consumer")],
        )

        result = await run_blocks(pipeline, str(run_oid), db, "lem")
        assert result == {"doubled": 14}

    async def test_output_always_saved_in_block_states(self) -> None:
        """Block output is always persisted for resume support."""
        db = _mock_db()
        run_oid = ObjectId()
        await db["runs"].insert_one({"_id": run_oid, "status": "running"})

        pipeline = PipelineDefinition(
            id="save1", name="save-output-test",
            blocks=[_block("producer", "test_producer", value=42)],
            pipes=[],
        )

        await run_blocks(pipeline, str(run_oid), db, "lem")

        run_doc = await db["runs"].find_one({"_id": run_oid})
        assert run_doc["block_states"]["producer"]["output"] == {"value": 42}
