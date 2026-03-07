"""Tests for the debug trace system."""

from __future__ import annotations

import json
from typing import Any

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
from llming_plumber.worker.debug_trace import (
    DebugTracer,
    _glimpse_label,
    _truncate_parcel,
    get_debug_parcel,
    get_debug_trace,
    search_debug_parcels,
)
from llming_plumber.worker.executor import run_blocks

# ------------------------------------------------------------------
# Fake Redis for testing (dict-backed)
# ------------------------------------------------------------------


class FakeRedis:
    """Minimal async Redis stub backed by a dict."""

    def __init__(self) -> None:
        self.store: dict[str, str] = {}

    async def setex(self, key: str, ttl: int, value: str) -> None:
        self.store[key] = value

    async def get(self, key: str) -> str | None:
        return self.store.get(key)


# ------------------------------------------------------------------
# Test blocks
# ------------------------------------------------------------------


class _FileScanInput(BlockInput):
    directory: str = "/data"


class _FileScanOutput(BlockOutput):
    items: list[dict[str, Any]]
    total: int


class FileScanBlock(BaseBlock[_FileScanInput, _FileScanOutput]):
    block_type = "test_file_scan"
    fan_out_field = "items"

    async def execute(
        self, input: _FileScanInput, ctx: BlockContext | None = None,
    ) -> _FileScanOutput:
        files = [
            {"filename": "report.xlsx", "size": 1024},
            {"filename": "data.csv", "size": 2048},
            {"filename": "image.png", "size": 4096},
        ]
        return _FileScanOutput(items=files, total=len(files))


class _ProcessInput(BlockInput):
    filename: str = ""
    size: int = 0


class _ProcessOutput(BlockOutput):
    result: str
    filename: str


class ProcessBlock(BaseBlock[_ProcessInput, _ProcessOutput]):
    block_type = "test_process"

    async def execute(
        self, input: _ProcessInput, ctx: BlockContext | None = None,
    ) -> _ProcessOutput:
        return _ProcessOutput(
            result=f"processed:{input.filename}",
            filename=input.filename,
        )


class _FailInput(BlockInput):
    text: str = ""


class _FailOutput(BlockOutput):
    result: str = ""


class FailBlock(BaseBlock[_FailInput, _FailOutput]):
    block_type = "test_always_fail"

    async def execute(
        self, input: _FailInput, ctx: BlockContext | None = None,
    ) -> _FailOutput:
        msg = "intentional failure"
        raise RuntimeError(msg)


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _block(uid: str, btype: str, **config: Any) -> BlockDefinition:
    return BlockDefinition(uid=uid, block_type=btype, label=uid, config=config)


def _pipe(uid: str, src: str, tgt: str, **kw: Any) -> PipeDefinition:
    return PipeDefinition(
        uid=uid,
        source_block_uid=src,
        source_fitting_uid="out",
        target_block_uid=tgt,
        target_fitting_uid="in",
        **kw,
    )


# ------------------------------------------------------------------
# Unit tests: glimpse extraction
# ------------------------------------------------------------------


class TestGlimpseLabel:
    def test_filename_field(self) -> None:
        fields = {"filename": "report.xlsx", "size": 42}
        assert _glimpse_label(fields, 0) == "report.xlsx"

    def test_name_field(self) -> None:
        assert _glimpse_label({"name": "My Doc", "x": 1}, 0) == "My Doc"

    def test_title_field(self) -> None:
        assert _glimpse_label({"title": "Quarterly Report"}, 0) == "Quarterly Report"

    def test_url_field(self) -> None:
        assert _glimpse_label({"url": "https://example.com"}, 3) == "https://example.com"

    def test_fallback_to_string_value(self) -> None:
        assert _glimpse_label({"x": 42, "msg": "hello"}, 0) == "hello"

    def test_fallback_to_index(self) -> None:
        assert _glimpse_label({"x": 42, "y": 99}, 5) == "item-5"

    def test_long_value_truncated(self) -> None:
        long_name = "A" * 200
        label = _glimpse_label({"name": long_name}, 0)
        assert len(label) == 120
        assert label.endswith("...")

    def test_blob_name_field(self) -> None:
        assert _glimpse_label({"blob_name": "data/file.json"}, 0) == "data/file.json"


# ------------------------------------------------------------------
# Unit tests: parcel truncation
# ------------------------------------------------------------------


class TestTruncateParcel:
    def test_small_parcel_unchanged(self) -> None:
        fields = {"name": "test", "value": 42}
        assert _truncate_parcel(fields, 10_000) == fields

    def test_large_string_truncated(self) -> None:
        fields = {"content": "x" * 50_000, "name": "test"}
        result = _truncate_parcel(fields, 1_000)
        assert len(result["content"]) < 1_000
        assert "chars total" in result["content"]

    def test_large_list_truncated(self) -> None:
        fields = {"items": list(range(500)), "name": "test"}
        result = _truncate_parcel(fields, 1_000)
        assert len(result["items"]) <= 20
        assert "_items_truncated" in result


# ------------------------------------------------------------------
# Unit tests: DebugTracer
# ------------------------------------------------------------------


class TestDebugTracer:
    async def test_disabled_tracer_noop(self) -> None:
        tracer = DebugTracer(None, "run1", enabled=False)
        assert not tracer.enabled
        # These should not raise
        await tracer.record_order(["a", "b"])
        await tracer.record_block("a", "test", duration_ms=10, parcel_count=1)
        await tracer.record_parcels("a", [{"x": 1}])

    async def test_record_order(self) -> None:
        redis = FakeRedis()
        tracer = DebugTracer(redis, "run1", enabled=True)
        await tracer.record_order(["a", "b", "c"])
        stored = json.loads(redis.store["plumber:debug:run1:order"])
        assert stored == ["a", "b", "c"]

    async def test_record_block_summary(self) -> None:
        redis = FakeRedis()
        tracer = DebugTracer(redis, "run1", enabled=True)
        await tracer.record_block(
            "scan", "file_scan",
            duration_ms=42.5, parcel_count=3,
        )
        summary = json.loads(redis.store["plumber:debug:run1:scan"])
        assert summary["block_uid"] == "scan"
        assert summary["block_type"] == "file_scan"
        assert summary["duration_ms"] == 42.5
        assert summary["parcel_count"] == 3
        assert summary["status"] == "completed"

    async def test_record_block_failure(self) -> None:
        redis = FakeRedis()
        tracer = DebugTracer(redis, "run1", enabled=True)
        await tracer.record_block(
            "bad", "broken",
            duration_ms=5, parcel_count=0,
            status="failed", error="boom",
        )
        summary = json.loads(redis.store["plumber:debug:run1:bad"])
        assert summary["status"] == "failed"
        assert summary["error"] == "boom"

    async def test_record_parcels_glimpses(self) -> None:
        redis = FakeRedis()
        tracer = DebugTracer(redis, "run1", enabled=True)
        items = [
            {"filename": "a.xlsx"},
            {"filename": "b.csv"},
            {"filename": "c.pdf"},
        ]
        await tracer.record_parcels("scan", items)
        glimpses = json.loads(redis.store["plumber:debug:run1:scan:g"])
        assert len(glimpses) == 3
        assert glimpses[0]["label"] == "a.xlsx"
        assert glimpses[1]["label"] == "b.csv"
        assert glimpses[2]["label"] == "c.pdf"

    async def test_record_parcels_detail(self) -> None:
        redis = FakeRedis()
        tracer = DebugTracer(redis, "run1", enabled=True, max_parcels=2)
        items = [
            {"filename": "a.xlsx", "data": "AAA"},
            {"filename": "b.csv", "data": "BBB"},
            {"filename": "c.pdf", "data": "CCC"},
        ]
        await tracer.record_parcels("scan", items)
        # Only first 2 stored in detail
        assert "plumber:debug:run1:scan:p:0" in redis.store
        assert "plumber:debug:run1:scan:p:1" in redis.store
        assert "plumber:debug:run1:scan:p:2" not in redis.store

        detail = json.loads(redis.store["plumber:debug:run1:scan:p:0"])
        assert detail["filename"] == "a.xlsx"

    async def test_glimpses_capped(self) -> None:
        redis = FakeRedis()
        tracer = DebugTracer(redis, "run1", enabled=True, max_glimpses=3)
        items = [{"name": f"item-{i}"} for i in range(10)]
        await tracer.record_parcels("block", items)
        glimpses = json.loads(redis.store["plumber:debug:run1:block:g"])
        # 3 real + 1 "... and 7 more"
        assert len(glimpses) == 4
        assert glimpses[3]["label"] == "... and 7 more"


# ------------------------------------------------------------------
# Unit tests: reading debug data
# ------------------------------------------------------------------


class TestReadDebugTrace:
    async def test_get_full_trace(self) -> None:
        redis = FakeRedis()
        tracer = DebugTracer(redis, "run1", enabled=True)
        await tracer.record_order(["a", "b"])
        await tracer.record_block("a", "scan", duration_ms=10, parcel_count=2)
        await tracer.record_parcels("a", [
            {"filename": "x.csv"},
            {"filename": "y.csv"},
        ])
        await tracer.record_block("b", "process", duration_ms=5, parcel_count=1)
        await tracer.record_parcels("b", [{"result": "done"}])

        trace = await get_debug_trace(redis, "run1")
        assert trace["order"] == ["a", "b"]
        assert "a" in trace["blocks"]
        assert trace["blocks"]["a"]["parcel_count"] == 2
        assert len(trace["blocks"]["a"]["glimpses"]) == 2

    async def test_get_parcel_detail(self) -> None:
        redis = FakeRedis()
        tracer = DebugTracer(redis, "run1", enabled=True)
        await tracer.record_parcels("scan", [
            {"filename": "a.xlsx", "content": "data_a"},
            {"filename": "b.csv", "content": "data_b"},
        ])

        parcel = await get_debug_parcel(redis, "run1", "scan", 1)
        assert parcel is not None
        assert parcel["filename"] == "b.csv"

    async def test_get_missing_parcel(self) -> None:
        redis = FakeRedis()
        result = await get_debug_parcel(redis, "run1", "scan", 99)
        assert result is None

    async def test_get_empty_trace(self) -> None:
        redis = FakeRedis()
        result = await get_debug_trace(redis, "nonexistent")
        assert result == {}

    async def test_search_by_label(self) -> None:
        redis = FakeRedis()
        tracer = DebugTracer(redis, "run1", enabled=True)
        await tracer.record_parcels("scan", [
            {"filename": "report_q1.xlsx"},
            {"filename": "data_2024.csv"},
            {"filename": "report_q2.xlsx"},
        ])

        matches = await search_debug_parcels(
            redis, "run1", "scan", label_contains="report",
        )
        assert len(matches) == 2
        assert matches[0]["label"] == "report_q1.xlsx"
        assert matches[1]["label"] == "report_q2.xlsx"

    async def test_search_no_filter(self) -> None:
        redis = FakeRedis()
        tracer = DebugTracer(redis, "run1", enabled=True)
        await tracer.record_parcels("scan", [
            {"filename": "a.csv"},
            {"filename": "b.csv"},
        ])
        matches = await search_debug_parcels(redis, "run1", "scan")
        assert len(matches) == 2

    async def test_search_max_results(self) -> None:
        redis = FakeRedis()
        tracer = DebugTracer(redis, "run1", enabled=True)
        await tracer.record_parcels("scan", [{"name": f"item-{i}"} for i in range(50)])
        matches = await search_debug_parcels(
            redis, "run1", "scan", max_results=5,
        )
        assert len(matches) == 5


# ------------------------------------------------------------------
# Integration: debug trace through the executor
# ------------------------------------------------------------------


class TestDebugExecutor:
    async def test_linear_pipeline_traced(self) -> None:
        """A simple 2-block pipeline produces debug trace in Redis."""
        BlockRegistry.discover()
        redis = FakeRedis()
        tracer = DebugTracer(redis, "run1", enabled=True)

        pipeline = PipelineDefinition(
            id="p1", name="traced",
            blocks=[
                _block("a", "test_file_scan"),
                _block("b", "test_process"),
            ],
            pipes=[_pipe("p1", "a", "b")],
        )
        client = AsyncMongoMockClient()
        db = client["test_plumber"]
        run_oid = ObjectId()
        await db["runs"].insert_one({"_id": run_oid, "status": "running"})

        await run_blocks(pipeline, str(run_oid), db, "lem", tracer=tracer)

        # Execution order stored
        order = json.loads(redis.store["plumber:debug:run1:order"])
        assert order == ["a", "b"]

        # Block summaries stored
        a_summary = json.loads(redis.store["plumber:debug:run1:a"])
        assert a_summary["block_type"] == "test_file_scan"
        assert a_summary["parcel_count"] == 3  # 3 files fanned out

        b_summary = json.loads(redis.store["plumber:debug:run1:b"])
        assert b_summary["block_type"] == "test_process"
        assert b_summary["parcel_count"] == 3  # processed each file

        # Block A glimpses show filenames
        a_glimpses = json.loads(redis.store["plumber:debug:run1:a:g"])
        labels = [g["label"] for g in a_glimpses]
        assert "report.xlsx" in labels
        assert "data.csv" in labels
        assert "image.png" in labels

        # Block B glimpses show processed results
        b_glimpses = json.loads(redis.store["plumber:debug:run1:b:g"])
        assert len(b_glimpses) == 3

        # Full parcel detail available
        p0 = json.loads(redis.store["plumber:debug:run1:b:p:0"])
        assert p0["result"] == "processed:report.xlsx"

    async def test_disabled_tracer_no_redis_writes(self) -> None:
        """When tracer is disabled, no Redis keys are written."""
        BlockRegistry.discover()
        redis = FakeRedis()
        tracer = DebugTracer(redis, "run1", enabled=False)

        pipeline = PipelineDefinition(
            id="p1", name="no-trace",
            blocks=[_block("a", "test_file_scan")],
            pipes=[],
        )
        client = AsyncMongoMockClient()
        db = client["test_plumber"]
        run_oid = ObjectId()
        await db["runs"].insert_one({"_id": run_oid, "status": "running"})

        await run_blocks(pipeline, str(run_oid), db, "lem", tracer=tracer)
        assert len(redis.store) == 0

    async def test_trace_survives_block_failure(self) -> None:
        """Debug trace for earlier blocks survives later failure."""
        BlockRegistry.discover()
        redis = FakeRedis()
        tracer = DebugTracer(redis, "run1", enabled=True)

        pipeline = PipelineDefinition(
            id="p1", name="fail-trace",
            blocks=[
                _block("a", "test_file_scan"),
                _block("fail", "test_always_fail"),
            ],
            pipes=[_pipe("p1", "a", "fail")],
        )
        client = AsyncMongoMockClient()
        db = client["test_plumber"]
        run_oid = ObjectId()
        await db["runs"].insert_one({"_id": run_oid, "status": "running"})

        with pytest.raises(RuntimeError, match="intentional"):
            await run_blocks(pipeline, str(run_oid), db, "lem", tracer=tracer)

        # Block A trace was written before failure
        assert "plumber:debug:run1:a" in redis.store
        a_summary = json.loads(redis.store["plumber:debug:run1:a"])
        assert a_summary["status"] == "completed"

        # Failed block also recorded
        assert "plumber:debug:run1:fail" in redis.store
        fail_summary = json.loads(redis.store["plumber:debug:run1:fail"])
        assert fail_summary["status"] == "failed"
        assert "intentional" in fail_summary["error"]

    async def test_full_trace_retrieval(self) -> None:
        """get_debug_trace returns the complete picture."""
        BlockRegistry.discover()
        redis = FakeRedis()
        tracer = DebugTracer(redis, "run1", enabled=True)

        pipeline = PipelineDefinition(
            id="p1", name="full-trace",
            blocks=[
                _block("scan", "test_file_scan"),
                _block("proc", "test_process"),
            ],
            pipes=[_pipe("p1", "scan", "proc")],
        )
        client = AsyncMongoMockClient()
        db = client["test_plumber"]
        run_oid = ObjectId()
        await db["runs"].insert_one({"_id": run_oid, "status": "running"})
        await run_blocks(pipeline, str(run_oid), db, "lem", tracer=tracer)

        trace = await get_debug_trace(redis, "run1")
        assert trace["order"] == ["scan", "proc"]
        assert trace["blocks"]["scan"]["parcel_count"] == 3
        assert len(trace["blocks"]["scan"]["glimpses"]) == 3

        # Can select a specific file and see its processed result
        parcel = await get_debug_parcel(redis, "run1", "proc", 0)
        assert parcel is not None
        assert parcel["result"].startswith("processed:")

        # Can search by filename
        matches = await search_debug_parcels(
            redis, "run1", "scan", label_contains="data",
        )
        assert len(matches) == 1
        assert matches[0]["label"] == "data.csv"
        # Use the index to get full detail
        detail = await get_debug_parcel(redis, "run1", "scan", matches[0]["index"])
        assert detail is not None
        assert detail["filename"] == "data.csv"
