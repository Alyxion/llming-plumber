"""Practical graph execution tests — real blocks wired into real pipelines.

Tests fan-out, parallel execution, collection, field mapping,
diamond graphs, error propagation, and multi-step data processing.
"""

from __future__ import annotations

import asyncio
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
from llming_plumber.worker.executor import run_blocks

# ------------------------------------------------------------------
# Test helpers
# ------------------------------------------------------------------


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


async def _run(
    blocks: list[BlockDefinition],
    pipes: list[PipeDefinition],
) -> dict[str, Any]:
    """Build a pipeline, create a mock DB, and run it."""
    pipeline = PipelineDefinition(
        id="test-pipe", name="test", blocks=blocks, pipes=pipes,
    )
    client = AsyncMongoMockClient()
    db = client["test_plumber"]
    run_oid = ObjectId()
    await db["runs"].insert_one({"_id": run_oid, "status": "running"})
    return await run_blocks(pipeline, str(run_oid), db, "test-lemming")


# ------------------------------------------------------------------
# Custom test blocks (lightweight, no external deps)
# ------------------------------------------------------------------


class _UpperInput(BlockInput):
    text: str = ""


class _UpperOutput(BlockOutput):
    text: str


class UpperBlock(BaseBlock[_UpperInput, _UpperOutput]):
    """Uppercases text — used to verify data flows through fan-out."""

    block_type: ClassVar[str] = "_test_upper"

    async def execute(
        self, input: _UpperInput, ctx: BlockContext | None = None
    ) -> _UpperOutput:
        return _UpperOutput(text=input.text.upper())


class _DelayInput(BlockInput):
    text: str = ""
    delay: float = 0.0


class _DelayOutput(BlockOutput):
    text: str
    task_name: str


class DelayBlock(BaseBlock[_DelayInput, _DelayOutput]):
    """Sleeps briefly, used to verify parallel execution."""

    block_type: ClassVar[str] = "_test_delay"

    async def execute(
        self, input: _DelayInput, ctx: BlockContext | None = None
    ) -> _DelayOutput:
        await asyncio.sleep(input.delay)
        task = asyncio.current_task()
        return _DelayOutput(
            text=input.text,
            task_name=task.get_name() if task else "unknown",
        )


class _FailIfInput(BlockInput):
    text: str = ""
    fail_on: str = ""


class _FailIfOutput(BlockOutput):
    text: str


class FailIfBlock(BaseBlock[_FailIfInput, _FailIfOutput]):
    """Raises when text matches fail_on — used for error tests."""

    block_type: ClassVar[str] = "_test_fail_if"

    async def execute(
        self, input: _FailIfInput, ctx: BlockContext | None = None
    ) -> _FailIfOutput:
        if input.text == input.fail_on:
            msg = f"Intentional failure on '{input.text}'"
            raise RuntimeError(msg)
        return _FailIfOutput(text=input.text)


class _DoubleInput(BlockInput):
    value: int = 0


class _DoubleOutput(BlockOutput):
    value: int
    doubled: int


class DoubleBlock(BaseBlock[_DoubleInput, _DoubleOutput]):
    block_type: ClassVar[str] = "_test_double"

    async def execute(
        self, input: _DoubleInput, ctx: BlockContext | None = None
    ) -> _DoubleOutput:
        return _DoubleOutput(value=input.value, doubled=input.value * 2)


class _ConcatInput(BlockInput):
    a: str = ""
    b: str = ""


class _ConcatOutput(BlockOutput):
    result: str


class ConcatBlock(BaseBlock[_ConcatInput, _ConcatOutput]):
    block_type: ClassVar[str] = "_test_concat"

    async def execute(
        self, input: _ConcatInput, ctx: BlockContext | None = None
    ) -> _ConcatOutput:
        return _ConcatOutput(result=f"{input.a}{input.b}")


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _register_blocks() -> None:
    """Make sure all test blocks + real blocks are registered."""
    BlockRegistry.reset()
    BlockRegistry.discover()
    # Register test-only blocks
    for cls in (
        UpperBlock, DelayBlock, FailIfBlock, DoubleBlock, ConcatBlock,
    ):
        BlockRegistry._registry[cls.block_type] = cls  # type: ignore[assignment]


# ------------------------------------------------------------------
# 1. Linear pipelines with real blocks
# ------------------------------------------------------------------


class TestLinearPipelines:
    async def test_csv_parse_then_filter(self) -> None:
        """CSV text → CsvParser → Filter → only matching rows."""
        result = await _run(
            blocks=[
                _block(
                    "csv", "csv_parser",
                    csv_text="name,score\nalice,90\nbob,40\ncarol,75",
                ),
                _block("filter", "filter", field="score", operator="gte", value="70"),
            ],
            pipes=[
                _pipe("p1", "csv", "filter", field_mapping={"items": "rows"}),
            ],
        )
        assert result["filtered_count"] == 2
        names = {r["name"] for r in result["items"]}
        assert names == {"alice", "carol"}

    async def test_split_text_chunks(self) -> None:
        """Split multi-line text, verify chunk output."""
        result = await _run(
            blocks=[
                _block(
                    "splitter", "split_text",
                    text="line1\nline2\nline3",
                    delimiter="\n",
                ),
            ],
            pipes=[],
        )
        assert result["chunk_count"] == 3
        assert result["chunks"] == ["line1", "line2", "line3"]

    async def test_three_block_chain(self) -> None:
        """text_template → hash_generator → verify hash output."""
        result = await _run(
            blocks=[
                _block(
                    "tmpl", "text_template",
                    template="Hello {name}!",
                    values={"name": "World"},
                ),
                _block("hash", "hash_generator", algorithm="sha256"),
            ],
            pipes=[
                _pipe(
                    "p1", "tmpl", "hash",
                    field_mapping={"text": "rendered"},
                ),
            ],
        )
        assert len(result["hash_hex"]) == 64
        assert result["algorithm"] == "sha256"

    async def test_hash_then_upper(self) -> None:
        """Hash text, then uppercase the hex digest."""
        result = await _run(
            blocks=[
                _block("hash", "hash_generator", text="hello", algorithm="md5"),
                _block("up", "_test_upper"),
            ],
            pipes=[
                _pipe("p1", "hash", "up", field_mapping={"text": "hash_hex"}),
            ],
        )
        assert result["text"] == result["text"].upper()
        assert len(result["text"]) == 32


# ------------------------------------------------------------------
# 2. Diamond / fan-in graphs
# ------------------------------------------------------------------


class TestDiamondGraphs:
    async def test_two_branches_merge_into_one(self) -> None:
        """
        producer_a ──┐
                     ├──► concat
        producer_b ──┘
        """
        result = await _run(
            blocks=[
                _block("a", "_test_concat", a="Hello", b=" "),
                _block("b", "_test_concat", a="World", b="!"),
                _block("join", "_test_concat"),
            ],
            pipes=[
                _pipe("p1", "a", "join", field_mapping={"a": "result"}),
                _pipe("p2", "b", "join", field_mapping={"b": "result"}),
            ],
        )
        assert result["result"] == "Hello World!"

    async def test_shared_source_diamond(self) -> None:
        """
        template ──┬──► hash_sha
                   └──► hash_md5
        """
        # Run both hashes from same template
        pipeline = PipelineDefinition(
            id="diamond",
            name="diamond",
            blocks=[
                _block(
                    "tmpl", "text_template",
                    template="test-{v}", values={"v": "123"},
                ),
                _block("sha", "hash_generator", algorithm="sha256"),
                _block("md5", "hash_generator", algorithm="md5"),
            ],
            pipes=[
                _pipe("p1", "tmpl", "sha", field_mapping={"text": "rendered"}),
                _pipe("p2", "tmpl", "md5", field_mapping={"text": "rendered"}),
            ],
        )
        client = AsyncMongoMockClient()
        db = client["test_plumber"]
        run_oid = ObjectId()
        await db["runs"].insert_one({"_id": run_oid, "status": "running"})
        await run_blocks(pipeline, str(run_oid), db, "lem")

        # Check both block states were written
        run_doc = await db["runs"].find_one({"_id": run_oid})
        sha_out = run_doc["block_states"]["sha"]["output"]
        md5_out = run_doc["block_states"]["md5"]["output"]
        assert sha_out["algorithm"] == "sha256"
        assert md5_out["algorithm"] == "md5"
        assert len(sha_out["hash_hex"]) == 64
        assert len(md5_out["hash_hex"]) == 32


# ------------------------------------------------------------------
# 3. Fan-out + collect (iteration)
# ------------------------------------------------------------------


class TestFanOutCollect:
    async def test_split_uppercase_collect(self) -> None:
        """
        split([{text: "a"}, {text: "b"}, {text: "c"}])
          → upper (runs 3×)
          → collect
        Result: [{text: "A"}, {text: "B"}, {text: "C"}]
        """
        result = await _run(
            blocks=[
                _block(
                    "src", "split",
                    items=[
                        {"text": "hello"},
                        {"text": "world"},
                        {"text": "foo"},
                    ],
                ),
                _block("up", "_test_upper"),
                _block("coll", "collect"),
            ],
            pipes=[
                _pipe("p1", "src", "up"),
                _pipe("p2", "up", "coll"),
            ],
        )
        assert result["count"] == 3
        texts = sorted(item["text"] for item in result["items"])
        assert texts == ["FOO", "HELLO", "WORLD"]

    async def test_fan_out_with_field_mapping(self) -> None:
        """Fan-out items have 'name' field, downstream expects 'text'."""
        result = await _run(
            blocks=[
                _block(
                    "src", "split",
                    items=[{"name": "alice"}, {"name": "bob"}],
                ),
                _block("up", "_test_upper"),
                _block("coll", "collect"),
            ],
            pipes=[
                _pipe("p1", "src", "up", field_mapping={"text": "name"}),
                _pipe("p2", "up", "coll"),
            ],
        )
        assert result["count"] == 2
        texts = sorted(item["text"] for item in result["items"])
        assert texts == ["ALICE", "BOB"]

    async def test_fan_out_preserves_order(self) -> None:
        """Items come back in the same order they were split."""
        items = [{"text": f"item-{i}"} for i in range(10)]
        result = await _run(
            blocks=[
                _block("src", "split", items=items),
                _block("up", "_test_upper"),
                _block("coll", "collect"),
            ],
            pipes=[
                _pipe("p1", "src", "up"),
                _pipe("p2", "up", "coll"),
            ],
        )
        assert result["count"] == 10
        for i, item in enumerate(result["items"]):
            assert item["text"] == f"ITEM-{i}"

    async def test_fan_out_empty_list(self) -> None:
        """Splitting zero items: no fan-out occurs, collect gets one parcel."""
        result = await _run(
            blocks=[
                _block("src", "split", items=[]),
                _block("coll", "collect"),
            ],
            pipes=[
                _pipe("p1", "src", "coll"),
            ],
        )
        # Empty list → no fan-out, split output passes through as one parcel
        assert result["count"] == 1
        assert result["items"][0]["total"] == 0

    async def test_fan_out_single_item(self) -> None:
        """Fan-out with one item should work normally."""
        result = await _run(
            blocks=[
                _block("src", "split", items=[{"text": "only"}]),
                _block("up", "_test_upper"),
                _block("coll", "collect"),
            ],
            pipes=[
                _pipe("p1", "src", "up"),
                _pipe("p2", "up", "coll"),
            ],
        )
        assert result["count"] == 1
        assert result["items"][0]["text"] == "ONLY"

    async def test_multi_step_fan_out(self) -> None:
        """
        split → upper → hash → collect
        Three blocks in the fan-out branch.
        """
        result = await _run(
            blocks=[
                _block(
                    "src", "split",
                    items=[{"text": "a"}, {"text": "b"}],
                ),
                _block("up", "_test_upper"),
                _block("hash", "hash_generator", algorithm="md5"),
                _block("coll", "collect"),
            ],
            pipes=[
                _pipe("p1", "src", "up"),
                _pipe("p2", "up", "hash"),
                _pipe("p3", "hash", "coll"),
            ],
        )
        assert result["count"] == 2
        for item in result["items"]:
            assert len(item["hash_hex"]) == 32
            assert item["algorithm"] == "md5"

    async def test_fan_out_with_static_config(self) -> None:
        """Downstream block receives both fan-out data and static config."""
        result = await _run(
            blocks=[
                _block(
                    "src", "split",
                    items=[{"text": "test1"}, {"text": "test2"}],
                ),
                _block("hash", "hash_generator", algorithm="sha1"),
                _block("coll", "collect"),
            ],
            pipes=[
                _pipe("p1", "src", "hash"),
                _pipe("p2", "hash", "coll"),
            ],
        )
        assert result["count"] == 2
        for item in result["items"]:
            assert item["algorithm"] == "sha1"
            assert len(item["hash_hex"]) == 40  # SHA-1 = 40 hex chars


# ------------------------------------------------------------------
# 4. Parallel execution
# ------------------------------------------------------------------


class TestParallelExecution:
    async def test_fan_out_runs_concurrently(self) -> None:
        """Verify fan-out items run in parallel (not sequentially).

        5 items each sleeping 0.1s. Sequential = 0.5s, parallel < 0.3s.
        """
        items = [{"text": f"t{i}", "delay": 0.1} for i in range(5)]
        start = asyncio.get_event_loop().time()
        result = await _run(
            blocks=[
                _block("src", "split", items=items),
                _block("slow", "_test_delay"),
                _block("coll", "collect"),
            ],
            pipes=[
                _pipe("p1", "src", "slow"),
                _pipe("p2", "slow", "coll"),
            ],
        )
        elapsed = asyncio.get_event_loop().time() - start
        assert result["count"] == 5
        # Should be well under 0.5s if parallel
        assert elapsed < 0.4, f"Took {elapsed:.2f}s — not running in parallel?"

    async def test_concurrency_limit(self) -> None:
        """With _max_concurrency=2, at most 2 items run at once."""
        active: list[int] = [0]
        max_active: list[int] = [0]
        original_execute = DelayBlock.execute

        async def tracked_execute(
            self: Any, input: Any, ctx: Any = None
        ) -> Any:
            active[0] += 1
            max_active[0] = max(max_active[0], active[0])
            try:
                return await original_execute(self, input, ctx)
            finally:
                active[0] -= 1

        items = [{"text": f"t{i}", "delay": 0.05} for i in range(6)]
        with patch.object(DelayBlock, "execute", tracked_execute):
            result = await _run(
                blocks=[
                    _block("src", "split", items=items),
                    _block("slow", "_test_delay", _max_concurrency=2),
                    _block("coll", "collect"),
                ],
                pipes=[
                    _pipe("p1", "src", "slow"),
                    _pipe("p2", "slow", "coll"),
                ],
            )
        assert result["count"] == 6
        assert max_active[0] <= 2, f"Max concurrency was {max_active[0]}"


# ------------------------------------------------------------------
# 5. Error handling in fan-out
# ------------------------------------------------------------------


class TestFanOutErrors:
    async def test_error_in_fan_out_propagates(self) -> None:
        """If one item fails, the whole pipeline fails."""
        with pytest.raises(RuntimeError, match="Intentional failure"):
            await _run(
                blocks=[
                    _block(
                        "src", "split",
                        items=[
                            {"text": "ok1"},
                            {"text": "BOOM"},
                            {"text": "ok2"},
                        ],
                    ),
                    _block("check", "_test_fail_if", fail_on="BOOM"),
                    _block("coll", "collect"),
                ],
                pipes=[
                    _pipe("p1", "src", "check"),
                    _pipe("p2", "check", "coll"),
                ],
            )

    async def test_error_records_block_failure(self) -> None:
        """Failed fan-out block writes failure state to DB."""
        pipeline = PipelineDefinition(
            id="err", name="err",
            blocks=[
                _block(
                    "src", "split",
                    items=[{"text": "ok"}, {"text": "BAD"}],
                ),
                _block("check", "_test_fail_if", fail_on="BAD"),
                _block("coll", "collect"),
            ],
            pipes=[
                _pipe("p1", "src", "check"),
                _pipe("p2", "check", "coll"),
            ],
        )
        client = AsyncMongoMockClient()
        db = client["test_plumber"]
        run_oid = ObjectId()
        await db["runs"].insert_one({"_id": run_oid, "status": "running"})

        with pytest.raises(RuntimeError):
            await run_blocks(pipeline, str(run_oid), db, "lem")

        run_doc = await db["runs"].find_one({"_id": run_oid})
        assert run_doc["block_states"]["check"]["status"] == "failed"
        assert "Intentional failure" in run_doc["block_states"]["check"]["error"]


# ------------------------------------------------------------------
# 6. Field mapping scenarios
# ------------------------------------------------------------------


class TestFieldMapping:
    async def test_selective_field_mapping(self) -> None:
        """Only mapped fields pass through the pipe."""
        result = await _run(
            blocks=[
                _block(
                    "tmpl", "text_template",
                    template="{x}", values={"x": "data"},
                ),
                _block("hash", "hash_generator", algorithm="md5"),
            ],
            pipes=[
                # Only pass "rendered" as "text", drop everything else
                _pipe("p1", "tmpl", "hash", field_mapping={"text": "rendered"}),
            ],
        )
        assert result["algorithm"] == "md5"
        assert len(result["hash_hex"]) == 32

    async def test_config_plus_piped_fields(self) -> None:
        """Block config provides defaults, piped fields override."""
        result = await _run(
            blocks=[
                _block(
                    "tmpl", "text_template",
                    template="v={val}", values={"val": "42"},
                ),
                # hash has algorithm="sha256" in config
                _block("hash", "hash_generator", algorithm="sha256"),
            ],
            pipes=[
                _pipe("p1", "tmpl", "hash", field_mapping={"text": "rendered"}),
            ],
        )
        assert result["algorithm"] == "sha256"

    async def test_multiple_upstream_merge(self) -> None:
        """Two upstream blocks feed different fields into one block."""
        result = await _run(
            blocks=[
                _block("a", "_test_concat", a="prefix-", b=""),
                _block("b", "_test_concat", a="", b="-suffix"),
                _block("final", "_test_concat"),
            ],
            pipes=[
                _pipe("p1", "a", "final", field_mapping={"a": "result"}),
                _pipe("p2", "b", "final", field_mapping={"b": "result"}),
            ],
        )
        assert result["result"] == "prefix--suffix"


# ------------------------------------------------------------------
# 7. Real-world-ish pipeline scenarios
# ------------------------------------------------------------------


class TestRealWorldPipelines:
    async def test_csv_to_split_to_hash_to_collect(self) -> None:
        """
        CSV → parse → split (fan-out each row) → hash name → collect

        Simulates: spreadsheet of data → process each row → gather results.
        """
        csv_text = "name,city\nalice,berlin\nbob,paris\ncarol,tokyo"
        result = await _run(
            blocks=[
                _block("csv", "csv_parser", csv_text=csv_text),
                _block("split", "split"),
                _block("hash", "hash_generator", algorithm="md5"),
                _block("coll", "collect"),
            ],
            pipes=[
                _pipe("p1", "csv", "split", field_mapping={"items": "rows"}),
                _pipe("p2", "split", "hash", field_mapping={"text": "name"}),
                _pipe("p3", "hash", "coll"),
            ],
        )
        assert result["count"] == 3
        # Each item should have a hash
        for item in result["items"]:
            assert len(item["hash_hex"]) == 32

    async def test_fan_out_concat_per_row(self) -> None:
        """
        Split rows → concat per row → collect results.

        Simulates: per-row processing with fan-out.
        """
        result = await _run(
            blocks=[
                _block(
                    "split", "split",
                    items=[
                        {"a": "Hello ", "b": "Alice"},
                        {"a": "Hi ", "b": "Bob"},
                    ],
                ),
                _block("cat", "_test_concat"),
                _block("coll", "collect"),
            ],
            pipes=[
                _pipe("p1", "split", "cat"),
                _pipe("p2", "cat", "coll"),
            ],
        )
        assert result["count"] == 2
        results = sorted(item["result"] for item in result["items"])
        assert results == ["Hello Alice", "Hi Bob"]

    async def test_fan_out_hash_per_name(self) -> None:
        """Fan-out names from CSV rows, hash each, collect results."""
        result = await _run(
            blocks=[
                _block(
                    "split", "split",
                    items=[
                        {"text": "alice"},
                        {"text": "bob"},
                        {"text": "carol"},
                    ],
                ),
                _block("hash", "hash_generator", algorithm="sha256"),
                _block("coll", "collect"),
            ],
            pipes=[
                _pipe("p1", "split", "hash"),
                _pipe("p2", "hash", "coll"),
            ],
        )
        assert result["count"] == 3
        hashes = {item["hash_hex"] for item in result["items"]}
        assert len(hashes) == 3  # all different

    async def test_dedup_then_aggregate(self) -> None:
        """Deduplicate items then compute aggregate statistics."""
        result = await _run(
            blocks=[
                _block(
                    "dedup", "deduplicator",
                    items=[
                        {"id": "1", "score": 10},
                        {"id": "2", "score": 20},
                        {"id": "1", "score": 10},  # duplicate
                        {"id": "3", "score": 30},
                    ],
                    field="id",
                ),
                _block("agg", "aggregate", field="score", operation="sum"),
            ],
            pipes=[
                _pipe("p1", "dedup", "agg"),
            ],
        )
        assert result["result"] == 60.0  # 10 + 20 + 30
        assert result["operation"] == "sum"

    async def test_csv_filter_sort_aggregate(self) -> None:
        """
        CSV → Filter (score >= 50) → Sort → Aggregate (avg)

        Full 4-block pipeline with no fan-out.
        """
        csv = "student,score\nalice,90\nbob,40\ncarol,75\ndan,30\neve,85"
        result = await _run(
            blocks=[
                _block("csv", "csv_parser", csv_text=csv),
                _block(
                    "filter", "filter",
                    field="score", operator="gte", value="50",
                ),
                _block("agg", "aggregate", field="score", operation="avg"),
            ],
            pipes=[
                _pipe("p1", "csv", "filter", field_mapping={"items": "rows"}),
                _pipe("p2", "filter", "agg"),
            ],
        )
        # alice=90, carol=75, eve=85 → avg = (90+75+85)/3 ≈ 83.33
        assert abs(result["result"] - 83.33) < 0.1

    async def test_fan_out_regex_per_line(self) -> None:
        """
        Split pre-built items → RegexExtractor per item → Collect.

        Simulates: extract entities from each paragraph independently.
        """
        result = await _run(
            blocks=[
                _block(
                    "split", "split",
                    items=[
                        {"text": "Call 555-1234 today"},
                        {"text": "Or email info@x.com"},
                        {"text": "Dial 555-5678 now"},
                    ],
                ),
                _block(
                    "regex", "regex_extractor",
                    pattern=r"(?P<phone>\d{3}-\d{4})",
                ),
                _block("coll", "collect"),
            ],
            pipes=[
                _pipe("p1", "split", "regex"),
                _pipe("p2", "regex", "coll"),
            ],
        )
        assert result["count"] == 3
        phone_matches = sum(
            item["match_count"] for item in result["items"]
        )
        assert phone_matches == 2  # first and third items have phone numbers


# ------------------------------------------------------------------
# 8. Edge cases
# ------------------------------------------------------------------


class TestEdgeCases:
    async def test_single_block_no_pipes(self) -> None:
        """A pipeline with one block and no pipes."""
        result = await _run(
            blocks=[_block("hash", "hash_generator", text="hello", algorithm="md5")],
            pipes=[],
        )
        assert len(result["hash_hex"]) == 32

    async def test_block_state_tracking(self) -> None:
        """Verify block_states are written for every block in the run doc."""
        pipeline = PipelineDefinition(
            id="track", name="track",
            blocks=[
                _block("a", "_test_concat", a="x", b="y"),
                _block("b", "_test_upper"),
            ],
            pipes=[_pipe("p1", "a", "b", field_mapping={"text": "result"})],
        )
        client = AsyncMongoMockClient()
        db = client["test_plumber"]
        run_oid = ObjectId()
        await db["runs"].insert_one({"_id": run_oid, "status": "running"})
        await run_blocks(pipeline, str(run_oid), db, "lem")

        run_doc = await db["runs"].find_one({"_id": run_oid})
        assert run_doc["block_states"]["a"]["status"] == "completed"
        assert run_doc["block_states"]["b"]["status"] == "completed"
        assert run_doc["block_states"]["b"]["output"]["text"] == "XY"

    async def test_run_logs_written(self) -> None:
        """Every block execution produces a RunLog entry."""
        pipeline = PipelineDefinition(
            id="logs", name="logs",
            blocks=[
                _block("a", "_test_upper", text="hello"),
                _block("b", "_test_upper"),
            ],
            pipes=[_pipe("p1", "a", "b")],
        )
        client = AsyncMongoMockClient()
        db = client["test_plumber"]
        run_oid = ObjectId()
        await db["runs"].insert_one({"_id": run_oid, "status": "running"})
        await run_blocks(pipeline, str(run_oid), db, "lem")

        logs = await db["run_logs"].find({}).to_list(length=100)
        assert len(logs) == 2
        assert logs[0]["block_id"] == "a"
        assert logs[1]["block_id"] == "b"
        assert all(log["level"] == "info" for log in logs)

    async def test_fan_out_with_no_downstream(self) -> None:
        """A split block at the end of a pipeline (no collect).

        The split output has individual parcels but ``last_output``
        uses the first parcel's fields.
        """
        result = await _run(
            blocks=[
                _block(
                    "src", "split",
                    items=[{"x": 1}, {"x": 2}],
                ),
            ],
            pipes=[],
        )
        # first parcel is the first item
        assert result["x"] == 1

    async def test_collect_without_fan_out(self) -> None:
        """Collect from a single upstream (no fan-out) gets one item."""
        result = await _run(
            blocks=[
                _block("a", "_test_upper", text="hello"),
                _block("coll", "collect"),
            ],
            pipes=[_pipe("p1", "a", "coll")],
        )
        assert result["count"] == 1
        assert result["items"][0]["text"] == "HELLO"
