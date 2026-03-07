"""Tests for resource limit enforcement across blocks and executor.

Verifies that every risky entry point (file readers, list processors,
fan-out, base64, document builders) rejects oversized input.
"""

from __future__ import annotations

import base64
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
from llming_plumber.blocks.limits import (
    MAX_BASE64_INPUT_BYTES,
    MAX_FAN_OUT_ITEMS,
    MAX_FILE_BYTES,
    MAX_LIST_ITEMS,
    MAX_PAGES,
    MAX_RECORDS,
    MAX_SLIDES,
    ResourceLimitError,
    check_base64_size,
    check_file_size,
    check_list_size,
    check_page_count,
    estimate_decoded_size,
)
from llming_plumber.blocks.registry import BlockRegistry
from llming_plumber.models.pipeline import (
    BlockDefinition,
    PipeDefinition,
    PipelineDefinition,
)
from llming_plumber.worker.executor import run_blocks

# ------------------------------------------------------------------
# Unit tests for limits module helpers
# ------------------------------------------------------------------


class TestLimitsHelpers:
    def test_check_file_size_ok(self) -> None:
        check_file_size(1024, label="test")

    def test_check_file_size_rejects(self) -> None:
        with pytest.raises(ResourceLimitError, match="exceeds"):
            check_file_size(MAX_FILE_BYTES + 1, label="test")

    def test_check_base64_size_ok(self) -> None:
        check_base64_size("a" * 100, label="test")

    def test_check_base64_size_rejects(self) -> None:
        with pytest.raises(ResourceLimitError, match="exceeds"):
            check_base64_size("a" * (MAX_BASE64_INPUT_BYTES + 1), label="test")

    def test_check_list_size_ok(self) -> None:
        check_list_size([1, 2, 3], label="test")

    def test_check_list_size_rejects(self) -> None:
        with pytest.raises(ResourceLimitError, match="exceeds"):
            check_list_size(MAX_LIST_ITEMS + 1, label="test")

    def test_check_list_size_with_custom_limit(self) -> None:
        check_list_size([1, 2], limit=5, label="test")
        with pytest.raises(ResourceLimitError):
            check_list_size([1, 2, 3], limit=2, label="test")

    def test_check_page_count_ok(self) -> None:
        check_page_count(10, label="test")

    def test_check_page_count_rejects(self) -> None:
        with pytest.raises(ResourceLimitError, match="exceeds"):
            check_page_count(MAX_PAGES + 1, label="test")

    def test_estimate_decoded_size(self) -> None:
        original = b"Hello, World!"
        encoded = base64.b64encode(original).decode()
        estimated = estimate_decoded_size(encoded)
        assert estimated == len(original)

    def test_resource_limit_error_is_value_error(self) -> None:
        """ResourceLimitError is a ValueError so existing except blocks catch it."""
        assert issubclass(ResourceLimitError, ValueError)

    def test_env_override(self) -> None:
        """Limits can be overridden via PLUMBER_ environment variables."""
        with patch.dict("os.environ", {"PLUMBER_MAX_FILE_BYTES": "999"}):
            from llming_plumber.blocks.limits import _env_int

            assert _env_int("MAX_FILE_BYTES", 50 * 1024 * 1024) == 999


# ------------------------------------------------------------------
# List-processing blocks enforce limits
# ------------------------------------------------------------------


class TestListBlockLimits:
    """Verify that blocks with list inputs reject oversized lists."""

    @pytest.fixture(autouse=True)
    def _ensure_discovery(self) -> None:
        BlockRegistry.reset()
        BlockRegistry.discover()

    async def test_filter_rejects_oversized_list(self) -> None:
        from llming_plumber.blocks.core.filter_block import FilterBlock, FilterInput

        block = FilterBlock()
        big_list = [{"x": i} for i in range(MAX_LIST_ITEMS + 1)]
        with pytest.raises(ResourceLimitError):
            await block.execute(FilterInput(
                items=big_list, field="x", operator="eq", value="1",
            ))

    async def test_sort_rejects_oversized_list(self) -> None:
        from llming_plumber.blocks.core.sort_block import SortBlock, SortInput

        block = SortBlock()
        with pytest.raises(ResourceLimitError):
            await block.execute(SortInput(
                items=[{"x": 1}] * (MAX_LIST_ITEMS + 1), field="x",
            ))

    async def test_dedup_rejects_oversized_list(self) -> None:
        from llming_plumber.blocks.core.deduplicator import (
            DeduplicatorBlock,
            DeduplicatorInput,
        )

        block = DeduplicatorBlock()
        with pytest.raises(ResourceLimitError):
            await block.execute(DeduplicatorInput(
                items=[{"id": 1}] * (MAX_LIST_ITEMS + 1), field="id",
            ))

    async def test_aggregate_rejects_oversized_list(self) -> None:
        from llming_plumber.blocks.core.aggregate import (
            AggregateBlock,
            AggregateInput,
        )

        block = AggregateBlock()
        with pytest.raises(ResourceLimitError):
            await block.execute(AggregateInput(
                items=[{"v": 1}] * (MAX_LIST_ITEMS + 1),
                field="v",
                operation="sum",
            ))

    async def test_merge_rejects_oversized_total(self) -> None:
        from llming_plumber.blocks.core.merge import MergeBlock, MergeInput

        block = MergeBlock()
        half = MAX_LIST_ITEMS // 2 + 1
        with pytest.raises(ResourceLimitError):
            await block.execute(MergeInput(
                item_lists=[[{"x": 1}] * half, [{"x": 2}] * half],
            ))

    async def test_column_mapper_rejects_oversized_list(self) -> None:
        from llming_plumber.blocks.core.column_mapper import (
            ColumnMapperBlock,
            ColumnMapperInput,
        )

        block = ColumnMapperBlock()
        with pytest.raises(ResourceLimitError):
            await block.execute(ColumnMapperInput(
                records=[{"a": 1}] * (MAX_LIST_ITEMS + 1),
                mapping={"a": "b"},
            ))

    async def test_csv_parser_rejects_oversized(self) -> None:
        from llming_plumber.blocks.core.csv_parser import (
            CsvParserBlock,
            CsvParserInput,
        )

        block = CsvParserBlock()
        # Generate CSV with too many rows
        rows = "x\n" + "\n".join(str(i) for i in range(MAX_RECORDS + 1))
        with pytest.raises(ResourceLimitError):
            await block.execute(CsvParserInput(csv_text=rows))


# ------------------------------------------------------------------
# Fan-out limits
# ------------------------------------------------------------------


class TestFanOutLimits:
    @pytest.fixture(autouse=True)
    def _ensure_discovery(self) -> None:
        BlockRegistry.reset()
        BlockRegistry.discover()

    async def test_split_rejects_oversized_items(self) -> None:
        from llming_plumber.blocks.core.split import SplitBlock, SplitInput

        block = SplitBlock()
        with pytest.raises(ResourceLimitError):
            await block.execute(SplitInput(
                items=[{"x": i} for i in range(MAX_FAN_OUT_ITEMS + 1)],
            ))

    async def test_executor_rejects_fan_out_too_large(self) -> None:
        """Even if split somehow passes, executor catches it."""

        class _BigSplitInput(BlockInput):
            items: list[dict[str, Any]] = []

        class _BigSplitOutput(BlockOutput):
            items: list[dict[str, Any]]

        class BigSplitBlock(BaseBlock[_BigSplitInput, _BigSplitOutput]):
            block_type: ClassVar[str] = "_test_big_split"
            fan_out_field: ClassVar[str | None] = "items"

            async def execute(
                self, input: _BigSplitInput, ctx: BlockContext | None = None
            ) -> _BigSplitOutput:
                # Bypass block-level check to test executor guard
                return _BigSplitOutput(items=input.items)

        class _NoopInput(BlockInput):
            x: int = 0

        class _NoopOutput(BlockOutput):
            x: int

        class NoopBlock(BaseBlock[_NoopInput, _NoopOutput]):
            block_type: ClassVar[str] = "_test_noop"

            async def execute(
                self, input: _NoopInput, ctx: BlockContext | None = None
            ) -> _NoopOutput:
                return _NoopOutput(x=input.x)

        BlockRegistry._registry["_test_big_split"] = BigSplitBlock  # type: ignore[assignment]
        BlockRegistry._registry["_test_noop"] = NoopBlock  # type: ignore[assignment]

        items = [{"x": i} for i in range(MAX_FAN_OUT_ITEMS + 1)]
        pipeline = PipelineDefinition(
            id="test", name="test",
            blocks=[
                BlockDefinition(
                    uid="split", block_type="_test_big_split",
                    label="Split", config={"items": items},
                ),
                BlockDefinition(
                    uid="noop", block_type="_test_noop", label="Noop",
                ),
            ],
            pipes=[
                PipeDefinition(
                    uid="p1",
                    source_block_uid="split",
                    source_fitting_uid="out",
                    target_block_uid="noop",
                    target_fitting_uid="in",
                ),
            ],
        )
        client = AsyncMongoMockClient()
        db = client["test"]
        run_oid = ObjectId()
        await db["runs"].insert_one({"_id": run_oid, "status": "running"})

        with pytest.raises(ResourceLimitError, match="Fan-out"):
            await run_blocks(pipeline, str(run_oid), db, "lem")


# ------------------------------------------------------------------
# Base64 size checks
# ------------------------------------------------------------------


class TestBase64Limits:
    async def test_base64_codec_encode_rejects_oversized(self) -> None:
        from llming_plumber.blocks.core.base64_codec import (
            Base64CodecBlock,
            Base64CodecInput,
        )

        block = Base64CodecBlock()
        with pytest.raises(ResourceLimitError):
            await block.execute(Base64CodecInput(
                text="x" * (MAX_FILE_BYTES + 1), mode="encode",
            ))

    async def test_base64_codec_decode_rejects_oversized(self) -> None:
        from llming_plumber.blocks.core.base64_codec import (
            Base64CodecBlock,
            Base64CodecInput,
        )

        block = Base64CodecBlock()
        with pytest.raises(ResourceLimitError):
            await block.execute(Base64CodecInput(
                text="A" * (MAX_BASE64_INPUT_BYTES + 1), mode="decode",
            ))


# ------------------------------------------------------------------
# File reader size checks (mock-based: don't need real files)
# ------------------------------------------------------------------


class TestFileReaderLimits:
    """Verify that file readers reject oversized base64 content."""

    async def test_excel_reader_rejects_oversized_base64(self) -> None:
        from llming_plumber.blocks.documents.excel_reader import (
            ExcelReaderBlock,
            ExcelReaderInput,
        )

        block = ExcelReaderBlock()
        with pytest.raises(ResourceLimitError, match="exceeds"):
            await block.execute(ExcelReaderInput(
                content="A" * (MAX_BASE64_INPUT_BYTES + 1),
            ))

    async def test_pdf_reader_rejects_oversized_base64(self) -> None:
        from llming_plumber.blocks.documents.pdf_reader import (
            PdfReaderBlock,
            PdfReaderInput,
        )

        block = PdfReaderBlock()
        with pytest.raises(ResourceLimitError, match="exceeds"):
            await block.execute(PdfReaderInput(
                content="A" * (MAX_BASE64_INPUT_BYTES + 1),
            ))

    async def test_word_reader_rejects_oversized_base64(self) -> None:
        from llming_plumber.blocks.documents.word_reader import (
            WordReaderBlock,
            WordReaderInput,
        )

        block = WordReaderBlock()
        with pytest.raises(ResourceLimitError, match="exceeds"):
            await block.execute(WordReaderInput(
                content="A" * (MAX_BASE64_INPUT_BYTES + 1),
            ))

    async def test_powerpoint_reader_rejects_oversized_base64(self) -> None:
        from llming_plumber.blocks.documents.powerpoint_reader import (
            PowerpointReaderBlock,
            PowerpointReaderInput,
        )

        block = PowerpointReaderBlock()
        with pytest.raises(ResourceLimitError, match="exceeds"):
            await block.execute(PowerpointReaderInput(
                content="A" * (MAX_BASE64_INPUT_BYTES + 1),
            ))

    async def test_parquet_reader_rejects_oversized_base64(self) -> None:
        from llming_plumber.blocks.documents.parquet_reader import (
            ParquetReaderBlock,
            ParquetReaderInput,
        )

        block = ParquetReaderBlock()
        with pytest.raises(ResourceLimitError, match="exceeds"):
            await block.execute(ParquetReaderInput(
                content="A" * (MAX_BASE64_INPUT_BYTES + 1),
            ))

    async def test_excel_extractor_rejects_oversized(self) -> None:
        from llming_plumber.blocks.documents.excel_extractor import (
            ExcelExtractorBlock,
            ExcelExtractorInput,
        )

        block = ExcelExtractorBlock()
        with pytest.raises(ResourceLimitError, match="exceeds"):
            await block.execute(ExcelExtractorInput(
                content="A" * (MAX_BASE64_INPUT_BYTES + 1),
            ))

    async def test_pdf_extractor_rejects_oversized(self) -> None:
        from llming_plumber.blocks.documents.pdf_extractor import (
            PdfExtractorBlock,
            PdfExtractorInput,
        )

        block = PdfExtractorBlock()
        with pytest.raises(ResourceLimitError, match="exceeds"):
            await block.execute(PdfExtractorInput(
                content="A" * (MAX_BASE64_INPUT_BYTES + 1),
            ))

    async def test_word_extractor_rejects_oversized(self) -> None:
        from llming_plumber.blocks.documents.word_extractor import (
            WordExtractorBlock,
            WordExtractorInput,
        )

        block = WordExtractorBlock()
        with pytest.raises(ResourceLimitError, match="exceeds"):
            await block.execute(WordExtractorInput(
                content="A" * (MAX_BASE64_INPUT_BYTES + 1),
            ))

    async def test_powerpoint_extractor_rejects_oversized(self) -> None:
        from llming_plumber.blocks.documents.powerpoint_extractor import (
            PowerpointExtractorBlock,
            PowerpointExtractorInput,
        )

        block = PowerpointExtractorBlock()
        with pytest.raises(ResourceLimitError, match="exceeds"):
            await block.execute(PowerpointExtractorInput(
                content="A" * (MAX_BASE64_INPUT_BYTES + 1),
            ))


# ------------------------------------------------------------------
# Document builder limits
# ------------------------------------------------------------------


class TestDocumentBuilderLimits:
    async def test_excel_builder_rejects_too_many_rows(self) -> None:
        from llming_plumber.blocks.documents.excel_builder import (
            ExcelBuilderBlock,
            ExcelBuilderInput,
            SheetDef,
        )

        block = ExcelBuilderBlock()
        from llming_plumber.blocks.limits import MAX_ROWS_PER_SHEET

        rows = [{"x": 1}] * (MAX_ROWS_PER_SHEET + 1)
        with pytest.raises(ResourceLimitError):
            await block.execute(ExcelBuilderInput(
                sheets=[SheetDef(name="big", rows=rows)],
            ))

    async def test_pdf_builder_rejects_too_many_pages(self) -> None:
        from llming_plumber.blocks.documents.pdf_builder import (
            PageDef,
            PdfBuilderBlock,
            PdfBuilderInput,
        )

        block = PdfBuilderBlock()
        pages = [PageDef() for _ in range(MAX_PAGES + 1)]
        with pytest.raises(ResourceLimitError):
            await block.execute(PdfBuilderInput(pages=pages))

    async def test_powerpoint_builder_rejects_too_many_slides(self) -> None:
        from llming_plumber.blocks.documents.powerpoint_builder import (
            PowerpointBuilderBlock,
            PowerpointBuilderInput,
            SlideDef,
        )

        block = PowerpointBuilderBlock()
        slides = [SlideDef() for _ in range(MAX_SLIDES + 1)]
        with pytest.raises(ResourceLimitError):
            await block.execute(PowerpointBuilderInput(slides=slides))


# ------------------------------------------------------------------
# YAML size limit
# ------------------------------------------------------------------


class TestYamlLimits:
    async def test_yaml_rejects_oversized_content(self) -> None:
        from llming_plumber.blocks.documents.yaml_parser import (
            YamlParserBlock,
            YamlParserInput,
        )

        block = YamlParserBlock()
        with pytest.raises(ResourceLimitError):
            await block.execute(YamlParserInput(
                content="x: " + "y" * (MAX_FILE_BYTES + 1),
            ))


# ------------------------------------------------------------------
# Registry safety
# ------------------------------------------------------------------


class TestRegistrySafety:
    def test_iterative_subclass_walk(self) -> None:
        """Registry uses iterative (not recursive) subclass walking."""
        from llming_plumber.blocks.registry import _walk_subclasses

        # Should work without stack overflow
        result = _walk_subclasses(BaseBlock)
        assert len(result) > 0


# ------------------------------------------------------------------
# Normal-sized inputs still work
# ------------------------------------------------------------------


class TestLimitsDoNotBlockNormalUsage:
    async def test_small_csv_works(self) -> None:
        from llming_plumber.blocks.core.csv_parser import CsvParserBlock, CsvParserInput

        block = CsvParserBlock()
        result = await block.execute(CsvParserInput(csv_text="a,b\n1,2\n3,4"))
        assert len(result.rows) == 2

    async def test_small_filter_works(self) -> None:
        from llming_plumber.blocks.core.filter_block import FilterBlock, FilterInput

        block = FilterBlock()
        result = await block.execute(FilterInput(
            items=[{"x": "1"}, {"x": "2"}], field="x", operator="eq", value="1",
        ))
        assert result.filtered_count == 1

    async def test_small_split_works(self) -> None:
        from llming_plumber.blocks.core.split import SplitBlock, SplitInput

        block = SplitBlock()
        result = await block.execute(SplitInput(
            items=[{"a": 1}, {"a": 2}],
        ))
        assert result.total == 2

    async def test_small_base64_works(self) -> None:
        from llming_plumber.blocks.core.base64_codec import (
            Base64CodecBlock,
            Base64CodecInput,
        )

        block = Base64CodecBlock()
        result = await block.execute(Base64CodecInput(text="Hello", mode="encode"))
        assert result.result == base64.b64encode(b"Hello").decode()
