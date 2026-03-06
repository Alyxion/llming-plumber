from __future__ import annotations

from typing import ClassVar

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput


class DummyInput(BlockInput):
    value: int


class DummyOutput(BlockOutput):
    doubled: int


class DummyBlock(BaseBlock[DummyInput, DummyOutput]):
    block_type: ClassVar[str] = "dummy"

    async def execute(
        self, input: DummyInput, ctx: BlockContext | None = None
    ) -> DummyOutput:
        return DummyOutput(doubled=input.value * 2)


async def test_base_block_execute() -> None:
    block = DummyBlock()
    result = await block.execute(DummyInput(value=5))
    assert result.doubled == 10


async def test_base_block_execute_with_context() -> None:
    block = DummyBlock()
    ctx = BlockContext(run_id="r1", pipeline_id="p1", block_id="b1")
    result = await block.execute(DummyInput(value=3), ctx=ctx)
    assert result.doubled == 6


async def test_base_block_type() -> None:
    assert DummyBlock.block_type == "dummy"


async def test_base_block_cache_ttl_default() -> None:
    assert DummyBlock.cache_ttl == 0


async def test_block_context_defaults() -> None:
    ctx = BlockContext()
    assert ctx.run_id == ""
    assert ctx.pipeline_id == ""
    assert ctx.block_id == ""


async def test_block_input_output_serialization() -> None:
    inp = DummyInput(value=42)
    out = DummyOutput(doubled=84)
    assert inp.model_dump() == {"value": 42}
    assert out.model_dump() == {"doubled": 84}
    assert DummyInput.model_validate_json(inp.model_dump_json()) == inp
