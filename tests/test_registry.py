"""Tests for the block auto-discovery registry."""

from __future__ import annotations

import pytest

from llming_plumber.blocks.base import BaseBlock
from llming_plumber.blocks.registry import BlockMeta, BlockRegistry


@pytest.fixture(autouse=True)
def _reset_registry():
    """Reset registry before each test."""
    BlockRegistry.reset()
    yield
    BlockRegistry.reset()


def test_discover_finds_blocks():
    """After discover(), registry has entries."""
    BlockRegistry.discover()
    catalog = BlockRegistry.catalog()
    assert len(catalog) > 0, "Expected at least one block to be discovered"


def test_get_existing_block():
    """get() returns the correct class for a known block_type."""
    BlockRegistry.discover()
    cls = BlockRegistry.get("text_template")
    assert issubclass(cls, BaseBlock)
    assert cls.block_type == "text_template"


def test_get_missing_block():
    """get() raises KeyError for an unknown block_type."""
    BlockRegistry.discover()
    with pytest.raises(KeyError):
        BlockRegistry.get("nonexistent_block_type_xyz")


def test_create_block():
    """create() returns an instance of the block."""
    BlockRegistry.discover()
    block = BlockRegistry.create("text_template")
    assert isinstance(block, BaseBlock)
    assert block.block_type == "text_template"


def test_catalog_has_schemas():
    """catalog entries have input_schema and output_schema."""
    BlockRegistry.discover()
    catalog = BlockRegistry.catalog()
    for meta in catalog:
        assert isinstance(meta, BlockMeta)
        assert isinstance(meta.input_schema, dict), (
            f"{meta.block_type} missing input_schema"
        )
        assert isinstance(meta.output_schema, dict), (
            f"{meta.block_type} missing output_schema"
        )
        # Pydantic JSON schemas should have at least a 'properties' or 'type' key
        assert meta.input_schema, f"{meta.block_type} has empty input_schema"
        assert meta.output_schema, f"{meta.block_type} has empty output_schema"


def test_catalog_block_types_unique():
    """No duplicate block_types in catalog."""
    BlockRegistry.discover()
    catalog = BlockRegistry.catalog()
    block_types = [m.block_type for m in catalog]
    assert len(block_types) == len(set(block_types)), (
        "Duplicate block_types found: "
        f"{[t for t in block_types if block_types.count(t) > 1]}"
    )
