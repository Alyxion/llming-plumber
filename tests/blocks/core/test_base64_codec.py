from __future__ import annotations

import pytest

from llming_plumber.blocks.core.base64_codec import (
    Base64CodecBlock,
    Base64CodecInput,
)


async def test_encode() -> None:
    block = Base64CodecBlock()
    result = await block.execute(Base64CodecInput(text="hello world"))
    assert result.result == "aGVsbG8gd29ybGQ="


async def test_decode() -> None:
    block = Base64CodecBlock()
    result = await block.execute(
        Base64CodecInput(text="aGVsbG8gd29ybGQ=", mode="decode")
    )
    assert result.result == "hello world"


async def test_roundtrip() -> None:
    block = Base64CodecBlock()
    original = "The quick brown fox"
    encoded = await block.execute(Base64CodecInput(text=original, mode="encode"))
    decoded = await block.execute(
        Base64CodecInput(text=encoded.result, mode="decode")
    )
    assert decoded.result == original


async def test_invalid_mode_raises() -> None:
    block = Base64CodecBlock()
    with pytest.raises(ValueError, match="Unsupported mode"):
        await block.execute(Base64CodecInput(text="test", mode="compress"))


async def test_empty_string_encode() -> None:
    block = Base64CodecBlock()
    result = await block.execute(Base64CodecInput(text=""))
    assert result.result == ""


async def test_block_type() -> None:
    assert Base64CodecBlock.block_type == "base64_codec"
