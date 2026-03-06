from __future__ import annotations

import hashlib

import pytest

from llming_plumber.blocks.core.hash_generator import (
    HashGeneratorBlock,
    HashGeneratorInput,
)


async def test_sha256_default() -> None:
    block = HashGeneratorBlock()
    result = await block.execute(HashGeneratorInput(text="hello"))
    expected = hashlib.sha256(b"hello").hexdigest()
    assert result.hash_hex == expected
    assert result.algorithm == "sha256"


async def test_sha1() -> None:
    block = HashGeneratorBlock()
    result = await block.execute(
        HashGeneratorInput(text="hello", algorithm="sha1")
    )
    expected = hashlib.sha1(b"hello").hexdigest()
    assert result.hash_hex == expected
    assert result.algorithm == "sha1"


async def test_md5() -> None:
    block = HashGeneratorBlock()
    result = await block.execute(
        HashGeneratorInput(text="hello", algorithm="md5")
    )
    expected = hashlib.md5(b"hello").hexdigest()
    assert result.hash_hex == expected
    assert result.algorithm == "md5"


async def test_unsupported_algorithm_raises() -> None:
    block = HashGeneratorBlock()
    with pytest.raises(ValueError, match="Unsupported algorithm"):
        await block.execute(
            HashGeneratorInput(text="hello", algorithm="sha512")
        )


async def test_empty_text() -> None:
    block = HashGeneratorBlock()
    result = await block.execute(HashGeneratorInput(text=""))
    expected = hashlib.sha256(b"").hexdigest()
    assert result.hash_hex == expected


async def test_block_type() -> None:
    assert HashGeneratorBlock.block_type == "hash_generator"
