"""Compute hash of text using stdlib hashlib."""

from __future__ import annotations

import hashlib
from typing import ClassVar

from pydantic import Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput

SUPPORTED_ALGORITHMS = frozenset({"sha256", "sha1", "md5"})


class HashGeneratorInput(BlockInput):
    text: str = Field(
        title="Text",
        description="The text to compute a hash for",
        json_schema_extra={"widget": "textarea"},
    )
    algorithm: str = Field(
        default="sha256",
        title="Algorithm",
        description="Hash algorithm to use",
        json_schema_extra={"widget": "select", "options": ["sha256", "sha1", "md5"]},
    )


class HashGeneratorOutput(BlockOutput):
    hash_hex: str
    algorithm: str


class HashGeneratorBlock(BaseBlock[HashGeneratorInput, HashGeneratorOutput]):
    block_type: ClassVar[str] = "hash_generator"
    icon: ClassVar[str] = "tabler/hash"
    categories: ClassVar[list[str]] = ["core/transform"]
    description: ClassVar[str] = "Compute cryptographic hashes"
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self, input: HashGeneratorInput, ctx: BlockContext | None = None
    ) -> HashGeneratorOutput:
        algo = input.algorithm.lower()
        if algo not in SUPPORTED_ALGORITHMS:
            allowed = ", ".join(sorted(SUPPORTED_ALGORITHMS))
            msg = f"Unsupported algorithm: {input.algorithm}. Use one of: {allowed}"
            raise ValueError(msg)
        h = hashlib.new(algo, input.text.encode())
        return HashGeneratorOutput(hash_hex=h.hexdigest(), algorithm=algo)
