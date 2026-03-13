"""Barrier block: wait for all upstream branches and merge results.

The executor delivers every upstream parcel into ``items``.  Because
``tolerate_upstream_errors`` is ``True``, parcels from failed blocks
arrive as dicts with ``_error: True``.  The block separates successes
from failures and optionally raises when any upstream block errored.
"""

from __future__ import annotations

from typing import Any, ClassVar

from pydantic import Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput
from llming_plumber.blocks.limits import check_list_size


class UniteInput(BlockInput):
    items: list[dict[str, Any]] = Field(
        default_factory=list,
        title="Items",
        description="Collected upstream results (populated by executor during fan-in)",
    )
    require_all: bool = Field(
        default=True,
        title="Require All",
        description="Fail when any upstream block errored",
        json_schema_extra={"widget": "toggle"},
    )


class UniteOutput(BlockOutput):
    items: list[dict[str, Any]]
    succeeded: int
    failed: int
    errors: list[dict[str, Any]]
    all_ok: bool


class UniteBlock(BaseBlock[UniteInput, UniteOutput]):
    block_type: ClassVar[str] = "unite"
    icon: ClassVar[str] = "tabler/git-merge"
    categories: ClassVar[list[str]] = ["core/flow"]
    description: ClassVar[str] = (
        "Wait for all upstream blocks \u2014 optionally tolerate failures"
    )
    cache_ttl: ClassVar[int] = 0
    fan_in: ClassVar[bool] = True
    tolerate_upstream_errors: ClassVar[bool] = True

    async def execute(
        self, input: UniteInput, ctx: BlockContext | None = None
    ) -> UniteOutput:
        check_list_size(input.items, label="Unite items")

        ok: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []

        for item in input.items:
            if item.get("_error"):
                errors.append({
                    "block_uid": item.get("_block_uid", "unknown"),
                    "error": item.get("_message", "upstream block failed"),
                })
            else:
                ok.append(item)

        if input.require_all and errors:
            failed_names = ", ".join(e["block_uid"] for e in errors)
            raise ValueError(
                f"Unite: {len(errors)} upstream block(s) failed "
                f"(require_all=True): {failed_names}"
            )

        return UniteOutput(
            items=ok,
            succeeded=len(ok),
            failed=len(errors),
            errors=errors,
            all_ok=len(errors) == 0,
        )
