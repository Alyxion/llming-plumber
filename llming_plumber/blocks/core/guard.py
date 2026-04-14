"""Pipeline guard block — abort a run based on a dynamic check.

The guard internally instantiates and executes another block (the "check"),
then evaluates a condition on the check's output.  If the condition is
**false**, the pipeline is aborted with a clear message.

Typical use-case: abort crawl pipelines when the public IP matches the
office IP, so competitors never see crawl traffic from a known address.

.. code-block:: text

   [Guard (ip_geolocation → abort if IP == 88.79.…)] → [Crawlers…]
"""

from __future__ import annotations

import json
from typing import Any, ClassVar

from pydantic import Field

from llming_plumber.blocks.base import (
    BaseBlock,
    BlockContext,
    BlockInput,
    BlockOutput,
)
from llming_plumber.blocks.core.safe_eval import SafeEvalError, safe_eval
from llming_plumber.blocks.registry import BlockRegistry


class GuardAbortError(RuntimeError):
    """Raised when a guard condition triggers pipeline abort."""


class GuardInput(BlockInput):
    check_block_type: str = Field(
        title="Check Block",
        description="Block type to run as a pre-flight check (e.g. ip_geolocation)",
        json_schema_extra={
            "widget": "combobox",
            "options_ref": "block_types",
            "placeholder": "ip_geolocation",
        },
    )
    check_config: str = Field(
        default="{}",
        title="Check Config",
        description=(
            "JSON config to pass to the check block "
            '(e.g. {"ip_addresses": [], "api_key": "…"})'
        ),
        json_schema_extra={"widget": "code", "rows": 4},
    )
    condition: str = Field(
        title="Pass Condition",
        description=(
            "Expression that must be true for the pipeline to continue. "
            "Uses the check block's output fields as variables. "
            'Example: results[0]["query"] != "88.79.198.243"'
        ),
        json_schema_extra={
            "widget": "code",
            "rows": 2,
            "placeholder": 'results[0]["query"] != "88.79.198.243"',
        },
    )
    abort_message: str = Field(
        default="Guard check failed — pipeline aborted.",
        title="Abort Message",
        description=(
            "Message shown when the guard aborts the pipeline. "
            "May use {field} placeholders from the check output."
        ),
    )


class GuardOutput(BlockOutput):
    passed: bool = True
    check_value: str = ""
    check_output: dict[str, Any] = {}


class GuardBlock(BaseBlock[GuardInput, GuardOutput]):
    block_type: ClassVar[str] = "guard"
    icon: ClassVar[str] = "tabler/shield-check"
    categories: ClassVar[list[str]] = ["core/flow"]
    description: ClassVar[str] = (
        "Run a check block and abort the pipeline if a condition fails"
    )
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self,
        input: GuardInput,
        ctx: BlockContext | None = None,
    ) -> GuardOutput:
        # 1. Parse check config
        try:
            check_cfg: dict[str, Any] = json.loads(input.check_config)
        except json.JSONDecodeError as exc:
            msg = f"Invalid check_config JSON: {exc}"
            raise ValueError(msg) from exc

        # 2. Instantiate and run the check block
        BlockRegistry.discover()
        check_block = BlockRegistry.create(input.check_block_type)
        check_cls = type(check_block)

        # Resolve input type
        from llming_plumber.worker.executor import _get_input_output_types

        input_type, _output_type = _get_input_output_types(check_cls)
        check_input = input_type(**check_cfg)
        check_output_obj = await check_block.execute(check_input, ctx=None)
        check_output: dict[str, Any] = check_output_obj.model_dump()

        if ctx:
            await ctx.log(
                f"Guard check ({input.check_block_type}) completed"
            )

        # 3. Evaluate pass condition
        try:
            result = safe_eval(input.condition, check_output)
        except SafeEvalError as exc:
            msg = f"Guard condition error: {exc}"
            raise ValueError(msg) from exc

        passed = bool(result)

        if not passed:
            # Resolve {placeholders} in abort message from check output
            try:
                msg = input.abort_message.format_map(_SafeFmt(check_output))
            except Exception:
                msg = input.abort_message

            if ctx:
                await ctx.log(msg, level="error")
            raise GuardAbortError(msg)

        if ctx:
            await ctx.log("Guard passed — pipeline continues")

        return GuardOutput(
            passed=True,
            check_value=str(result),
            check_output=check_output,
        )


class _SafeFmt(dict):  # type: ignore[type-arg]
    """Dict that returns the original {key} for missing keys."""

    def __missing__(self, key: str) -> str:
        return "{" + key + "}"
