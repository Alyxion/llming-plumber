"""Periodic guard block — pause and resume a pipeline based on a recurring check.

Unlike the one-shot :mod:`guard` block, the periodic guard spawns a background
task (managed by the executor) that re-evaluates the condition at a configurable
interval.  When the condition fails the executor pauses fan-out processing and
long-running blocks that call ``await ctx.check_pause()``.  When the condition
passes again, execution resumes from where it left off.

If the pipeline stays paused longer than ``max_pause_seconds``, the run is
aborted.

.. code-block:: text

   [Periodic Guard (ip check, every 60 s)]
       → [Web Crawler]
       → [Azure Blob Resource]

The block's ``execute()`` performs the initial check (like a regular guard).
The periodic polling is handled by the executor via :func:`run_guard_loop`.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, ClassVar

from pydantic import Field

from llming_plumber.blocks.base import (
    BaseBlock,
    BlockContext,
    BlockInput,
    BlockOutput,
)
from llming_plumber.blocks.core.guard import GuardAbortError
from llming_plumber.blocks.core.safe_eval import SafeEvalError, safe_eval
from llming_plumber.blocks.limits import GUARD_MIN_INTERVAL_SECONDS, MAX_PAUSE_SECONDS
from llming_plumber.blocks.registry import BlockRegistry
from llming_plumber.worker.pause import PauseController

logger = logging.getLogger(__name__)


class PeriodicGuardInput(BlockInput):
    check_block_type: str = Field(
        title="Check Block",
        description="Block type to run as the periodic check (e.g. ip_geolocation)",
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
    interval_seconds: float = Field(
        default=60.0,
        title="Check Interval (seconds)",
        description="How often to re-evaluate the condition while the pipeline runs.",
        json_schema_extra={"widget": "number", "min": 10, "max": 3600},
    )
    pause_message: str = Field(
        default="Guard condition failed — pipeline paused.",
        title="Pause Message",
        description=(
            "Message shown when the guard pauses the pipeline. "
            "May use {field} placeholders from the check output."
        ),
    )
    max_pause_seconds: int = Field(
        default=MAX_PAUSE_SECONDS,
        title="Max Pause Duration (seconds)",
        description="Abort the pipeline if it stays paused longer than this.",
        json_schema_extra={"widget": "number", "min": 60},
    )


class PeriodicGuardOutput(BlockOutput):
    passed: bool = True
    check_value: str = ""
    check_output: dict[str, Any] = {}


def _run_check(
    check_block_type: str,
    check_config: dict[str, Any],
    condition: str,
) -> Any:
    """Prepare a coroutine that runs the check block and evaluates the condition.

    Returns a coroutine factory (call it to get the awaitable).
    """

    async def _do_check() -> tuple[bool, dict[str, Any]]:
        BlockRegistry.discover()
        check_block = BlockRegistry.create(check_block_type)
        check_cls = type(check_block)

        from llming_plumber.worker.executor import _get_input_output_types

        input_type, _ = _get_input_output_types(check_cls)
        check_input = input_type(**check_config)
        output_obj = await check_block.execute(check_input, ctx=None)
        output: dict[str, Any] = output_obj.model_dump()
        result = safe_eval(condition, output)
        return bool(result), output

    return _do_check


class _SafeFmt(dict):  # type: ignore[type-arg]
    """Dict that returns the original {key} for missing keys."""

    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


async def run_guard_loop(
    *,
    check_block_type: str,
    check_config: dict[str, Any],
    condition: str,
    interval_seconds: float,
    pause_message: str,
    max_pause_seconds: int,
    pause_ctl: PauseController,
    guard_block_uid: str,
    run_id: str,
    db: Any,
    console: Any | None = None,
    events: Any | None = None,
) -> None:
    """Background task: periodically re-evaluate a guard condition.

    Called by the executor after the periodic_guard block's initial check
    passes.  Runs until cancelled (when the pipeline finishes).
    """
    interval = max(interval_seconds, GUARD_MIN_INTERVAL_SECONDS)
    max_pause = min(max_pause_seconds, MAX_PAUSE_SECONDS)

    do_check = _run_check(check_block_type, check_config, condition)
    pause_start: float | None = None

    while True:
        await asyncio.sleep(interval)

        try:
            passed, output = await do_check()
        except Exception as exc:
            logger.warning(
                "Periodic guard check failed for run %s: %s",
                run_id, exc,
            )
            # Treat check failure as "condition failed" — pause
            passed = False
            output = {}

        if passed:
            if pause_ctl.is_paused:
                pause_ctl.resume()
                pause_start = None
                # Update run status back to running
                from bson import ObjectId

                await db["runs"].update_one(
                    {"_id": ObjectId(run_id)},
                    {"$set": {"status": "running"}},
                )
                msg = "Guard passed — pipeline resumed"
                if console:
                    await console.write(guard_block_uid, msg)
                if events:
                    await events.resumed(guard_block_uid, msg)
        else:
            if not pause_ctl.is_paused:
                pause_ctl.pause()
                pause_start = time.monotonic()
                # Update run status to paused
                from bson import ObjectId

                await db["runs"].update_one(
                    {"_id": ObjectId(run_id)},
                    {"$set": {"status": "paused"}},
                )
                try:
                    msg = pause_message.format_map(_SafeFmt(output))
                except Exception:
                    msg = pause_message
                if console:
                    await console.write(guard_block_uid, msg, level="warning")
                if events:
                    await events.paused(guard_block_uid, msg)
            elif pause_start and time.monotonic() - pause_start > max_pause:
                raise GuardAbortError(
                    f"Pipeline paused for >{max_pause}s — aborting"
                )


class PeriodicGuardBlock(BaseBlock[PeriodicGuardInput, PeriodicGuardOutput]):
    block_type: ClassVar[str] = "periodic_guard"
    icon: ClassVar[str] = "tabler/shield-check"
    categories: ClassVar[list[str]] = ["core/flow"]
    description: ClassVar[str] = (
        "Periodically check a condition and pause/resume the pipeline"
    )
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self,
        input: PeriodicGuardInput,
        ctx: BlockContext | None = None,
    ) -> PeriodicGuardOutput:
        """Run the initial check.  Periodic polling is managed by the executor."""
        # 1. Parse check config
        try:
            check_cfg: dict[str, Any] = json.loads(input.check_config)
        except json.JSONDecodeError as exc:
            msg = f"Invalid check_config JSON: {exc}"
            raise ValueError(msg) from exc

        # 2. Run initial check
        do_check = _run_check(
            input.check_block_type, check_cfg, input.condition,
        )
        try:
            passed, output = await do_check()
        except SafeEvalError as exc:
            msg = f"Guard condition error: {exc}"
            raise ValueError(msg) from exc

        if not passed:
            try:
                msg = input.pause_message.format_map(_SafeFmt(output))
            except Exception:
                msg = input.pause_message
            if ctx:
                await ctx.log(msg, level="error")
            raise GuardAbortError(msg)

        if ctx:
            await ctx.log(
                f"Guard initial check passed — will re-check every "
                f"{input.interval_seconds}s"
            )

        return PeriodicGuardOutput(
            passed=True,
            check_value=str(passed),
            check_output=output,
        )
