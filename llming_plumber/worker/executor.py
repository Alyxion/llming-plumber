"""Pipeline execution engine — topological sort, block runner, ARQ task."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import defaultdict, deque
from datetime import UTC, datetime
from typing import Any, get_args

from bson import ObjectId
from pymongo import ReturnDocument

from llming_plumber.blocks.base import (
    BaseBlock,
    BlockContext,
    BlockInput,
    BlockOutput,
)
from llming_plumber.blocks.limits import (
    FAN_OUT_BATCH_SIZE,
    LOG_BLOCK_OUTPUT,
    MAX_ERROR_MESSAGE_LENGTH,
    MAX_FAN_OUT_ITEMS,
    MAX_RUN_LOG_ENTRIES,
    MAX_RUN_WALL_SECONDS,
    ResourceLimitError,
    check_list_size,
)
from llming_plumber.blocks.registry import BlockRegistry
from llming_plumber.models.log import RunLog
from llming_plumber.models.mongo_helpers import model_to_doc
from llming_plumber.models.run import BlockLogEntry
from llming_plumber.models.parcel import Parcel
from llming_plumber.models.pipeline import (
    BlockDefinition,
    PipeDefinition,
    PipelineDefinition,
)
from llming_plumber.worker.console import RunConsole
from llming_plumber.worker.debug_trace import DebugTracer
from llming_plumber.worker.events import RunEventPublisher

logger = logging.getLogger(__name__)

DEFAULT_MAX_CONCURRENCY: int = 10


def topological_sort(
    blocks: list[BlockDefinition],
    pipes: list[PipeDefinition],
) -> list[str]:
    """Return block UIDs in execution order using Kahn's algorithm.

    Raises ``ValueError`` on cycles.
    """
    block_uids = {b.uid for b in blocks}

    # Build adjacency list and in-degree map
    in_degree: dict[str, int] = {uid: 0 for uid in block_uids}
    adjacency: dict[str, list[str]] = {uid: [] for uid in block_uids}

    for pipe in pipes:
        src, tgt = pipe.source_block_uid, pipe.target_block_uid
        adjacency[src].append(tgt)
        in_degree[tgt] += 1

    # Kahn's BFS
    queue: deque[str] = deque()
    for uid in sorted(block_uids):  # sorted for deterministic order
        if in_degree[uid] == 0:
            queue.append(uid)

    result: list[str] = []
    while queue:
        uid = queue.popleft()
        result.append(uid)
        for neighbour in sorted(adjacency[uid]):
            in_degree[neighbour] -= 1
            if in_degree[neighbour] == 0:
                queue.append(neighbour)

    if len(result) != len(block_uids):
        msg = "Pipeline contains a cycle — topological sort is impossible"
        raise ValueError(msg)

    return result


def _get_input_output_types(
    block_cls: type[BaseBlock],  # type: ignore[type-arg]
) -> tuple[type[BlockInput], type[BlockOutput]]:
    """Extract InputT and OutputT from a BaseBlock subclass via __orig_bases__."""
    for base in getattr(block_cls, "__orig_bases__", ()):
        origin = getattr(base, "__origin__", None)
        if origin is None:
            continue
        if origin is BaseBlock or (
            isinstance(origin, type) and issubclass(origin, BaseBlock)
        ):
            args = get_args(base)
            if len(args) == 2:
                return args[0], args[1]
    msg = f"Cannot extract InputT/OutputT from {block_cls.__name__}"
    raise TypeError(msg)


def _apply_pipe_mapping(
    pipe: PipeDefinition,
    source_parcel: Parcel,
) -> dict[str, Any]:
    """Extract fields from a source parcel according to a pipe's mapping."""
    if pipe.field_mapping:
        mapped: dict[str, Any] = {}
        for target_field, source_field in pipe.field_mapping.items():
            if source_field in source_parcel.fields:
                mapped[target_field] = source_parcel.fields[source_field]
        return mapped
    return dict(source_parcel.fields)


def _merge_upstream(
    block_uid: str,
    incoming_pipes: dict[str, list[PipeDefinition]],
    parcels: dict[str, list[Parcel]],
    *,
    fan_out_parcel: Parcel | None = None,
    fan_out_source_uid: str | None = None,
) -> dict[str, Any]:
    """Merge fields from all upstream parcels into a single dict.

    When *fan_out_parcel* is provided, use it instead of looking up
    the fan-out source in *parcels*.
    """
    merged: dict[str, Any] = {}
    for pipe in incoming_pipes.get(block_uid, []):
        if fan_out_parcel and pipe.source_block_uid == fan_out_source_uid:
            merged.update(_apply_pipe_mapping(pipe, fan_out_parcel))
        else:
            src_list = parcels.get(pipe.source_block_uid, [])
            if src_list:
                merged.update(_apply_pipe_mapping(pipe, src_list[0]))
    return merged


def _build_global_vars(
    run_id: str,
    pipeline_id: str,
    block_uid: str,
) -> dict[str, Any]:
    """Build the global template variables available in every block config."""
    now = datetime.now(UTC)
    return {
        "run_id": run_id,
        "pipeline_id": pipeline_id,
        "block_id": block_uid,
        "date": now.strftime("%Y-%m-%d"),
        "time": now.strftime("%H:%M:%S"),
        "datetime": now.isoformat(),
        "hour": now.hour,
        "minute": now.minute,
        "weekday": now.strftime("%A"),
        "weekday_short": now.strftime("%a"),
        "year": now.year,
        "month": now.month,
        "day": now.day,
        "iso": now.isoformat(),
        "timestamp": int(now.timestamp()),
    }


# Global variables metadata for the UI autocomplete
GLOBAL_VARIABLES: list[dict[str, str]] = [
    {"name": "run_id", "type": "string", "description": "Current run ID"},
    {"name": "pipeline_id", "type": "string", "description": "Current pipeline ID"},
    {"name": "block_id", "type": "string", "description": "Current block ID"},
    {"name": "date", "type": "string", "description": "Current date (YYYY-MM-DD)"},
    {"name": "time", "type": "string", "description": "Current time (HH:MM:SS)"},
    {"name": "datetime", "type": "string", "description": "ISO datetime"},
    {"name": "hour", "type": "integer", "description": "Current hour (0-23)"},
    {"name": "minute", "type": "integer", "description": "Current minute (0-59)"},
    {"name": "weekday", "type": "string", "description": "Day name (Monday, etc.)"},
    {"name": "weekday_short", "type": "string", "description": "Day name short (Mon, etc.)"},
    {"name": "year", "type": "integer", "description": "Current year"},
    {"name": "month", "type": "integer", "description": "Current month (1-12)"},
    {"name": "day", "type": "integer", "description": "Current day of month"},
    {"name": "iso", "type": "string", "description": "ISO 8601 timestamp"},
    {"name": "timestamp", "type": "integer", "description": "Unix timestamp"},
]


def _resolve_templates(
    config: dict[str, Any],
    global_vars: dict[str, Any],
    upstream: dict[str, Any],
) -> dict[str, Any]:
    """Resolve {variable} placeholders in string config values.

    Variables are resolved from global_vars first, then upstream fields.
    Non-string values pass through unchanged.
    """
    context = {**global_vars, **upstream}
    resolved: dict[str, Any] = {}
    for key, value in config.items():
        if isinstance(value, str) and "{" in value:
            try:
                resolved[key] = value.format_map(_SafeFormatMap(context))
            except (KeyError, ValueError, IndexError):
                resolved[key] = value
        else:
            resolved[key] = value
    return resolved


class _SafeFormatMap(dict):  # type: ignore[type-arg]
    """Dict that returns the original placeholder for missing keys."""

    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


def _truncate(text: str, max_len: int) -> str:
    if len(text) <= max_len:
        return text
    return text[:max_len] + "… (truncated)"


def _humanize_block_error(exc: Exception, label: str) -> dict[str, Any]:
    """Convert exceptions into human-readable messages with field info."""
    from pydantic import ValidationError

    if isinstance(exc, ValidationError):
        fields: list[dict[str, str]] = []
        parts: list[str] = []
        for err in exc.errors():
            loc = err.get("loc", ())
            field_name = str(loc[-1]) if loc else "unknown"
            err_type = err.get("type", "")
            if err_type == "missing":
                parts.append(f'"{field_name}" is required but has no value')
                hint = "Connect a pipe to this input, or set a value in the block config"
            else:
                msg = err.get("msg", str(err_type))
                parts.append(f'"{field_name}": {msg}')
                hint = ""
            fields.append({"field": field_name, "message": parts[-1], "hint": hint})
        return {"message": f"{label}: {'; '.join(parts)}", "fields": fields}

    msg = str(exc)[:300]
    return {"message": f"{label}: {msg}"}


class _RunLogger:
    """Caps the number of log entries written to MongoDB per run.

    Error-level entries are always written regardless of the cap.
    """

    def __init__(self, db: Any, max_entries: int = MAX_RUN_LOG_ENTRIES) -> None:
        self._db = db
        self._max = max_entries
        self._count = 0

    async def write(self, entry: RunLog) -> None:
        is_error = entry.level == "error"
        if not is_error and self._count >= self._max:
            return
        await self._db["run_logs"].insert_one(model_to_doc(entry))
        self._count += 1


def _summarize_output(d: dict[str, Any], max_str: int = 200) -> dict[str, Any]:
    """Create a compact summary of block output for inline logging.

    Large strings are replaced with metadata (length); lists with count;
    file-like values with filename/size/type metadata.
    """
    result: dict[str, Any] = {}
    for k, v in d.items():
        if isinstance(v, str):
            if len(v) > max_str:
                # Check for file-like fields
                if any(hint in k.lower() for hint in ("path", "file", "url", "location")):
                    result[k] = v  # keep paths as-is
                else:
                    result[k] = f"({len(v)} chars)"
            else:
                result[k] = v
        elif isinstance(v, list):
            if len(v) > 5:
                result[k] = f"({len(v)} items)"
            else:
                result[k] = v
        elif isinstance(v, dict):
            if len(str(v)) > max_str:
                result[k] = f"({len(v)} keys)"
            else:
                result[k] = v
        elif isinstance(v, bytes):
            result[k] = f"({len(v)} bytes)"
        else:
            result[k] = v
    return result


async def run_blocks(
    pipeline: PipelineDefinition,
    run_id: str,
    db: Any,
    lemming_id: str,
    *,
    tracer: DebugTracer | None = None,
    console: RunConsole | None = None,
    events: RunEventPublisher | None = None,
) -> dict[str, Any]:
    """Execute all blocks in topological order, piping data between them.

    Supports fan-out (split) and fan-in (collect) for iteration.
    Returns the output of the final block as a dict.
    """
    BlockRegistry.discover()

    order = topological_sort(pipeline.blocks, pipeline.pipes)
    if tracer is None:
        tracer = DebugTracer(None, run_id, enabled=False)
    if console is None:
        console = RunConsole(None, run_id)

    block_map: dict[str, BlockDefinition] = {b.uid: b for b in pipeline.blocks}

    # Build a lookup: target_block_uid -> list of pipes feeding into it
    incoming_pipes: dict[str, list[PipeDefinition]] = defaultdict(list)
    for pipe in pipeline.pipes:
        incoming_pipes[pipe.target_block_uid].append(pipe)

    # Parcel store: block_uid -> list of Parcels produced by that block
    parcels: dict[str, list[Parcel]] = {}

    last_output: dict[str, Any] = {}
    block_log: list[dict[str, Any]] = []
    run_logger = _RunLogger(db)
    run_start = time.monotonic()
    await tracer.record_order(order)

    if events:
        await events.start(order, len(order))

    for block_uid in order:
        # Wall-clock timeout check
        elapsed_total = time.monotonic() - run_start
        if elapsed_total > MAX_RUN_WALL_SECONDS:
            msg = (
                f"Pipeline exceeded wall-clock limit of "
                f"{MAX_RUN_WALL_SECONDS}s (ran {elapsed_total:.0f}s)"
            )
            raise ResourceLimitError(msg)

        block_def = block_map[block_uid]
        block = BlockRegistry.create(block_def.block_type)
        block_cls = type(block)
        input_type, _output_type = _get_input_output_types(block_cls)

        fan_out_field: str | None = getattr(block_cls, "fan_out_field", None)
        is_fan_in: bool = getattr(block_cls, "fan_in", False)

        # Update run: current_block
        await db["runs"].update_one(
            {"_id": ObjectId(run_id)},
            {"$set": {
                f"block_states.{block_uid}.status": "running",
                "current_block": block_uid,
            }},
        )

        if events:
            await events.block_start(
                block_uid, block_def.block_type,
                block_def.label, order.index(block_uid),
            )

        ctx = BlockContext(
            run_id=run_id,
            pipeline_id=pipeline.id,
            block_id=block_uid,
            console=console,
        )

        start = time.monotonic()

        try:
            if is_fan_in:
                output_dict = await _execute_fan_in(
                    block, input_type, block_def, block_uid,
                    incoming_pipes, parcels, ctx,
                )
                parcels[block_uid] = [
                    Parcel(uid=block_uid, fields=output_dict),
                ]
            else:
                # Check if any upstream produced multiple parcels (fan-out)
                fan_out_source_uid: str | None = None
                fan_out_parcels: list[Parcel] = []
                for pipe in incoming_pipes.get(block_uid, []):
                    src_list = parcels.get(pipe.source_block_uid, [])
                    if len(src_list) > 1:
                        fan_out_source_uid = pipe.source_block_uid
                        fan_out_parcels = src_list
                        break

                if fan_out_parcels:
                    output_dict = await _execute_fan_out_branch(
                        block, input_type, block_def, block_uid,
                        incoming_pipes, parcels,
                        fan_out_source_uid or "",
                        fan_out_parcels, ctx,
                        run_start=run_start,
                    )
                else:
                    output_dict = await _execute_single(
                        block, input_type, block_def, block_uid,
                        incoming_pipes, parcels, ctx,
                        fan_out_field=fan_out_field,
                    )

        except Exception as exc:
            elapsed_ms = (time.monotonic() - start) * 1000
            await _record_block_failure(
                db, run_id, lemming_id, block_uid, block_def,
                elapsed_ms, exc, run_logger,
            )
            await tracer.record_block(
                block_uid, block_def.block_type,
                duration_ms=elapsed_ms,
                parcel_count=0,
                status="failed",
                error=str(exc),
            )
            block_log.append(BlockLogEntry(
                uid=block_uid, block_type=block_def.block_type,
                label=block_def.label, status="failed",
                duration_ms=round(elapsed_ms, 1), parcel_count=0,
                error=_truncate(str(exc), 500),
            ).model_dump())
            # Save partial log even on failure
            await db["runs"].update_one(
                {"_id": ObjectId(run_id)},
                {"$set": {"log": block_log}},
            )
            if events:
                error_info = _humanize_block_error(exc, block_def.label)
                await events.block_done(
                    block_uid, block_def.block_type, block_def.label,
                    duration_ms=elapsed_ms, parcel_count=0,
                    status="failed", error=error_info["message"],
                    error_fields=error_info.get("fields"),
                )
                await events.error(block_uid, error_info["message"])
            raise

        elapsed_ms = (time.monotonic() - start) * 1000
        last_output = (
            parcels[block_uid][0].fields if parcels[block_uid] else {}
        )

        # Write RunLog entry — never include output content by default
        log_entry = RunLog(
            run_id=run_id,
            lemming_id=lemming_id,
            block_id=block_uid,
            block_type=block_def.block_type,
            level="info",
            msg="Block completed",
            duration_ms=elapsed_ms,
            output_summary=last_output if LOG_BLOCK_OUTPUT else None,
        )
        await run_logger.write(log_entry)

        # Update block state — status and timing only, no output content
        state_update: dict[str, Any] = {
            f"block_states.{block_uid}.status": "completed",
            f"block_states.{block_uid}.duration_ms": elapsed_ms,
        }
        if LOG_BLOCK_OUTPUT:
            state_update[f"block_states.{block_uid}.output"] = last_output
        await db["runs"].update_one(
            {"_id": ObjectId(run_id)},
            {"$set": state_update},
        )

        # Debug trace: record block summary + parcel glimpses
        block_parcels = parcels.get(block_uid, [])
        await tracer.record_block(
            block_uid, block_def.block_type,
            duration_ms=elapsed_ms,
            parcel_count=len(block_parcels),
        )
        await tracer.record_parcels(
            block_uid,
            [p.fields for p in block_parcels],
        )

        if events:
            await events.block_done(
                block_uid, block_def.block_type, block_def.label,
                duration_ms=elapsed_ms,
                parcel_count=len(block_parcels),
                status="completed",
                output=last_output,
            )

        block_log.append(BlockLogEntry(
            uid=block_uid, block_type=block_def.block_type,
            label=block_def.label, status="completed",
            duration_ms=round(elapsed_ms, 1),
            parcel_count=len(block_parcels),
            output_summary=_summarize_output(last_output),
        ).model_dump())

    total_ms = (time.monotonic() - run_start) * 1000
    if events:
        await events.done(
            total_ms=total_ms,
            blocks_run=len(order),
            status="completed",
            output=last_output,
        )

    # Persist inline log on the run document
    try:
        await db["runs"].update_one(
            {"_id": ObjectId(run_id)},
            {"$set": {"log": block_log}},
        )
    except Exception:
        logger.debug("Failed to save inline block log", exc_info=True)

    return last_output


# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------


async def _execute_single(
    block: BaseBlock,  # type: ignore[type-arg]
    input_type: type[BlockInput],
    block_def: BlockDefinition,
    block_uid: str,
    incoming_pipes: dict[str, list[PipeDefinition]],
    parcels: dict[str, list[Parcel]],
    ctx: BlockContext,
    *,
    fan_out_field: str | None = None,
) -> dict[str, Any]:
    """Run a block once with merged upstream fields."""
    merged = _merge_upstream(block_uid, incoming_pipes, parcels)
    global_vars = _build_global_vars(ctx.run_id, ctx.pipeline_id, block_uid)
    resolved_config = _resolve_templates(block_def.config, global_vars, merged)
    input_data = input_type(**{**resolved_config, **merged})
    output = await block.execute(input_data, ctx)
    output_dict = output.model_dump()

    if fan_out_field and fan_out_field in output_dict:
        items = output_dict[fan_out_field]
        check_list_size(
            items,
            limit=MAX_FAN_OUT_ITEMS,
            label=f"Fan-out field '{fan_out_field}' in block '{block_uid}'",
        )
        result_parcels: list[Parcel] = []
        for item in items:
            if isinstance(item, dict):
                result_parcels.append(Parcel(uid=block_uid, fields=item))
            else:
                result_parcels.append(
                    Parcel(uid=block_uid, fields={"item": item})
                )
        if result_parcels:
            parcels[block_uid] = result_parcels
        else:
            # Empty list → store output as-is (no fan-out occurs)
            parcels[block_uid] = [Parcel(uid=block_uid, fields=output_dict)]
    else:
        parcels[block_uid] = [Parcel(uid=block_uid, fields=output_dict)]

    return output_dict


async def _execute_fan_out_branch(
    block: BaseBlock,  # type: ignore[type-arg]
    input_type: type[BlockInput],
    block_def: BlockDefinition,
    block_uid: str,
    incoming_pipes: dict[str, list[PipeDefinition]],
    parcels: dict[str, list[Parcel]],
    fan_out_source_uid: str,
    fan_out_parcels: list[Parcel],
    ctx: BlockContext,
    *,
    run_start: float = 0.0,
) -> dict[str, Any]:
    """Run a block once per fan-out parcel, in batches with concurrency."""
    check_list_size(
        fan_out_parcels,
        limit=MAX_FAN_OUT_ITEMS,
        label=f"Fan-out for block '{block_uid}'",
    )

    max_conc = int(block_def.config.pop("_max_concurrency", DEFAULT_MAX_CONCURRENCY))
    sem = asyncio.Semaphore(max_conc)
    batch_size = FAN_OUT_BATCH_SIZE

    global_vars = _build_global_vars(ctx.run_id, ctx.pipeline_id, block_uid)

    async def _run_one(src_parcel: Parcel) -> BlockOutput:
        async with sem:
            merged = _merge_upstream(
                block_uid, incoming_pipes, parcels,
                fan_out_parcel=src_parcel,
                fan_out_source_uid=fan_out_source_uid,
            )
            resolved_config = _resolve_templates(block_def.config, global_vars, merged)
            inp = input_type(**{**resolved_config, **merged})
            return await block.execute(inp, ctx)

    # Process in batches to avoid creating thousands of coroutines at once
    all_results: list[BlockOutput] = []
    for i in range(0, len(fan_out_parcels), batch_size):
        # Wall-clock check between batches
        if run_start and time.monotonic() - run_start > MAX_RUN_WALL_SECONDS:
            msg = (
                f"Fan-out for '{block_uid}' exceeded wall-clock "
                f"limit of {MAX_RUN_WALL_SECONDS}s"
            )
            raise ResourceLimitError(msg)
        batch = fan_out_parcels[i : i + batch_size]
        batch_results = await asyncio.gather(*[_run_one(p) for p in batch])
        all_results.extend(batch_results)

    parcels[block_uid] = [
        Parcel(uid=block_uid, fields=r.model_dump()) for r in all_results
    ]
    return all_results[0].model_dump() if all_results else {}


async def _execute_fan_in(
    block: BaseBlock,  # type: ignore[type-arg]
    input_type: type[BlockInput],
    block_def: BlockDefinition,
    block_uid: str,
    incoming_pipes: dict[str, list[PipeDefinition]],
    parcels: dict[str, list[Parcel]],
    ctx: BlockContext,
) -> dict[str, Any]:
    """Gather all upstream parcels into an ``items`` list and execute once."""
    all_items: list[dict[str, Any]] = []
    for pipe in incoming_pipes.get(block_uid, []):
        for p in parcels.get(pipe.source_block_uid, []):
            all_items.append(_apply_pipe_mapping(pipe, p))

    global_vars = _build_global_vars(ctx.run_id, ctx.pipeline_id, block_uid)
    resolved_config = _resolve_templates(block_def.config, global_vars, {})
    input_data = input_type(**{**resolved_config, "items": all_items})
    output = await block.execute(input_data, ctx)
    return output.model_dump()


async def _record_block_failure(
    db: Any,
    run_id: str,
    lemming_id: str,
    block_uid: str,
    block_def: BlockDefinition,
    elapsed_ms: float,
    exc: Exception,
    run_logger: _RunLogger,
) -> None:
    """Write failure state and log for a block."""
    error_msg = _truncate(str(exc), MAX_ERROR_MESSAGE_LENGTH)
    await db["runs"].update_one(
        {"_id": ObjectId(run_id)},
        {
            "$set": {
                f"block_states.{block_uid}.status": "failed",
                f"block_states.{block_uid}.error": error_msg,
                f"block_states.{block_uid}.duration_ms": elapsed_ms,
            }
        },
    )
    log_entry = RunLog(
        run_id=run_id,
        lemming_id=lemming_id,
        block_id=block_uid,
        block_type=block_def.block_type,
        level="error",
        msg=f"Block failed: {error_msg}",
        duration_ms=elapsed_ms,
    )
    await run_logger.write(log_entry)


async def _publish_status(
    db: Any,
    run_id: str,
    status: str,
    lemming_id: str,
) -> None:
    """Publish a run status update to Redis pub/sub."""
    try:
        from llming_plumber.db import get_redis

        redis = get_redis()
        await redis.publish(
            "plumber:run_updates",
            json.dumps({
                "run_id": run_id,
                "status": status,
                "lemming_id": lemming_id,
            }),
        )
    except Exception:
        logger.warning("Failed to publish run update to Redis", exc_info=True)


async def _prune_old_runs(db: Any, pipeline_id: Any) -> None:
    """Keep at most max_runs_per_pipeline runs per pipeline.

    Deletes the oldest finished runs (and their run_logs) beyond the limit.
    Active runs (queued/running) are never pruned.
    """
    from llming_plumber.config import settings

    limit = settings.max_runs_per_pipeline
    try:
        # Find IDs of runs to keep (newest N)
        keep_cursor = db["runs"].find(
            {"pipeline_id": pipeline_id},
            {"_id": 1},
        ).sort("created_at", -1).limit(limit)
        keep_ids = [doc["_id"] async for doc in keep_cursor]

        if len(keep_ids) < limit:
            return  # nothing to prune

        # Delete older runs that are finished
        result = await db["runs"].delete_many({
            "pipeline_id": pipeline_id,
            "_id": {"$nin": keep_ids},
            "status": {"$nin": ["queued", "running"]},
        })
        if result.deleted_count:
            # Also clean up associated run_logs
            await db["run_logs"].delete_many({
                "run_id": {"$nin": [str(rid) for rid in keep_ids]},
            })
            logger.debug(
                "Pruned %d old runs for pipeline %s",
                result.deleted_count, pipeline_id,
            )
    except Exception:
        logger.debug("Run pruning failed", exc_info=True)


async def execute_pipeline(ctx: dict, *, run_id: str) -> dict[str, Any]:  # type: ignore[type-arg]
    """ARQ task function.

    Claims run via atomic find_one_and_update, executes
    blocks, updates status.
    """
    db = ctx["db"]
    lemming_id = ctx["lemming_id"]

    # Atomic claim: only one lemming gets this run
    run_doc = await db["runs"].find_one_and_update(
        {"_id": ObjectId(run_id), "status": "queued"},
        {
            "$set": {
                "status": "running",
                "lemming_id": lemming_id,
                "started_at": datetime.now(UTC),
            }
        },
        return_document=ReturnDocument.AFTER,
    )
    if not run_doc:
        return {"skipped": True}

    await _publish_status(db, run_id, "running", lemming_id)

    pid = run_doc["pipeline_id"]
    if isinstance(pid, str):
        pid = ObjectId(pid)
    pipeline_doc = await db["pipelines"].find_one({"_id": pid})
    if not pipeline_doc:
        await db["runs"].update_one(
            {"_id": ObjectId(run_id)},
            {
                "$set": {
                    "status": "failed",
                    "error": "Pipeline definition not found",
                    "finished_at": datetime.now(UTC),
                }
            },
        )
        return {"error": "Pipeline definition not found"}

    # Convert pipeline doc to model
    from llming_plumber.models.mongo_helpers import doc_to_model

    pipeline = doc_to_model(pipeline_doc, PipelineDefinition)

    # Redis is used for console (always) and debug trace (when flagged)
    redis = None
    try:
        from llming_plumber.db import get_redis
        redis = get_redis()
    except Exception:
        logger.warning("Redis unavailable — console and debug disabled", exc_info=True)

    console = RunConsole(redis, run_id)
    debug_enabled = run_doc.get("debug", False)
    tracer = DebugTracer(redis, run_id, enabled=debug_enabled)
    event_pub = RunEventPublisher(redis, run_id, str(run_doc["pipeline_id"]))

    pipeline_id = run_doc["pipeline_id"]

    try:
        result = await run_blocks(
            pipeline, run_id, db, lemming_id,
            tracer=tracer, console=console, events=event_pub,
        )
        await db["runs"].update_one(
            {"_id": ObjectId(run_id)},
            {
                "$set": {
                    "status": "completed",
                    "finished_at": datetime.now(UTC),
                }
            },
        )
        await _publish_status(db, run_id, "completed", lemming_id)
        await _prune_old_runs(db, pipeline_id)
        return result

    except Exception as e:
        await db["runs"].update_one(
            {"_id": ObjectId(run_id)},
            {
                "$set": {
                    "status": "failed",
                    "error": _truncate(str(e), MAX_ERROR_MESSAGE_LENGTH),
                    "finished_at": datetime.now(UTC),
                }
            },
        )
        await _publish_status(db, run_id, "failed", lemming_id)
        await event_pub.done(
            total_ms=0, blocks_run=0,
            status="failed",
        )
        await _prune_old_runs(db, pipeline_id)
        raise
