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
    MAX_FAN_OUT_ITEMS,
    check_list_size,
)
from llming_plumber.blocks.registry import BlockRegistry
from llming_plumber.models.log import RunLog
from llming_plumber.models.mongo_helpers import model_to_doc
from llming_plumber.models.parcel import Parcel
from llming_plumber.models.pipeline import (
    BlockDefinition,
    PipeDefinition,
    PipelineDefinition,
)

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


async def run_blocks(
    pipeline: PipelineDefinition,
    run_id: str,
    db: Any,
    lemming_id: str,
) -> dict[str, Any]:
    """Execute all blocks in topological order, piping data between them.

    Supports fan-out (split) and fan-in (collect) for iteration.
    Returns the output of the final block as a dict.
    """
    BlockRegistry.discover()

    order = topological_sort(pipeline.blocks, pipeline.pipes)

    block_map: dict[str, BlockDefinition] = {b.uid: b for b in pipeline.blocks}

    # Build a lookup: target_block_uid -> list of pipes feeding into it
    incoming_pipes: dict[str, list[PipeDefinition]] = defaultdict(list)
    for pipe in pipeline.pipes:
        incoming_pipes[pipe.target_block_uid].append(pipe)

    # Parcel store: block_uid -> list of Parcels produced by that block
    parcels: dict[str, list[Parcel]] = {}

    last_output: dict[str, Any] = {}

    for block_uid in order:
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

        ctx = BlockContext(
            run_id=run_id,
            pipeline_id=pipeline.id,
            block_id=block_uid,
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
                db, run_id, lemming_id, block_uid, block_def, elapsed_ms, exc,
            )
            raise

        elapsed_ms = (time.monotonic() - start) * 1000
        last_output = (
            parcels[block_uid][0].fields if parcels[block_uid] else {}
        )

        # Write RunLog entry
        log_entry = RunLog(
            run_id=run_id,
            lemming_id=lemming_id,
            block_id=block_uid,
            block_type=block_def.block_type,
            level="info",
            msg="Block completed",
            duration_ms=elapsed_ms,
            output_summary=last_output,
        )
        await db["run_logs"].insert_one(model_to_doc(log_entry))

        # Update block state in run doc
        await db["runs"].update_one(
            {"_id": ObjectId(run_id)},
            {
                "$set": {
                    f"block_states.{block_uid}.status": "completed",
                    f"block_states.{block_uid}.output": last_output,
                    f"block_states.{block_uid}.duration_ms": elapsed_ms,
                }
            },
        )

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
    input_data = input_type(**{**block_def.config, **merged})
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

    async def _run_one(src_parcel: Parcel) -> BlockOutput:
        async with sem:
            merged = _merge_upstream(
                block_uid, incoming_pipes, parcels,
                fan_out_parcel=src_parcel,
                fan_out_source_uid=fan_out_source_uid,
            )
            inp = input_type(**{**block_def.config, **merged})
            return await block.execute(inp, ctx)

    # Process in batches to avoid creating thousands of coroutines at once
    all_results: list[BlockOutput] = []
    for i in range(0, len(fan_out_parcels), batch_size):
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

    input_data = input_type(**{**block_def.config, "items": all_items})
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
) -> None:
    """Write failure state and log for a block."""
    await db["runs"].update_one(
        {"_id": ObjectId(run_id)},
        {
            "$set": {
                f"block_states.{block_uid}.status": "failed",
                f"block_states.{block_uid}.error": str(exc),
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
        msg=f"Block failed: {exc}",
        duration_ms=elapsed_ms,
    )
    await db["run_logs"].insert_one(model_to_doc(log_entry))


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

    pipeline_doc = await db["pipelines"].find_one({"_id": run_doc["pipeline_id"]})
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

    try:
        result = await run_blocks(pipeline, run_id, db, lemming_id)
        await db["runs"].update_one(
            {"_id": ObjectId(run_id)},
            {
                "$set": {
                    "status": "completed",
                    "output": result,
                    "finished_at": datetime.now(UTC),
                }
            },
        )
        await _publish_status(db, run_id, "completed", lemming_id)
        return result

    except Exception as e:
        await db["runs"].update_one(
            {"_id": ObjectId(run_id)},
            {
                "$set": {
                    "status": "failed",
                    "error": str(e),
                    "finished_at": datetime.now(UTC),
                }
            },
        )
        await _publish_status(db, run_id, "failed", lemming_id)
        raise
