"""Pipeline execution engine — topological sort, block runner, ARQ task."""

from __future__ import annotations

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


async def run_blocks(
    pipeline: PipelineDefinition,
    run_id: str,
    db: Any,
    lemming_id: str,
) -> dict[str, Any]:
    """Execute all blocks in topological order, piping data between them.

    Returns the output of the final block as a dict.
    """
    BlockRegistry.discover()

    order = topological_sort(pipeline.blocks, pipeline.pipes)

    block_map: dict[str, BlockDefinition] = {b.uid: b for b in pipeline.blocks}

    # Build a lookup: target_block_uid -> list of pipes feeding into it
    incoming_pipes: dict[str, list[PipeDefinition]] = defaultdict(list)
    for pipe in pipeline.pipes:
        incoming_pipes[pipe.target_block_uid].append(pipe)

    # Parcel store: block_uid -> Parcel produced by that block
    parcels: dict[str, Parcel] = {}

    last_output: dict[str, Any] = {}

    for block_uid in order:
        block_def = block_map[block_uid]
        block = BlockRegistry.create(block_def.block_type)
        block_cls = type(block)
        input_type, _output_type = _get_input_output_types(block_cls)

        # Update run: current_block
        await db["runs"].update_one(
            {"_id": ObjectId(run_id)},
            {"$set": {
                f"block_states.{block_uid}.status": "running",
                "current_block": block_uid,
            }},
        )

        # Collect input fields from upstream parcels
        merged_fields: dict[str, Any] = {}
        for pipe in incoming_pipes.get(block_uid, []):
            source_parcel = parcels.get(pipe.source_block_uid)
            if source_parcel is None:
                continue
            if pipe.field_mapping:
                for target_field, source_field in pipe.field_mapping.items():
                    if source_field in source_parcel.fields:
                        merged_fields[target_field] = source_parcel.fields[source_field]
            else:
                merged_fields.update(source_parcel.fields)

        # Build input: config values as defaults, piped fields override
        input_data = input_type(**{**block_def.config, **merged_fields})

        ctx = BlockContext(
            run_id=run_id,
            pipeline_id=pipeline.id,
            block_id=block_uid,
        )

        start = time.monotonic()
        try:
            output = await block.execute(input_data, ctx)
        except Exception as exc:
            elapsed_ms = (time.monotonic() - start) * 1000

            # Write block failure state
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

            # Write failure log
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

            raise

        elapsed_ms = (time.monotonic() - start) * 1000

        # Convert output to parcel
        output_dict = output.model_dump()
        parcel = Parcel(uid=block_uid, fields=output_dict)
        parcels[block_uid] = parcel
        last_output = output_dict

        # Write RunLog entry
        log_entry = RunLog(
            run_id=run_id,
            lemming_id=lemming_id,
            block_id=block_uid,
            block_type=block_def.block_type,
            level="info",
            msg="Block completed",
            duration_ms=elapsed_ms,
            output_summary=output_dict,
        )
        await db["run_logs"].insert_one(model_to_doc(log_entry))

        # Update block state in run doc
        await db["runs"].update_one(
            {"_id": ObjectId(run_id)},
            {
                "$set": {
                    f"block_states.{block_uid}.status": "completed",
                    f"block_states.{block_uid}.output": output_dict,
                    f"block_states.{block_uid}.duration_ms": elapsed_ms,
                }
            },
        )

    return last_output


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
