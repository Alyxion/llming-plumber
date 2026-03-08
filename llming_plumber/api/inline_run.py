"""Inline pipeline execution — runs blocks directly in the API process.

Persists Run + RunLog records to MongoDB (same as the ARQ worker path)
so runs appear in history. Streams block-by-block progress as SSE events.
"""

from __future__ import annotations

import json
import logging
import time
import traceback
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any, AsyncGenerator

from fastapi import APIRouter
from pydantic import BaseModel, Field
from starlette.responses import StreamingResponse

from llming_plumber.blocks.base import (
    BlockContext,
    BlockOutput,
)
from llming_plumber.blocks.limits import (
    LOG_BLOCK_OUTPUT,
    MAX_ERROR_MESSAGE_LENGTH,
    MAX_FAN_OUT_ITEMS,
    MAX_RUN_LOG_ENTRIES,
    MAX_RUN_WALL_SECONDS,
    check_list_size,
)
from llming_plumber.blocks.registry import BlockRegistry
from llming_plumber.db import get_database
from llming_plumber.models.log import RunLog
from llming_plumber.models.mongo_helpers import model_to_doc
from llming_plumber.models.parcel import Parcel
from llming_plumber.models.pipeline import (
    PipeDefinition,
    PipelineDefinition,
)
from llming_plumber.models.run import BlockState, Run, RunStatus
from llming_plumber.worker.executor import (
    topological_sort,
    _apply_pipe_mapping,
    _merge_upstream,
    _get_input_output_types,
)

logger = logging.getLogger(__name__)
router = APIRouter()


class InlineRunRequest(BaseModel):
    """Payload for inline pipeline execution."""

    name: str = "Untitled"
    pipeline_id: str | None = None
    blocks: list[Any] = Field(default_factory=list)
    pipes: list[Any] = Field(default_factory=list)


def _sse(event: str, data: dict[str, Any]) -> str:
    """Format a Server-Sent Event."""
    return f"event: {event}\ndata: {json.dumps(data, default=str)}\n\n"


async def _run_pipeline_stream(
    pipeline: PipelineDefinition,
    pipeline_id: str | None = None,
) -> AsyncGenerator[str, None]:
    """Execute a pipeline inline and yield SSE events.

    Also persists a Run document + RunLog entries to MongoDB.
    """
    BlockRegistry.discover()
    db = get_database()

    try:
        order = topological_sort(pipeline.blocks, pipeline.pipes)
    except ValueError as e:
        yield _sse("error", {"message": str(e)})
        return

    # Create Run record
    run = Run(
        pipeline_id=pipeline_id or "inline",
        pipeline_version=1,
        status=RunStatus.running,
        started_at=datetime.now(UTC),
        lemming_id="inline",
    )
    run_doc = model_to_doc(run)
    run_doc.pop("_id", None)
    result = await db["runs"].insert_one(run_doc)
    run_id = str(result.inserted_id)

    yield _sse("start", {
        "run_id": run_id,
        "blocks": order,
        "total": len(order),
    })

    block_map = {b.uid: b for b in pipeline.blocks}
    incoming_pipes: dict[str, list[PipeDefinition]] = defaultdict(list)
    for pipe in pipeline.pipes:
        incoming_pipes[pipe.target_block_uid].append(pipe)

    parcels: dict[str, list[Parcel]] = {}
    run_start = time.monotonic()
    block_states: dict[str, BlockState] = {}
    final_status = RunStatus.completed
    error_message: str | None = None
    log_count = 0
    total_blocks = len(order)

    for i, block_uid in enumerate(order):
        elapsed_total = time.monotonic() - run_start
        if elapsed_total > MAX_RUN_WALL_SECONDS:
            error_message = f"Wall-clock limit exceeded ({MAX_RUN_WALL_SECONDS}s)"
            final_status = RunStatus.failed
            yield _sse("error", {
                "block_uid": block_uid,
                "message": error_message,
            })
            break

        block_def = block_map[block_uid]
        yield _sse("block_start", {
            "block_uid": block_uid,
            "block_type": block_def.block_type,
            "label": block_def.label,
            "index": i,
        })

        # Update current_block in DB
        await db["runs"].update_one(
            {"_id": result.inserted_id},
            {"$set": {"current_block": block_uid}},
        )

        start = time.monotonic()

        try:
            block = BlockRegistry.create(block_def.block_type)
            block_cls = type(block)
            input_type, _output_type = _get_input_output_types(block_cls)

            fan_out_field: str | None = getattr(block_cls, "fan_out_field", None)
            is_fan_in: bool = getattr(block_cls, "fan_in", False)

            ctx = BlockContext(
                run_id=run_id,
                pipeline_id=pipeline_id or "inline",
                block_id=block_uid,
            )

            if is_fan_in:
                all_items: list[dict[str, Any]] = []
                for pipe in incoming_pipes.get(block_uid, []):
                    for p in parcels.get(pipe.source_block_uid, []):
                        all_items.append(_apply_pipe_mapping(pipe, p))
                input_data = input_type(**{**block_def.config, "items": all_items})
                output = await block.execute(input_data, ctx)
                output_dict = output.model_dump()
                parcels[block_uid] = [Parcel(uid=block_uid, fields=output_dict)]
            else:
                fan_out_source_uid: str | None = None
                fan_out_parcel_list: list[Parcel] = []
                for pipe in incoming_pipes.get(block_uid, []):
                    src_list = parcels.get(pipe.source_block_uid, [])
                    if len(src_list) > 1:
                        fan_out_source_uid = pipe.source_block_uid
                        fan_out_parcel_list = src_list
                        break

                if fan_out_parcel_list:
                    check_list_size(
                        fan_out_parcel_list,
                        limit=MAX_FAN_OUT_ITEMS,
                        label=f"Fan-out for block '{block_uid}'",
                    )
                    results: list[BlockOutput] = []
                    for src_parcel in fan_out_parcel_list:
                        merged = _merge_upstream(
                            block_uid, incoming_pipes, parcels,
                            fan_out_parcel=src_parcel,
                            fan_out_source_uid=fan_out_source_uid,
                        )
                        inp = input_type(**{**block_def.config, **merged})
                        results.append(await block.execute(inp, ctx))
                    parcels[block_uid] = [
                        Parcel(uid=block_uid, fields=r.model_dump()) for r in results
                    ]
                    output_dict = results[0].model_dump() if results else {}
                else:
                    merged = _merge_upstream(block_uid, incoming_pipes, parcels)
                    input_data = input_type(**{**block_def.config, **merged})
                    output = await block.execute(input_data, ctx)
                    output_dict = output.model_dump()

                    if fan_out_field and fan_out_field in output_dict:
                        items = output_dict[fan_out_field]
                        check_list_size(
                            items, limit=MAX_FAN_OUT_ITEMS,
                            label=f"Fan-out '{fan_out_field}' in '{block_uid}'",
                        )
                        parcels[block_uid] = [
                            Parcel(uid=block_uid, fields=(
                                item if isinstance(item, dict) else {"item": item}
                            ))
                            for item in items
                        ] or [Parcel(uid=block_uid, fields=output_dict)]
                    else:
                        parcels[block_uid] = [
                            Parcel(uid=block_uid, fields=output_dict),
                        ]

            elapsed_ms = (time.monotonic() - start) * 1000

            # Store output in block_states only for first/last block or when opted in
            is_boundary = (i == 0 or i == total_blocks - 1)
            store_output = LOG_BLOCK_OUTPUT or is_boundary

            block_states[block_uid] = BlockState(
                status="completed",
                output=_truncate_output(output_dict) if store_output else None,
                duration_ms=round(elapsed_ms, 1),
            )

            # Write RunLog entry (capped per run; always log first/last)
            if log_count < MAX_RUN_LOG_ENTRIES or is_boundary:
                log_entry = RunLog(
                    run_id=run_id,
                    block_id=block_uid,
                    block_type=block_def.block_type,
                    level="info",
                    msg=f"Completed {block_def.label} in {round(elapsed_ms, 1)}ms",
                    duration_ms=round(elapsed_ms, 1),
                    output_summary=(
                        _truncate_output(output_dict, max_str=200)
                        if store_output else None
                    ),
                )
                log_doc = model_to_doc(log_entry)
                log_doc.pop("_id", None)
                await db["run_logs"].insert_one(log_doc)
                log_count += 1

            output_preview = _truncate_output(output_dict)

            yield _sse("block_done", {
                "block_uid": block_uid,
                "block_type": block_def.block_type,
                "label": block_def.label,
                "duration_ms": round(elapsed_ms, 1),
                "parcel_count": len(parcels.get(block_uid, [])),
                "output": output_preview,
                "status": "completed",
            })

        except Exception as exc:
            elapsed_ms = (time.monotonic() - start) * 1000
            error_info = _humanize_error(exc, block_def.label)

            block_states[block_uid] = BlockState(
                status="failed",
                error=error_info["message"],
                duration_ms=round(elapsed_ms, 1),
            )

            # Write error RunLog entry (errors always logged, bypass cap)
            log_entry = RunLog(
                run_id=run_id,
                block_id=block_uid,
                block_type=block_def.block_type,
                level="error",
                msg=error_info["message"][:MAX_ERROR_MESSAGE_LENGTH],
                duration_ms=round(elapsed_ms, 1),
            )
            log_doc = model_to_doc(log_entry)
            log_doc.pop("_id", None)
            await db["run_logs"].insert_one(log_doc)

            final_status = RunStatus.failed
            error_message = error_info["message"]

            yield _sse("block_done", {
                "block_uid": block_uid,
                "block_type": block_def.block_type,
                "label": block_def.label,
                "duration_ms": round(elapsed_ms, 1),
                "status": "failed",
                "error": error_info["message"],
                "error_fields": error_info.get("fields", []),
            })
            yield _sse("error", {
                "block_uid": block_uid,
                "message": error_info["message"],
                "error_fields": error_info.get("fields", []),
                "traceback": traceback.format_exc()[-1000:],
            })
            break

    total_ms = (time.monotonic() - run_start) * 1000

    # Final output from the last block
    final_output = {}
    if order:
        last_parcels = parcels.get(order[-1], [])
        if last_parcels:
            final_output = _truncate_output(last_parcels[0].fields, max_str=2000)

    # Finalize Run record in DB
    await db["runs"].update_one(
        {"_id": result.inserted_id},
        {"$set": {
            "status": final_status.value,
            "finished_at": datetime.now(UTC),
            "block_states": {k: v.model_dump() for k, v in block_states.items()},
            "output": final_output if final_status == RunStatus.completed else None,
            "error": error_message,
            "current_block": None,
        }},
    )

    yield _sse("done", {
        "run_id": run_id,
        "total_ms": round(total_ms, 1),
        "blocks_run": len(order),
        "output": final_output,
        "status": final_status.value,
    })


def _humanize_error(exc: Exception, block_label: str) -> dict[str, Any]:
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
            elif err_type == "string_type":
                parts.append(f'"{field_name}" expects text but got a different type')
                hint = "Check that the upstream block outputs the correct type"
            elif "type" in err_type:
                expected = err.get("msg", "")
                parts.append(f'"{field_name}": {expected}')
                hint = "Check the field value or upstream connection"
            else:
                msg = err.get("msg", str(err_type))
                parts.append(f'"{field_name}": {msg}')
                hint = ""
            fields.append({"field": field_name, "message": parts[-1], "hint": hint})
        message = f"{block_label}: {'; '.join(parts)}"
        return {"message": message, "fields": fields}

    if isinstance(exc, ValueError):
        return {"message": f"{block_label}: {exc}"}

    msg = str(exc)[:300]
    if ": " in msg:
        msg = msg.split(": ", 1)[1]
    return {"message": f"{block_label}: {msg}"}


def _truncate_output(d: dict[str, Any], max_str: int = 500) -> dict[str, Any]:
    """Truncate large values for SSE preview."""
    result: dict[str, Any] = {}
    for k, v in d.items():
        if isinstance(v, str) and len(v) > max_str:
            result[k] = v[:max_str] + f"... ({len(v)} chars)"
        elif isinstance(v, list) and len(v) > 10:
            result[k] = v[:10]
            result[f"_{k}_total"] = len(v)
        elif isinstance(v, dict) and len(str(v)) > max_str:
            result[k] = {kk: "..." for kk in list(v.keys())[:10]}
        else:
            result[k] = v
    return result


@router.post("/run-inline")
async def run_inline(body: InlineRunRequest) -> StreamingResponse:
    """Execute a pipeline inline and stream results via SSE.

    Creates a Run record in MongoDB so the run appears in history.
    """
    from llming_plumber.models.pipeline import BlockDefinition

    pipeline = PipelineDefinition(
        name=body.name,
        blocks=[BlockDefinition(**b) if isinstance(b, dict) else b for b in body.blocks],
        pipes=[PipeDefinition(**p) if isinstance(p, dict) else p for p in body.pipes],
    )
    return StreamingResponse(
        _run_pipeline_stream(pipeline, pipeline_id=body.pipeline_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
