"""Inline pipeline execution — runs blocks directly in the API process.

No Redis or ARQ needed. Streams block-by-block progress as SSE events.
Intended for the UI editor's "Run" button during development.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import traceback
from collections import defaultdict, deque
from typing import Any, AsyncGenerator, get_args

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field
from starlette.responses import StreamingResponse

from llming_plumber.blocks.base import (
    BaseBlock,
    BlockContext,
    BlockInput,
    BlockOutput,
)
from llming_plumber.blocks.limits import (
    FAN_OUT_BATCH_SIZE,
    MAX_FAN_OUT_ITEMS,
    MAX_RUN_WALL_SECONDS,
    ResourceLimitError,
    check_list_size,
)
from llming_plumber.blocks.registry import BlockRegistry
from llming_plumber.models.pipeline import (
    BlockDefinition,
    PipeDefinition,
    PipelineDefinition,
)
from llming_plumber.models.parcel import Parcel
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
    blocks: list[BlockDefinition] = Field(default_factory=list)
    pipes: list[PipeDefinition] = Field(default_factory=list)


def _sse(event: str, data: dict[str, Any]) -> str:
    """Format a Server-Sent Event."""
    return f"event: {event}\ndata: {json.dumps(data, default=str)}\n\n"


async def _run_pipeline_stream(
    pipeline: PipelineDefinition,
) -> AsyncGenerator[str, None]:
    """Execute a pipeline inline and yield SSE events."""
    BlockRegistry.discover()

    try:
        order = topological_sort(pipeline.blocks, pipeline.pipes)
    except ValueError as e:
        yield _sse("error", {"message": str(e)})
        return

    yield _sse("start", {
        "blocks": order,
        "total": len(order),
    })

    block_map = {b.uid: b for b in pipeline.blocks}
    incoming_pipes: dict[str, list[PipeDefinition]] = defaultdict(list)
    for pipe in pipeline.pipes:
        incoming_pipes[pipe.target_block_uid].append(pipe)

    parcels: dict[str, list[Parcel]] = {}
    run_start = time.monotonic()

    for i, block_uid in enumerate(order):
        elapsed_total = time.monotonic() - run_start
        if elapsed_total > MAX_RUN_WALL_SECONDS:
            yield _sse("error", {
                "block_uid": block_uid,
                "message": f"Wall-clock limit exceeded ({MAX_RUN_WALL_SECONDS}s)",
            })
            return

        block_def = block_map[block_uid]
        yield _sse("block_start", {
            "block_uid": block_uid,
            "block_type": block_def.block_type,
            "label": block_def.label,
            "index": i,
        })

        start = time.monotonic()

        try:
            block = BlockRegistry.create(block_def.block_type)
            block_cls = type(block)
            input_type, _output_type = _get_input_output_types(block_cls)

            fan_out_field: str | None = getattr(block_cls, "fan_out_field", None)
            is_fan_in: bool = getattr(block_cls, "fan_in", False)

            ctx = BlockContext(
                run_id="inline",
                pipeline_id="inline",
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
                # Check for fan-out upstream
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

            # Truncate output for SSE (keep it reasonable)
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
            return

    total_ms = (time.monotonic() - run_start) * 1000
    # Final output from the last block
    final_output = {}
    if order:
        last_parcels = parcels.get(order[-1], [])
        if last_parcels:
            final_output = _truncate_output(last_parcels[0].fields, max_str=2000)

    yield _sse("done", {
        "total_ms": round(total_ms, 1),
        "blocks_run": len(order),
        "output": final_output,
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
    # Strip class prefixes like "httpx.HTTPStatusError: ..."
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
    """Execute a pipeline inline (no DB, no Redis) and stream results via SSE."""
    pipeline = PipelineDefinition(
        name=body.name,
        blocks=body.blocks,
        pipes=body.pipes,
    )
    return StreamingResponse(
        _run_pipeline_stream(pipeline),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
