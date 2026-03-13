from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

from bson import ObjectId
from fastapi import APIRouter, Body, Depends, HTTPException, Query
from motor.motor_asyncio import AsyncIOMotorDatabase

from llming_plumber.api.deps import get_arq_pool, get_current_user, get_db, require_pipeline_access
from llming_plumber.blocks.registry import BlockRegistry
from llming_plumber.models.mongo_helpers import doc_to_model, model_to_doc
from llming_plumber.models.pipeline import PipelineDefinition
from llming_plumber.models.run import Run, RunStatus

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("")
async def list_pipelines(
    owner_id: str | None = Query(None),
    tag: str | None = Query(None),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    db: AsyncIOMotorDatabase = Depends(get_db),  # type: ignore[type-arg]
) -> list[dict[str, Any]]:
    """List pipelines enriched with schedule and latest run info."""
    query: dict[str, Any] = {}
    if owner_id is not None:
        query["owner_id"] = owner_id
    if tag is not None:
        query["tags"] = tag

    cursor = db["pipelines"].find(query).skip(skip).limit(limit)
    results: list[dict[str, Any]] = []
    pipeline_ids: list[str] = []
    async for doc in cursor:
        p = doc_to_model(doc, PipelineDefinition).model_dump(mode="json")
        results.append(p)
        pipeline_ids.append(p["id"])

    if not results:
        return results

    # Batch-fetch schedules for all pipelines
    schedule_map: dict[str, dict[str, Any]] = {}
    async for sdoc in db["schedules"].find({
        "pipeline_id": {"$in": pipeline_ids},
        "enabled": True,
    }):
        pid = sdoc["pipeline_id"]
        schedule_map[pid] = {
            "enabled": True,
            "interval_seconds": sdoc.get("interval_seconds"),
            "cron_expression": sdoc.get("cron_expression"),
            "next_run_at": sdoc.get("next_run_at", ""),
        }

    # Batch-fetch latest run per pipeline (one query with aggregation)
    latest_runs: dict[str, dict[str, Any]] = {}
    pipeline = [
        {"$match": {"pipeline_id": {"$in": pipeline_ids}}},
        {"$sort": {"created_at": -1}},
        {"$group": {
            "_id": "$pipeline_id",
            "status": {"$first": "$status"},
            "created_at": {"$first": "$created_at"},
            "started_at": {"$first": "$started_at"},
            "finished_at": {"$first": "$finished_at"},
            "run_id": {"$first": {"$toString": "$_id"}},
            "log": {"$first": "$log"},
            "error": {"$first": "$error"},
        }},
    ]
    async for rdoc in db["runs"].aggregate(pipeline):
        pid = rdoc["_id"]
        started = rdoc.get("started_at")
        finished = rdoc.get("finished_at")
        duration_ms = None
        if started and finished:
            try:
                duration_ms = int((finished - started).total_seconds() * 1000)
            except Exception:
                pass
        # Compact the block log for the list view (strip output_summary)
        raw_log = rdoc.get("log") or []
        compact_log = [
            {
                "uid": e.get("uid", ""),
                "block_type": e.get("block_type", ""),
                "label": e.get("label", ""),
                "status": e.get("status", ""),
                "duration_ms": e.get("duration_ms", 0),
                "parcel_count": e.get("parcel_count", 0),
                "error": e.get("error"),
                "output_summary": e.get("output_summary", {}),
            }
            for e in raw_log
        ]
        def _iso(dt: Any) -> str:
            """Format datetime as ISO with UTC timezone for JS parsing."""
            if not dt:
                return ""
            if hasattr(dt, "isoformat"):
                s = dt.isoformat()
                if "+" not in s and "Z" not in s:
                    s += "+00:00"
                return s
            return str(dt)

        latest_runs[pid] = {
            "run_id": rdoc.get("run_id"),
            "status": rdoc.get("status"),
            "created_at": _iso(rdoc.get("created_at")),
            "started_at": _iso(rdoc.get("started_at")),
            "finished_at": _iso(rdoc.get("finished_at")),
            "duration_ms": duration_ms,
            "log": compact_log,
            "error": rdoc.get("error"),
        }

    # Enrich each pipeline
    for p in results:
        pid = p["id"]
        p["schedule"] = schedule_map.get(pid)
        p["latest_run"] = latest_runs.get(pid)

    return results


@router.post("", status_code=201)
async def create_pipeline(
    pipeline: PipelineDefinition,
    db: AsyncIOMotorDatabase = Depends(get_db),  # type: ignore[type-arg]
    user: str = Depends(get_current_user),
) -> dict[str, Any]:
    """Create a new pipeline definition. Validates that all block_types exist."""
    for block in pipeline.blocks:
        try:
            BlockRegistry.get(block.block_type)
        except KeyError:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown block_type: {block.block_type}",
            )

    pipeline.owner_id = user
    doc = model_to_doc(pipeline)
    doc.pop("_id", None)  # let MongoDB generate _id
    result = await db["pipelines"].insert_one(doc)
    pipeline.id = str(result.inserted_id)

    await _sync_timer_schedule(db, pipeline.id, pipeline.blocks)

    return pipeline.model_dump(mode="json")


@router.get("/{pipeline_id}")
async def get_pipeline(
    pipeline_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db),  # type: ignore[type-arg]
) -> dict[str, Any]:
    """Get a single pipeline by ID."""
    doc = await db["pipelines"].find_one({"_id": ObjectId(pipeline_id)})
    if doc is None:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    return doc_to_model(doc, PipelineDefinition).model_dump(mode="json")


@router.put("/{pipeline_id}")
async def update_pipeline(
    pipeline_id: str,
    pipeline: PipelineDefinition,
    db: AsyncIOMotorDatabase = Depends(get_db),  # type: ignore[type-arg]
    user: str = Depends(get_current_user),
) -> dict[str, Any]:
    """Update a pipeline, bumping the version."""
    existing = await require_pipeline_access(pipeline_id, db, user)

    for block in pipeline.blocks:
        try:
            BlockRegistry.get(block.block_type)
        except KeyError:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown block_type: {block.block_type}",
            )

    doc = model_to_doc(pipeline)
    doc.pop("_id", None)
    doc["version"] = existing.get("version", 1) + 1
    doc["updated_at"] = datetime.now(UTC)
    doc["owner_id"] = existing.get("owner_id", user)  # preserve original owner

    await db["pipelines"].replace_one({"_id": ObjectId(pipeline_id)}, doc)

    # Auto-manage schedule from timer_trigger config
    await _sync_timer_schedule(db, pipeline_id, pipeline.blocks)

    doc["_id"] = ObjectId(pipeline_id)
    return doc_to_model(doc, PipelineDefinition).model_dump(mode="json")


@router.patch("/{pipeline_id}/blocks/{block_uid}/disabled")
async def set_block_disabled(
    pipeline_id: str,
    block_uid: str,
    body: dict[str, Any] = Body(...),
    db: AsyncIOMotorDatabase = Depends(get_db),  # type: ignore[type-arg]
    user: str = Depends(get_current_user),
) -> dict[str, Any]:
    """Toggle a block's disabled state without saving the entire pipeline."""
    await require_pipeline_access(pipeline_id, db, user)
    disabled = bool(body.get("disabled", False))

    result = await db["pipelines"].update_one(
        {"_id": ObjectId(pipeline_id), "blocks.uid": block_uid},
        {"$set": {
            "blocks.$.disabled": disabled,
            "updated_at": datetime.now(UTC),
        }},
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Block not found in pipeline")

    # Re-sync timer schedule if the toggled block is a timer_trigger
    pipeline_doc = await db["pipelines"].find_one({"_id": ObjectId(pipeline_id)})
    if pipeline_doc:
        await _sync_timer_schedule(db, pipeline_id, pipeline_doc.get("blocks", []))

    return {"block_uid": block_uid, "disabled": disabled}


@router.delete("/{pipeline_id}", status_code=204)
async def delete_pipeline(
    pipeline_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db),  # type: ignore[type-arg]
    user: str = Depends(get_current_user),
) -> None:
    """Delete a pipeline by ID."""
    await require_pipeline_access(pipeline_id, db, user)
    await db["pipelines"].delete_one({"_id": ObjectId(pipeline_id)})


@router.post("/{pipeline_id}/run", status_code=201)
async def run_pipeline(
    pipeline_id: str,
    body: dict[str, Any] = Body(default={}),
    db: AsyncIOMotorDatabase = Depends(get_db),  # type: ignore[type-arg]
    user: str = Depends(get_current_user),
) -> dict[str, Any]:
    """Create a Run document and dispatch execution."""
    pipeline_doc = await require_pipeline_access(pipeline_id, db, user)

    if not pipeline_doc.get("enabled", True):
        raise HTTPException(status_code=409, detail="Pipeline is disabled")

    run = Run(
        pipeline_id=pipeline_id,
        pipeline_version=pipeline_doc.get("version", 1),
        status=RunStatus.queued,
        input=body,
    )
    run_doc = model_to_doc(run)
    run_doc.pop("_id", None)
    result = await db["runs"].insert_one(run_doc)
    run_id = str(result.inserted_id)

    # Execute inline (in-process) — no separate ARQ worker needed
    asyncio.create_task(_run_inline_bg(run_id, db))

    return {"run_id": run_id, "status": "queued", "dispatched_via": "inline"}


async def _run_inline_bg(run_id: str, db: AsyncIOMotorDatabase) -> None:  # type: ignore[type-arg]
    """Background task: run a pipeline in-process (dev mode fallback).

    Mirrors what the ARQ worker does but runs inside the API process.
    """
    from llming_plumber.worker.executor import execute_pipeline

    ctx: dict[str, Any] = {"db": db, "lemming_id": "inline"}
    try:
        await execute_pipeline(ctx, run_id=run_id)
    except Exception:
        logger.exception("Inline pipeline execution failed for run %s", run_id)


async def _sync_timer_schedule(
    db: AsyncIOMotorDatabase,  # type: ignore[type-arg]
    pipeline_id: str,
    blocks: list[Any],
) -> None:
    """Create or update a schedule based on the timer_trigger block config.

    If the pipeline has a timer_trigger with interval_seconds > 0 or a
    cron_expression, ensure a matching schedule exists. If the trigger
    config is removed or set to 0, disable the schedule.
    """
    timer_block = None
    for b in blocks:
        bt = b.block_type if hasattr(b, "block_type") else b.get("block_type", "")
        if bt == "timer_trigger":
            timer_block = b
            break

    config = {}
    block_disabled = False
    if timer_block is not None:
        config = timer_block.config if hasattr(timer_block, "config") else timer_block.get("config", {})
        block_disabled = (
            timer_block.disabled if hasattr(timer_block, "disabled")
            else timer_block.get("disabled", False)
        )

    interval = int(config.get("interval_seconds", 0) or 0)
    cron_expr = config.get("cron_expression", "").strip() or None

    # Disabled block → disable the schedule
    if block_disabled:
        interval = 0
        cron_expr = None

    existing = await db["schedules"].find_one({
        "pipeline_id": pipeline_id,
        "tags": "_auto_timer",
    })

    if interval <= 0 and not cron_expr:
        # No scheduling needed — disable if exists
        if existing:
            await db["schedules"].update_one(
                {"_id": existing["_id"]},
                {"$set": {"enabled": False}},
            )
        return

    from llming_plumber.worker.scheduler import compute_next_run

    now = datetime.now(UTC)

    # For cron schedules, compute the next real cron slot instead of
    # triggering immediately.  Interval schedules fire after one interval.
    if cron_expr:
        next_run = compute_next_run(cron_expr, now)
    elif interval > 0:
        next_run = now
    else:
        next_run = now

    schedule_doc: dict[str, Any] = {
        "pipeline_id": pipeline_id,
        "enabled": True,
        "tags": ["_auto_timer"],
        "interval_seconds": interval if interval > 0 else None,
        "cron_expression": cron_expr,
        "next_run_at": next_run,
        "updated_at": now,
    }

    # Add time windows from the trigger config
    tw_start = config.get("time_window_start", "").strip()
    tw_end = config.get("time_window_end", "").strip()
    weekdays_str = config.get("weekdays", "").strip()
    if tw_start and tw_end:
        weekday_map = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}
        weekdays = [0, 1, 2, 3, 4, 5, 6]
        if weekdays_str:
            weekdays = [weekday_map.get(d.strip().lower()[:3], -1) for d in weekdays_str.split(",")]
            weekdays = [d for d in weekdays if d >= 0]
        schedule_doc["time_windows"] = [{"start": tw_start, "end": tw_end, "weekdays": weekdays}]

    if existing:
        await db["schedules"].update_one(
            {"_id": existing["_id"]},
            {"$set": schedule_doc},
        )
    else:
        schedule_doc["created_at"] = now
        await db["schedules"].insert_one(schedule_doc)
