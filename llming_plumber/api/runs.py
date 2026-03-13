from __future__ import annotations

import asyncio
import base64
import logging
from datetime import UTC, datetime
from typing import Any

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response
from motor.motor_asyncio import AsyncIOMotorDatabase

from llming_plumber.api.deps import get_arq_pool, get_db
from llming_plumber.models.log import RunLog
from llming_plumber.models.mongo_helpers import doc_to_model, model_to_doc
from llming_plumber.models.run import Run, RunStatus

router = APIRouter()


@router.get("")
async def list_runs(
    status: RunStatus | None = Query(None),
    pipeline_id: str | None = Query(None),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    db: AsyncIOMotorDatabase = Depends(get_db),  # type: ignore[type-arg]
) -> list[dict[str, Any]]:
    """List runs with optional filtering by status and pipeline_id."""
    query: dict[str, Any] = {}
    if status is not None:
        query["status"] = status.value
    if pipeline_id is not None:
        query["pipeline_id"] = pipeline_id

    cursor = db["runs"].find(query).sort("created_at", -1).skip(skip).limit(limit)
    results: list[dict[str, Any]] = []
    async for doc in cursor:
        run = doc_to_model(doc, Run).model_dump(mode="json")
        # Strip output_summary from inline log entries for list view (keep labels + status)
        for entry in run.get("log", []):
            entry.pop("output_summary", None)
        # Strip block_states from list view (detailed, use run detail endpoint)
        run.pop("block_states", None)
        results.append(run)
    return results


@router.get("/{run_id}")
async def get_run(
    run_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db),  # type: ignore[type-arg]
) -> dict[str, Any]:
    """Get run detail including block_states."""
    doc = await db["runs"].find_one({"_id": ObjectId(run_id)})
    if doc is None:
        raise HTTPException(status_code=404, detail="Run not found")
    return doc_to_model(doc, Run).model_dump(mode="json")


@router.get("/{run_id}/logs")
async def get_run_logs(
    run_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db),  # type: ignore[type-arg]
) -> list[dict[str, Any]]:
    """Get RunLog entries for a run, sorted by timestamp."""
    cursor = db["run_logs"].find({"run_id": run_id}).sort("ts", 1)
    results: list[dict[str, Any]] = []
    async for doc in cursor:
        results.append(doc_to_model(doc, RunLog).model_dump(mode="json"))
    return results


@router.post("/{run_id}/cancel")
async def cancel_run(
    run_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db),  # type: ignore[type-arg]
) -> dict[str, str]:
    """Cancel a run. Only works if status is queued or running."""
    result = await db["runs"].find_one_and_update(
        {
            "_id": ObjectId(run_id),
            "status": {"$in": [RunStatus.queued.value, RunStatus.running.value]},
        },
        {
            "$set": {
                "status": RunStatus.cancelled.value,
                "finished_at": datetime.now(UTC),
            }
        },
    )
    if result is None:
        raise HTTPException(
            status_code=400,
            detail=(
                "Run not found or not in a cancellable"
                " state (must be queued or running)"
            ),
        )
    return {"run_id": run_id, "status": "cancelled"}


@router.post("/{run_id}/retry", status_code=201)
async def retry_run(
    run_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db),  # type: ignore[type-arg]
    arq_pool: Any = Depends(get_arq_pool),
) -> dict[str, Any]:
    """Retry a failed run by creating a new Run doc and re-enqueuing."""
    original = await db["runs"].find_one({"_id": ObjectId(run_id)})
    if original is None:
        raise HTTPException(status_code=404, detail="Run not found")
    if original.get("status") != RunStatus.failed.value:
        raise HTTPException(status_code=400, detail="Only failed runs can be retried")

    new_run = Run(
        pipeline_id=original["pipeline_id"],
        pipeline_version=original.get("pipeline_version", 1),
        status=RunStatus.queued,
        input=original.get("input", {}),
        attempt=original.get("attempt", 0) + 1,
        max_attempts=original.get("max_attempts", 3),
        tags=original.get("tags", []),
    )
    new_doc = model_to_doc(new_run)
    new_doc.pop("_id", None)
    result = await db["runs"].insert_one(new_doc)
    new_run_id = str(result.inserted_id)

    # Mark original as retrying
    await db["runs"].update_one(
        {"_id": ObjectId(run_id)},
        {"$set": {"status": RunStatus.retrying.value}},
    )

    # Enqueue via ARQ
    await arq_pool.enqueue_job("execute_pipeline", run_id=new_run_id)

    return {"run_id": new_run_id, "status": "queued"}


@router.post("/{run_id}/resume", status_code=200)
async def resume_run(
    run_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db),  # type: ignore[type-arg]
) -> dict[str, Any]:
    """Resume a failed/cancelled run from the last completed block.

    Completed blocks are skipped and their outputs reused.  The executor
    re-runs from the first incomplete block.  Blocks with internal
    checkpoints (e.g. web_crawler) resume their own progress too.
    """
    doc = await db["runs"].find_one({"_id": ObjectId(run_id)})
    if doc is None:
        raise HTTPException(status_code=404, detail="Run not found")
    if doc.get("status") not in ("failed", "cancelled"):
        raise HTTPException(
            status_code=400,
            detail="Only failed or cancelled runs can be resumed",
        )

    # Reset failed block states so they get re-executed
    updates: dict[str, Any] = {
        "status": "queued",
        "error": None,
        "finished_at": None,
        "current_block": None,
    }
    for uid, state in (doc.get("block_states") or {}).items():
        if isinstance(state, dict) and state.get("status") == "failed":
            updates[f"block_states.{uid}.status"] = ""
            updates[f"block_states.{uid}.error"] = None

    await db["runs"].update_one(
        {"_id": ObjectId(run_id)},
        {"$set": updates},
    )

    # Dispatch inline
    asyncio.create_task(_resume_bg(run_id, db))
    return {"run_id": run_id, "status": "queued", "resumed": True}


async def _resume_bg(
    run_id: str,
    db: AsyncIOMotorDatabase,  # type: ignore[type-arg]
) -> None:
    """Background task: resume a pipeline run in-process."""
    from llming_plumber.worker.executor import execute_pipeline

    ctx: dict[str, Any] = {"db": db, "lemming_id": "inline"}
    try:
        await execute_pipeline(ctx, run_id=run_id)
    except Exception:
        logging.getLogger(__name__).exception(
            "Resume failed for run %s", run_id,
        )


@router.get("/{run_id}/blocks/{block_uid}/download")
async def download_block_output(
    run_id: str,
    block_uid: str,
    db: AsyncIOMotorDatabase = Depends(get_db),  # type: ignore[type-arg]
) -> Response:
    """Download a block's output as a file.

    Supports blocks that produce ``archive_base64`` (zip archives),
    ``file_ref`` (FileRef objects), or ``content`` (raw text).
    Falls back to JSON dump of the full output.
    """
    doc = await db["runs"].find_one({"_id": ObjectId(run_id)})
    if doc is None:
        raise HTTPException(status_code=404, detail="Run not found")

    run = doc_to_model(doc, Run)
    block_state = run.block_states.get(block_uid)
    if block_state is None or block_state.output is None:
        raise HTTPException(
            status_code=404,
            detail=f"No output for block '{block_uid}' in this run",
        )

    output = block_state.output

    # Zip archive (zip_create, etc.)
    if output.get("archive_base64"):
        raw = base64.b64decode(output["archive_base64"])
        filename = output.get("blob_path", "").rsplit("/", 1)[-1] or "archive.zip"
        if not filename.endswith(".zip"):
            filename += ".zip"
        return Response(
            content=raw,
            media_type="application/zip",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    # FileRef
    if output.get("file_ref"):
        from llming_plumber.models.file_ref import FileRef

        ref = FileRef(**output["file_ref"])
        raw = ref.decode()
        return Response(
            content=raw,
            media_type=ref.mime_type,
            headers={
                "Content-Disposition": f'attachment; filename="{ref.filename}"',
            },
        )

    # Azure blob output — redirect to blob URL
    if output.get("url") and output.get("blob_name"):
        import json

        # Return metadata as JSON (user can use the URL to download)
        return Response(
            content=json.dumps(output, indent=2),
            media_type="application/json",
            headers={
                "Content-Disposition": (
                    f'attachment; filename="{output["blob_name"].rsplit("/", 1)[-1]}.json"'
                ),
            },
        )

    # Fallback: dump output as JSON
    import json

    raw = json.dumps(output, ensure_ascii=False, indent=2).encode()
    return Response(
        content=raw,
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="{block_uid}_output.json"'},
    )
