"""File browser API — browse and preview files from storage backends.

Supports any block that writes to a sink (Azure Blob, etc.).
The file browser uses sink metadata stored in ``block_states`` to locate
files and the resource block's connection string from the pipeline config.
"""

from __future__ import annotations

import os
from typing import Any

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response
from motor.motor_asyncio import AsyncIOMotorDatabase

from llming_plumber.api.deps import get_db

router = APIRouter()

# Maximum raw content size we'll serve inline (512 KB)
_MAX_PREVIEW_BYTES = 512 * 1024


def _get_connection_string(
    pipeline_doc: dict[str, Any],
    resource_block_uid: str,
) -> str:
    """Extract the connection string for a resource block from pipeline config."""
    for block in pipeline_doc.get("blocks", []):
        if block.get("uid") == resource_block_uid:
            cs = block.get("config", {}).get("connection_string", "")
            if cs:
                return cs
    return os.environ.get("AZURE_STORAGE_CONNECTION_STRING", "")


async def _get_blob_service(connection_string: str) -> Any:
    from azure.storage.blob.aio import BlobServiceClient
    return BlobServiceClient.from_connection_string(connection_string)


def _find_resource_block_uid(
    pipeline_doc: dict[str, Any],
    action_block_uid: str,
) -> str | None:
    """Find the resource block connected to an action block."""
    for pipe in pipeline_doc.get("pipes", []):
        if pipe.get("source_block_uid") == action_block_uid:
            target_uid = pipe.get("target_block_uid", "")
            for block in pipeline_doc.get("blocks", []):
                if block.get("uid") == target_uid and block.get("block_type", "").endswith("_resource"):
                    return target_uid
    return None


def _resolve_sink_info(
    run_doc: dict[str, Any],
    pipeline_doc: dict[str, Any],
    block_uid: str,
) -> tuple[str, str, str]:
    """Resolve container, base_path, and connection_string for a block's files.

    Checks both the action block's sink_* metadata and the resource block's config.
    Returns (container, base_path, connection_string).
    """
    bs = run_doc.get("block_states", {}).get(block_uid, {})

    # Case 1: action block with sink metadata
    container = bs.get("sink_container", "")
    base_path = bs.get("sink_base_path", "")
    if container:
        resource_uid = _find_resource_block_uid(pipeline_doc, block_uid)
        conn_str = _get_connection_string(pipeline_doc, resource_uid or "")
        return container, base_path, conn_str

    # Case 2: resource block itself
    rc = bs.get("resource_config", {})
    if rc.get("container"):
        # Find which action blocks connect TO this resource
        conn_str = _get_connection_string(pipeline_doc, block_uid)
        return rc["container"], rc.get("base_path", ""), conn_str

    raise HTTPException(
        status_code=404,
        detail=f"No file storage info found for block '{block_uid}' in this run",
    )


def _find_action_blocks_for_resource(
    pipeline_doc: dict[str, Any],
    resource_uid: str,
) -> list[str]:
    """Find all action block UIDs that pipe into a resource block."""
    return [
        pipe["source_block_uid"]
        for pipe in pipeline_doc.get("pipes", [])
        if pipe.get("target_block_uid") == resource_uid
    ]


@router.get("/pipelines/{pipeline_id}/blocks/{block_uid}/versions")
async def list_block_versions(
    pipeline_id: str,
    block_uid: str,
    limit: int = Query(20, ge=1, le=100),
    db: AsyncIOMotorDatabase = Depends(get_db),  # type: ignore[type-arg]
) -> list[dict[str, Any]]:
    """List past runs where a block produced files (has sink metadata).

    For resource blocks, aggregates file counts from all action blocks that
    pipe into this resource.
    """
    # Pipeline _id may be ObjectId or string — try both
    pipeline_doc = await db["pipelines"].find_one({"_id": pipeline_id})
    if pipeline_doc is None:
        try:
            pipeline_doc = await db["pipelines"].find_one(
                {"_id": ObjectId(pipeline_id)},
            )
        except Exception:
            pass
    if pipeline_doc is None:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    # Determine which block UIDs to search for sink metadata.
    # For resource blocks, look at the connected action blocks.
    action_uids = _find_action_blocks_for_resource(pipeline_doc, block_uid)
    search_uids = action_uids if action_uids else [block_uid]

    # Build $or conditions: any of these blocks having sink metadata
    or_conditions: list[dict[str, Any]] = []
    for uid in search_uids:
        or_conditions.append(
            {f"block_states.{uid}.sink_container": {"$exists": True}},
        )
    or_conditions.append(
        {f"block_states.{block_uid}.resource_config.container": {"$exists": True}},
    )

    # Project all relevant block_states
    projection: dict[str, Any] = {
        "_id": 1, "status": 1, "created_at": 1, "finished_at": 1,
        "block_states": 1,
    }

    cursor = (
        db["runs"]
        .find(
            {
                "pipeline_id": pipeline_id,
                "status": {"$in": ["completed", "failed"]},
                "$or": or_conditions,
            },
            projection,
        )
        .sort("created_at", -1)
        .limit(limit)
    )

    versions: list[dict[str, Any]] = []
    async for doc in cursor:
        all_bs = doc.get("block_states", {})
        version: dict[str, Any] = {
            "run_id": str(doc["_id"]),
            "status": doc.get("status", ""),
            "created_at": doc.get("created_at"),
            "finished_at": doc.get("finished_at"),
        }

        # Aggregate file counts from all action blocks writing to this sink
        total_files = 0
        total_bytes = 0
        container = ""
        base_path = ""
        for uid in search_uids:
            bs = all_bs.get(uid, {})
            if bs.get("sink_container"):
                container = container or bs["sink_container"]
                base_path = base_path or bs.get("sink_base_path", "")
                total_files += bs.get("sink_files_written", 0)
                total_bytes += bs.get("sink_total_bytes", 0)

        # Fall back to resource_config if no action blocks have data
        if not container:
            rc = all_bs.get(block_uid, {}).get("resource_config", {})
            container = rc.get("container", "")
            base_path = rc.get("base_path", "")

        if container:
            version["container"] = container
            version["base_path"] = base_path
            version["files_written"] = total_files
            version["total_bytes"] = total_bytes

        versions.append(version)
    return versions


@router.get("/runs/{run_id}/blocks/{block_uid}/files")
async def list_block_files(
    run_id: str,
    block_uid: str,
    prefix: str = Query("", description="Sub-path within the block's base_path"),
    db: AsyncIOMotorDatabase = Depends(get_db),  # type: ignore[type-arg]
) -> dict[str, Any]:
    """List files produced by a block in a specific run.

    Returns a flat list of blobs under the block's base_path, grouped into
    a virtual folder tree for the UI.
    """
    run_doc = await db["runs"].find_one({"_id": ObjectId(run_id)})
    if run_doc is None:
        raise HTTPException(status_code=404, detail="Run not found")

    pipeline_doc = await db["pipelines"].find_one(
        {"_id": ObjectId(run_doc["pipeline_id"])},
    )
    if pipeline_doc is None:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    container, base_path, conn_str = _resolve_sink_info(
        run_doc, pipeline_doc, block_uid,
    )
    if not conn_str:
        raise HTTPException(status_code=500, detail="No storage connection string")

    # Build the full prefix to list
    full_prefix = base_path
    if prefix:
        full_prefix = f"{base_path}/{prefix}" if base_path else prefix
    if full_prefix and not full_prefix.endswith("/"):
        full_prefix += "/"

    service = await _get_blob_service(conn_str)
    try:
        container_client = service.get_container_client(container)
        blobs: list[dict[str, Any]] = []
        folders: set[str] = set()

        async for blob in container_client.list_blobs(name_starts_with=full_prefix):
            # Get path relative to full_prefix
            rel_path = blob.name[len(full_prefix):]
            if not rel_path:
                continue

            # Check if this is a "folder" (has more path segments)
            parts = rel_path.split("/")
            if len(parts) > 1:
                folders.add(parts[0])
            else:
                blobs.append({
                    "name": parts[0],
                    "path": blob.name,
                    "size": blob.size,
                    "content_type": (
                        blob.content_settings.content_type
                        if blob.content_settings else ""
                    ),
                    "last_modified": (
                        blob.last_modified.isoformat()
                        if blob.last_modified else ""
                    ),
                })

        return {
            "container": container,
            "base_path": base_path,
            "prefix": prefix,
            "folders": sorted(folders),
            "files": sorted(blobs, key=lambda b: b["name"]),
            "total_files": len(blobs),
            "total_folders": len(folders),
        }
    finally:
        await service.close()


@router.get("/runs/{run_id}/blocks/{block_uid}/files/content")
async def get_file_content(
    run_id: str,
    block_uid: str,
    path: str = Query(..., description="Full blob path"),
    db: AsyncIOMotorDatabase = Depends(get_db),  # type: ignore[type-arg]
) -> Response:
    """Get the content of a specific file for preview.

    Returns raw content for text files (HTML, JSON, TXT, CSV, XML) up to 512 KB.
    Returns a JSON metadata stub for larger or binary files.
    """
    run_doc = await db["runs"].find_one({"_id": ObjectId(run_id)})
    if run_doc is None:
        raise HTTPException(status_code=404, detail="Run not found")

    pipeline_doc = await db["pipelines"].find_one(
        {"_id": ObjectId(run_doc["pipeline_id"])},
    )
    if pipeline_doc is None:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    container, _base_path, conn_str = _resolve_sink_info(
        run_doc, pipeline_doc, block_uid,
    )
    if not conn_str:
        raise HTTPException(status_code=500, detail="No storage connection string")

    service = await _get_blob_service(conn_str)
    try:
        blob_client = service.get_blob_client(container=container, blob=path)
        props = await blob_client.get_blob_properties()
        ct = props.content_settings.content_type if props.content_settings else ""
        size = props.size or 0

        # Check if previewable
        is_text = any(
            ct.startswith(t)
            for t in ("text/", "application/json", "application/xml")
        )
        if not is_text or size > _MAX_PREVIEW_BYTES:
            import json as jsonlib
            return Response(
                content=jsonlib.dumps({
                    "preview": False,
                    "reason": "too_large" if size > _MAX_PREVIEW_BYTES else "binary",
                    "size": size,
                    "content_type": ct,
                    "path": path,
                }),
                media_type="application/json",
            )

        stream = await blob_client.download_blob()
        data = await stream.readall()
        return Response(content=data, media_type=ct)
    finally:
        await service.close()
