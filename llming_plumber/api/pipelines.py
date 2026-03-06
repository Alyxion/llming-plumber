from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from bson import ObjectId
from fastapi import APIRouter, Body, Depends, HTTPException, Query
from motor.motor_asyncio import AsyncIOMotorDatabase

from llming_plumber.api.deps import get_arq_pool, get_db
from llming_plumber.blocks.registry import BlockRegistry
from llming_plumber.models.mongo_helpers import doc_to_model, model_to_doc
from llming_plumber.models.pipeline import PipelineDefinition
from llming_plumber.models.run import Run, RunStatus

router = APIRouter()


@router.get("")
async def list_pipelines(
    owner_id: str | None = Query(None),
    tag: str | None = Query(None),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    db: AsyncIOMotorDatabase = Depends(get_db),  # type: ignore[type-arg]
) -> list[dict[str, Any]]:
    """List pipelines with optional filtering."""
    query: dict[str, Any] = {}
    if owner_id is not None:
        query["owner_id"] = owner_id
    if tag is not None:
        query["tags"] = tag

    cursor = db["pipelines"].find(query).skip(skip).limit(limit)
    results: list[dict[str, Any]] = []
    async for doc in cursor:
        results.append(doc_to_model(doc, PipelineDefinition).model_dump(mode="json"))
    return results


@router.post("", status_code=201)
async def create_pipeline(
    pipeline: PipelineDefinition,
    db: AsyncIOMotorDatabase = Depends(get_db),  # type: ignore[type-arg]
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

    doc = model_to_doc(pipeline)
    doc.pop("_id", None)  # let MongoDB generate _id
    result = await db["pipelines"].insert_one(doc)
    pipeline.id = str(result.inserted_id)
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
) -> dict[str, Any]:
    """Update a pipeline, bumping the version."""
    existing = await db["pipelines"].find_one({"_id": ObjectId(pipeline_id)})
    if existing is None:
        raise HTTPException(status_code=404, detail="Pipeline not found")

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

    await db["pipelines"].replace_one({"_id": ObjectId(pipeline_id)}, doc)

    doc["_id"] = ObjectId(pipeline_id)
    return doc_to_model(doc, PipelineDefinition).model_dump(mode="json")


@router.delete("/{pipeline_id}", status_code=204)
async def delete_pipeline(
    pipeline_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db),  # type: ignore[type-arg]
) -> None:
    """Delete a pipeline by ID."""
    result = await db["pipelines"].delete_one({"_id": ObjectId(pipeline_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Pipeline not found")


@router.post("/{pipeline_id}/run", status_code=201)
async def run_pipeline(
    pipeline_id: str,
    body: dict[str, Any] = Body(default={}),
    db: AsyncIOMotorDatabase = Depends(get_db),  # type: ignore[type-arg]
    arq_pool: Any = Depends(get_arq_pool),
) -> dict[str, Any]:
    """Create a Run document (status=queued) and enqueue via ARQ."""
    pipeline_doc = await db["pipelines"].find_one({"_id": ObjectId(pipeline_id)})
    if pipeline_doc is None:
        raise HTTPException(status_code=404, detail="Pipeline not found")

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

    # Enqueue into Redis via ARQ
    await arq_pool.enqueue_job("execute_pipeline", run_id=run_id)

    return {"run_id": run_id, "status": "queued"}
