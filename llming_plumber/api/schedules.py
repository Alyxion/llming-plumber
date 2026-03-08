from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from bson import ObjectId
from croniter import croniter
from fastapi import APIRouter, Depends, HTTPException, Query
from motor.motor_asyncio import AsyncIOMotorDatabase

from llming_plumber.api.deps import get_db
from llming_plumber.models.mongo_helpers import doc_to_model, model_to_doc
from llming_plumber.models.schedule import Schedule

router = APIRouter()


def _compute_next_run(cron_expression: str) -> datetime:
    """Compute the next run time from a cron expression."""
    now = datetime.now(UTC)
    cron = croniter(cron_expression, now)
    next_dt: datetime = cron.get_next(datetime)
    return next_dt.replace(tzinfo=UTC) if next_dt.tzinfo is None else next_dt


@router.get("")
async def list_schedules(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    db: AsyncIOMotorDatabase = Depends(get_db),  # type: ignore[type-arg]
) -> list[dict[str, Any]]:
    """List all schedules."""
    cursor = db["schedules"].find().skip(skip).limit(limit)
    results: list[dict[str, Any]] = []
    async for doc in cursor:
        results.append(doc_to_model(doc, Schedule).model_dump(mode="json"))
    return results


@router.post("", status_code=201)
async def create_schedule(
    schedule: Schedule,
    db: AsyncIOMotorDatabase = Depends(get_db),  # type: ignore[type-arg]
) -> dict[str, Any]:
    """Create a new schedule. Computes next_run_at from cron or interval."""
    if schedule.cron_expression:
        try:
            schedule.next_run_at = _compute_next_run(schedule.cron_expression)
        except (ValueError, KeyError) as exc:
            msg = f"Invalid cron expression: {exc}"
            raise HTTPException(status_code=400, detail=msg)
    elif schedule.interval_seconds and not schedule.next_run_at:
        schedule.next_run_at = datetime.now(UTC)

    doc = model_to_doc(schedule)
    doc.pop("_id", None)
    result = await db["schedules"].insert_one(doc)
    schedule.id = str(result.inserted_id)
    return schedule.model_dump(mode="json")


@router.put("/{schedule_id}")
async def update_schedule(
    schedule_id: str,
    schedule: Schedule,
    db: AsyncIOMotorDatabase = Depends(get_db),  # type: ignore[type-arg]
) -> dict[str, Any]:
    """Update a schedule."""
    existing = await db["schedules"].find_one({"_id": ObjectId(schedule_id)})
    if existing is None:
        raise HTTPException(status_code=404, detail="Schedule not found")

    if schedule.cron_expression:
        try:
            schedule.next_run_at = _compute_next_run(schedule.cron_expression)
        except (ValueError, KeyError) as exc:
            msg = f"Invalid cron expression: {exc}"
            raise HTTPException(status_code=400, detail=msg)
    elif schedule.enabled and schedule.interval_seconds and not schedule.next_run_at:
        schedule.next_run_at = datetime.now(UTC)

    doc = model_to_doc(schedule)
    doc.pop("_id", None)
    await db["schedules"].replace_one({"_id": ObjectId(schedule_id)}, doc)

    doc["_id"] = ObjectId(schedule_id)
    return doc_to_model(doc, Schedule).model_dump(mode="json")


@router.delete("/{schedule_id}", status_code=204)
async def delete_schedule(
    schedule_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db),  # type: ignore[type-arg]
) -> None:
    """Delete a schedule by ID."""
    result = await db["schedules"].delete_one({"_id": ObjectId(schedule_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Schedule not found")
