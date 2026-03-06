from __future__ import annotations

from fastapi import APIRouter

from llming_plumber.api.blocks import router as blocks_router
from llming_plumber.api.health import router as health_router
from llming_plumber.api.lemmings import router as lemmings_router
from llming_plumber.api.pipelines import router as pipelines_router
from llming_plumber.api.runs import router as runs_router
from llming_plumber.api.schedules import router as schedules_router

router = APIRouter()
router.include_router(pipelines_router, prefix="/pipelines", tags=["pipelines"])
router.include_router(runs_router, prefix="/runs", tags=["runs"])
router.include_router(schedules_router, prefix="/schedules", tags=["schedules"])
router.include_router(blocks_router, prefix="/blocks", tags=["blocks"])
router.include_router(lemmings_router, prefix="/lemmings", tags=["lemmings"])
router.include_router(health_router, tags=["health"])
