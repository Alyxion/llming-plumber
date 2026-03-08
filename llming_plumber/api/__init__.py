from __future__ import annotations

from fastapi import APIRouter

from llming_plumber.api.blocks import router as blocks_router
from llming_plumber.api.demo import router as demo_router
from llming_plumber.api.events import router as events_router
from llming_plumber.api.health import router as health_router
from llming_plumber.api.inline_run import router as inline_run_router
from llming_plumber.api.llm_defaults import router as llm_defaults_router
from llming_plumber.api.lemmings import router as lemmings_router
from llming_plumber.api.pipelines import router as pipelines_router
from llming_plumber.api.runs import router as runs_router
from llming_plumber.api.schedules import router as schedules_router
from llming_plumber.api.session import router as session_router
from llming_plumber.api.variables import router as variables_router

router = APIRouter()
router.include_router(pipelines_router, prefix="/pipelines", tags=["pipelines"])
router.include_router(runs_router, prefix="/runs", tags=["runs"])
router.include_router(schedules_router, prefix="/schedules", tags=["schedules"])
router.include_router(blocks_router, prefix="/blocks", tags=["blocks"])
router.include_router(lemmings_router, prefix="/lemmings", tags=["lemmings"])
router.include_router(events_router, tags=["events"])
router.include_router(inline_run_router, tags=["inline-run"])
router.include_router(llm_defaults_router, tags=["llm"])
router.include_router(health_router, tags=["health"])
router.include_router(session_router, tags=["session"])
router.include_router(variables_router, prefix="/variables", tags=["variables"])
router.include_router(demo_router, prefix="/demo-pipelines", tags=["demo"])
