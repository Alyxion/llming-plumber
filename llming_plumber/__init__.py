from __future__ import annotations

import contextlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from bson import ObjectId

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from starlette.responses import FileResponse

from llming_plumber.config import settings

# Load .env so block API keys (OPENWEATHER_API_KEY, etc.) are available
load_dotenv()

# UI directory — vendored JS/CSS, no build step needed
_UI_DIR = Path(__file__).parent / "ui"


def create_app(mode: str | None = None) -> FastAPI:
    """Application factory for the Plumber FastAPI app.

    Parameters
    ----------
    mode:
        Deployment mode override (``"all"``, ``"ui"``, ``"worker"``).
        Falls back to ``settings.mode`` when *None*.
    """
    effective_mode = mode or settings.mode

    app = FastAPI(
        title="Plumber",
        description="Pipeline automation engine",
        lifespan=_lifespan(effective_mode),
    )

    from llming_plumber.api import router as api_router

    app.include_router(api_router, prefix=settings.api_prefix)

    # Serve the UI — all assets are local, no npm/CDN needed
    if _UI_DIR.is_dir():
        # Static sub-directories
        app.mount("/vendor", StaticFiles(directory=str(_UI_DIR / "vendor")), name="vendor")
        app.mount("/static", StaticFiles(directory=str(_UI_DIR / "static")), name="ui-static")
        app.mount("/themes", StaticFiles(directory=str(_UI_DIR / "themes")), name="themes")
        app.mount("/plugins", StaticFiles(directory=str(_UI_DIR / "plugins")), name="plugins")

        # Serve JS modules (app.js and any future modules)
        @app.get("/app.js")
        async def _serve_app_js() -> FileResponse:
            return FileResponse(str(_UI_DIR / "app.js"), media_type="text/javascript")

        # SPA catch-all — must be last
        @app.get("/{full_path:path}")
        async def _serve_spa(full_path: str) -> FileResponse:
            """Serve index.html for any non-API, non-static route."""
            file = _UI_DIR / full_path
            if file.is_file() and not full_path.startswith("api"):
                return FileResponse(str(file))
            return FileResponse(str(_UI_DIR / "index.html"))

    return app


def _lifespan(mode: str) -> Any:
    """Return an async context-manager lifespan for the FastAPI app."""

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI) -> Any:  # type: ignore[misc]
        import asyncio
        import logging

        from arq.connections import RedisSettings, create_pool

        from llming_plumber.blocks.registry import BlockRegistry
        from llming_plumber.db import close_connections, ensure_indexes, get_database

        logger = logging.getLogger("llming_plumber")

        # --- startup ---
        BlockRegistry.discover()

        db = get_database()
        await ensure_indexes(db)

        # Cancel stale queued/running runs from previous crashes
        stale = await db["runs"].update_many(
            {"status": {"$in": ["queued", "running"]}},
            {"$set": {"status": "cancelled", "error": "Stale run cancelled on startup"}},
        )
        if stale.modified_count:
            logger.info("Cancelled %d stale runs from previous session", stale.modified_count)

        if mode in ("all", "ui"):
            app.state.arq_pool = await create_pool(
                RedisSettings.from_dsn(settings.redis_url)
            )
        else:
            app.state.arq_pool = None

        # In all/ui mode, run the scheduler loop + inline worker in-process
        scheduler_task = None
        if mode in ("all", "ui"):
            from llming_plumber.worker.scheduler import check_schedules

            async def _dispatch_run(run_id: str) -> None:
                """Dispatch a scheduled run inline (no separate worker needed)."""
                from llming_plumber.worker.executor import execute_pipeline

                async def _run_with_timeout() -> None:
                    timeout = settings.run_timeout
                    try:
                        await asyncio.wait_for(
                            execute_pipeline(
                                {"db": db, "lemming_id": "inline"},
                                run_id=run_id,
                            ),
                            timeout=timeout,
                        )
                    except TimeoutError:
                        logger.error(
                            "Run %s timed out after %ds — marking as failed",
                            run_id, timeout,
                        )
                        await db["runs"].update_one(
                            {"_id": ObjectId(run_id)},
                            {"$set": {
                                "status": "failed",
                                "error": f"Run timed out after {timeout}s",
                                "finished_at": datetime.now(UTC),
                            }},
                        )
                    except Exception:
                        logger.exception("Run %s failed unexpectedly", run_id)

                asyncio.create_task(_run_with_timeout())

            async def _scheduler_loop() -> None:
                poll = settings.scheduler_poll_seconds
                ctx: dict[str, Any] = {
                    "db": db,
                    "redis": app.state.arq_pool,
                    "pool": app.state.arq_pool,
                    "dispatch_run": _dispatch_run,
                }
                logger.info("In-process scheduler started (every %ds)", poll)
                while True:
                    try:
                        await check_schedules(ctx)
                    except Exception:
                        logger.exception("Scheduler iteration failed")
                    await asyncio.sleep(poll)

            scheduler_task = asyncio.create_task(_scheduler_loop())

        yield

        # --- shutdown ---
        if scheduler_task:
            scheduler_task.cancel()
        if app.state.arq_pool is not None:
            await app.state.arq_pool.aclose()
        await close_connections()

    return lifespan
