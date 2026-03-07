from __future__ import annotations

import contextlib
from pathlib import Path
from typing import Any

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
        from arq.connections import RedisSettings, create_pool

        from llming_plumber.blocks.registry import BlockRegistry
        from llming_plumber.db import close_connections, ensure_indexes, get_database

        # --- startup ---
        BlockRegistry.discover()

        db = get_database()
        await ensure_indexes(db)

        if mode in ("all", "ui"):
            app.state.arq_pool = await create_pool(
                RedisSettings.from_dsn(settings.redis_url)
            )
        else:
            # Worker-only mode: no ARQ pool needed on the HTTP side
            app.state.arq_pool = None

        yield

        # --- shutdown ---
        if app.state.arq_pool is not None:
            await app.state.arq_pool.aclose()
        await close_connections()

    return lifespan
