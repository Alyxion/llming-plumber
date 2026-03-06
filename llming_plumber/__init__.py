from __future__ import annotations

import contextlib
from typing import Any

from fastapi import FastAPI

from llming_plumber.config import settings


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
