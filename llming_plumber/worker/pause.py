"""Pause controller — coordinates pause/resume signaling between guards and the executor.

A ``PauseController`` wraps two :class:`asyncio.Event` objects — one for "paused"
and one for "running" (resumed).  The periodic guard sets/clears these events,
and the executor (or individual blocks) calls :meth:`wait_if_paused` at natural
breakpoints to suspend execution until the guard clears.
"""

from __future__ import annotations

import asyncio


class PauseController:
    """Thread-safe, single-run pause/resume coordination.

    ``is_paused``
        ``True`` while the pipeline is paused.

    ``wait_if_paused()``
        Blocks (``await``) until the pipeline is un-paused.
        Returns immediately if already running.
    """

    def __init__(self) -> None:
        self._paused = asyncio.Event()      # set = paused
        self._resumed = asyncio.Event()      # set = running
        self._resumed.set()                  # starts in "running" state

    @property
    def is_paused(self) -> bool:
        return self._paused.is_set()

    def pause(self) -> None:
        """Signal all waiters to suspend."""
        self._paused.set()
        self._resumed.clear()

    def resume(self) -> None:
        """Signal all waiters to continue."""
        self._paused.clear()
        self._resumed.set()

    async def wait_if_paused(self) -> None:
        """Block until resumed.  Returns immediately if not paused."""
        await self._resumed.wait()
