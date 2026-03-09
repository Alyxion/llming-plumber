from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict, Field


class FittingDescriptor(BaseModel):
    """Describes a single input or output socket on a block.

    Blocks with multiple outputs (e.g. cache hit/miss) declare them
    via ``output_fittings``. The UI renders each as a distinct handle
    with its own position, color, and tooltip.
    """

    uid: str
    label: str
    kind: str = "output"  # "input" or "output"
    color: str = ""  # CSS color; empty = default
    description: str = ""


class BlockInput(BaseModel):
    """Base class for all block input models.

    Use Field() with title, description, and json_schema_extra for UI hints:
        city: str = Field(title="City", description="City name with country code")
        api_key: str = Field(title="API Key", json_schema_extra={"secret": True})
        units: str = Field(default="metric", json_schema_extra={
            "widget": "select", "options": ["metric", "imperial"]
        })

    Supported json_schema_extra keys for low-code editors:
        secret (bool)     — mask input, use credential store
        widget (str)      — text, textarea, select, number, toggle, code, color
        options (list)    — choices for select widget
        placeholder (str) — placeholder text
        group (str)       — group related fields in the UI
        min / max (float) — numeric range constraints
        rows (int)        — textarea height
    """


class BlockOutput(BaseModel):
    """Base class for all block output models."""


# ------------------------------------------------------------------
# Sink — streaming write handle provided by resource blocks
# ------------------------------------------------------------------


class Sink(ABC):
    """Abstract interface for streaming writes to external storage.

    Resource blocks create Sink instances.  Action blocks receive them
    via ``ctx.sink`` and write data incrementally — no buffering.
    """

    @abstractmethod
    async def write(
        self,
        path: str,
        content: str | bytes,
        *,
        content_type: str = "",
        metadata: dict[str, str] | None = None,
    ) -> None:
        """Write a single object/file to the sink."""
        ...

    async def finalize(self) -> dict[str, Any]:
        """Called when the action block finishes.  Returns summary."""
        return {}


class BlockContext(BaseModel):
    """Runtime context provided by the pipeline executor.

    Passed as None when a block runs standalone (outside a pipeline).
    Blocks can write to the run console via ``await ctx.log("message")``.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    run_id: str = ""
    pipeline_id: str = ""
    block_id: str = ""
    console: Any = Field(default=None, exclude=True)
    sink: Sink | None = Field(default=None, exclude=True)

    async def log(
        self,
        message: str,
        *,
        level: str = "info",
    ) -> None:
        """Write a message to the run console.

        No-ops gracefully when running standalone (``console is None``).
        """
        if self.console is not None:
            await self.console.write(self.block_id, message, level=level)


class BaseBlock[InputT: BlockInput, OutputT: BlockOutput](ABC):
    """Base class for all Plumber blocks.

    Every block declares its type, input/output models, and an async execute method.
    Blocks must work standalone with ctx=None.

    Block kinds
    -----------
    ``block_kind = "action"`` (default)
        Executed in sequence.  Receives parcels, runs logic, produces parcels.

    ``block_kind = "resource"``
        Defines a connection target (Azure Blob, S3, SFTP, ...).  **Not
        executed** as a pipeline step.  The executor reads its config and
        creates a :class:`Sink` (or source) that connected action blocks
        use for streaming I/O.
    """

    block_type: ClassVar[str]
    block_kind: ClassVar[str] = "action"
    cache_ttl: ClassVar[int] = 0
    icon: ClassVar[str] = "tabler/puzzle"
    categories: ClassVar[list[str]] = []
    description: ClassVar[str] = ""

    # Fan-out: output field name whose list items become individual parcels.
    fan_out_field: ClassVar[str | None] = None
    # Fan-in: gather all upstream parcels into an ``items`` list input.
    fan_in: ClassVar[bool] = False

    # Override to declare multiple input/output sockets.
    # When empty, the block has a single "input" and "output" fitting.
    input_fittings: ClassVar[list[FittingDescriptor]] = []
    output_fittings: ClassVar[list[FittingDescriptor]] = []

    @abstractmethod
    async def execute(self, input: InputT, ctx: BlockContext | None = None) -> OutputT:
        ...

    def create_sink(self, resolved_config: dict[str, Any]) -> Sink | None:
        """Create a Sink from this block's config.  Resource blocks override this."""
        return None
