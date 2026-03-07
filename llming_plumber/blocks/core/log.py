"""Write a message to the run console.

The console is a Redis-backed, per-run log stream visible to anyone
with access to the run.  Useful for debugging and status updates.

The message supports ``{expression}`` placeholders that are evaluated
safely against any extra fields piped into the block::

    message = "Processing item #{index + 1}"
    # With index=3 piped from upstream → "Processing item #4"

Use ``{{`` / ``}}`` for literal braces.
"""

from __future__ import annotations

from typing import Any, ClassVar

from pydantic import ConfigDict, Field

from llming_plumber.blocks.base import (
    BaseBlock,
    BlockContext,
    BlockInput,
    BlockOutput,
)
from llming_plumber.blocks.core.safe_eval import SafeEvalError, render_template


class LogInput(BlockInput):
    model_config = ConfigDict(extra="allow")

    message: str = Field(
        title="Message",
        description=(
            "Text to write to the console. "
            "Use {variable} to interpolate piped fields."
        ),
        json_schema_extra={"widget": "textarea", "rows": 3},
    )
    level: str = Field(
        default="info",
        title="Level",
        json_schema_extra={
            "widget": "select",
            "options": ["debug", "info", "warning", "error"],
        },
    )


class LogOutput(BlockOutput):
    logged: bool
    message: str


class LogBlock(BaseBlock[LogInput, LogOutput]):
    block_type: ClassVar[str] = "log"
    icon: ClassVar[str] = "tabler/terminal-2"
    categories: ClassVar[list[str]] = ["core/flow"]
    description: ClassVar[str] = "Write a message to the run console"
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self,
        input: LogInput,
        ctx: BlockContext | None = None,
    ) -> LogOutput:
        # Collect extra fields as template variables
        variables: dict[str, Any] = input.model_extra or {}

        try:
            rendered = render_template(input.message, variables)
        except SafeEvalError:
            # Fall back to raw message if interpolation fails
            rendered = input.message

        if ctx is not None:
            await ctx.log(rendered, level=input.level)
        return LogOutput(logged=ctx is not None, message=rendered)
