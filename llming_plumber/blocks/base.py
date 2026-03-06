from __future__ import annotations

from abc import ABC, abstractmethod
from typing import ClassVar

from pydantic import BaseModel


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


class BlockContext(BaseModel):
    """Runtime context provided by the pipeline executor.

    Passed as None when a block runs standalone (outside a pipeline).
    """

    run_id: str = ""
    pipeline_id: str = ""
    block_id: str = ""


class BaseBlock[I: BlockInput, O: BlockOutput](ABC):
    """Base class for all Plumber blocks.

    Every block declares its type, input/output models, and an async execute method.
    Blocks must work standalone with ctx=None.
    """

    block_type: ClassVar[str]
    cache_ttl: ClassVar[int] = 0
    icon: ClassVar[str] = "tabler/puzzle"
    categories: ClassVar[list[str]] = []
    description: ClassVar[str] = ""

    @abstractmethod
    async def execute(self, input: I, ctx: BlockContext | None = None) -> O:
        ...
