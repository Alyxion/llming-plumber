"""Set Variables block — execute a script of variable operations.

Supports multiple operations per block in a Python-like DSL.
Each line is one operation:

    counter = 0
    counter += 1
    counter -= 1
    gl_total += amount
    pl_run_count += 1
    job_status = "processing"
    label = "item_" + str(index)
    greeting = name + " (" + city + ")"

Scopes:
    gl_name  — global (Redis, atomic, needs admin grant)
    pl_name  — pipeline-scoped (Redis, atomic)
    job_name — run-scoped (Redis, persisted in run log)
    name     — ephemeral (block-local, passed as output)
"""

from __future__ import annotations

import re
from typing import Any, ClassVar

from pydantic import ConfigDict, Field

from llming_plumber.blocks.base import (
    BaseBlock,
    BlockContext,
    BlockInput,
    BlockOutput,
)
from llming_plumber.blocks.core.safe_eval import SafeEvalError, safe_eval


class SetVariablesInput(BlockInput):
    model_config = ConfigDict(extra="allow")

    script: str = Field(
        default="",
        title="Script",
        description=(
            "Variable operations, one per line. "
            "Use = to set, += to increment/append, -= to decrement. "
            "Prefix: gl_ (global), pl_ (pipeline), job_ (run), none (local)."
        ),
        json_schema_extra={"widget": "textarea", "rows": 8},
    )


class SetVariablesOutput(BlockOutput):
    model_config = ConfigDict(extra="allow")

    variables: dict[str, Any] = Field(default_factory=dict)
    operations_run: int = 0


# Patterns for parsing operations
_ASSIGN_RE = re.compile(
    r"^([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*(.+)$",
)
_INCR_RE = re.compile(
    r"^([a-zA-Z_][a-zA-Z0-9_]*)\s*\+=\s*(.+)$",
)
_DECR_RE = re.compile(
    r"^([a-zA-Z_][a-zA-Z0-9_]*)\s*-=\s*(.+)$",
)

MAX_SCRIPT_LINES = 50
MAX_VARS = 100


class SetVariablesBlock(BaseBlock[SetVariablesInput, SetVariablesOutput]):
    block_type: ClassVar[str] = "set_variables"
    icon: ClassVar[str] = "tabler/variable"
    categories: ClassVar[list[str]] = ["core/data"]
    description: ClassVar[str] = (
        "Set, increment, or compute variables with "
        "global, pipeline, run, or local scope"
    )

    async def execute(
        self,
        input: SetVariablesInput,
        ctx: BlockContext | None = None,
    ) -> SetVariablesOutput:
        from llming_plumber.blocks.core.variable_store import VariableStore

        # Build variable store
        redis = None
        pipeline_id = ""
        run_id = ""
        if ctx:
            pipeline_id = ctx.pipeline_id
            run_id = ctx.run_id
            try:
                from llming_plumber.db import get_redis
                redis = get_redis()
            except Exception:
                pass

        store = VariableStore(redis, pipeline_id, run_id)

        # Parse and execute lines
        lines = [
            line.strip()
            for line in input.script.strip().splitlines()
            if line.strip() and not line.strip().startswith("#")
        ]

        if len(lines) > MAX_SCRIPT_LINES:
            msg = f"Script too long ({len(lines)} lines, max {MAX_SCRIPT_LINES})"
            raise SafeEvalError(msg)

        # Evaluation context: upstream input fields + extra piped fields
        eval_vars: dict[str, Any] = {}
        for key, val in input.model_dump().items():
            if key != "script":
                eval_vars[key] = val
        # Include extra fields piped from upstream blocks
        if input.model_extra:
            eval_vars.update(input.model_extra)

        ops_run = 0

        for line in lines:
            # Try += (increment/append)
            m = _INCR_RE.match(line)
            if m:
                name, expr = m.group(1), m.group(2)
                value = safe_eval(expr, eval_vars)
                if isinstance(value, str):
                    result = await store.append(name, value)
                else:
                    result = await store.incr(name, float(value))
                eval_vars[name] = result
                ops_run += 1
                continue

            # Try -= (decrement)
            m = _DECR_RE.match(line)
            if m:
                name, expr = m.group(1), m.group(2)
                value = safe_eval(expr, eval_vars)
                result = await store.decr(name, float(value))
                eval_vars[name] = result
                ops_run += 1
                continue

            # Try = (assign)
            m = _ASSIGN_RE.match(line)
            if m:
                name, expr = m.group(1), m.group(2)
                value = safe_eval(expr, eval_vars)
                await store.set(name, value)
                eval_vars[name] = value
                ops_run += 1
                continue

            msg = f"Invalid operation: {line!r}"
            raise SafeEvalError(msg)

        if len(eval_vars) > MAX_VARS:
            msg = f"Too many variables ({len(eval_vars)}, max {MAX_VARS})"
            raise SafeEvalError(msg)

        # Collect all variables for output
        all_vars = dict(eval_vars)
        # Include job vars for persistence
        job_vars = await store.get_job_vars()
        all_vars.update(job_vars)

        return SetVariablesOutput(
            variables=all_vars,
            operations_run=ops_run,
            **all_vars,  # flatten as top-level fields for downstream piping
        )
