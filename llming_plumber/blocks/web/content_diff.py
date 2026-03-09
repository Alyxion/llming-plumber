"""Content diff block — compare two text snapshots and detect changes.

Works on plain text extracted from web pages.  Returns structured diff
information: added lines, removed lines, change percentage, and a human-readable
summary.  Designed to chain after web_crawler for change monitoring.
"""

from __future__ import annotations

import difflib
from typing import Any, ClassVar

from pydantic import Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput


class ContentDiffInput(BlockInput):
    previous: str = Field(
        default="",
        title="Previous Content",
        description="The previous snapshot text to compare against",
        json_schema_extra={"widget": "textarea"},
    )
    current: str = Field(
        default="",
        title="Current Content",
        description="The current text to compare",
        json_schema_extra={"widget": "textarea"},
    )
    context_lines: int = Field(
        default=3,
        title="Context Lines",
        description="Number of surrounding context lines in the diff",
        json_schema_extra={"min": 0, "max": 20},
    )
    min_change_threshold: float = Field(
        default=0.01,
        title="Min Change Threshold",
        description="Minimum change ratio (0-1) to report as changed (filters noise)",
        json_schema_extra={"min": 0.0, "max": 1.0},
    )
    label: str = Field(
        default="",
        title="Label",
        description="Optional label for this comparison (e.g. URL or page name)",
    )


class ContentDiffOutput(BlockOutput):
    has_changes: bool = False
    change_ratio: float = 0.0
    added_lines: int = 0
    removed_lines: int = 0
    modified_sections: int = 0
    diff_text: str = ""
    added_content: str = ""
    removed_content: str = ""
    summary: str = ""
    label: str = ""


class ContentDiffBlock(BaseBlock[ContentDiffInput, ContentDiffOutput]):
    block_type: ClassVar[str] = "content_diff"
    icon: ClassVar[str] = "tabler/diff"
    categories: ClassVar[list[str]] = ["web/monitor", "core/transform"]
    description: ClassVar[str] = "Compare two text snapshots and detect changes"
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self, input: ContentDiffInput, ctx: BlockContext | None = None
    ) -> ContentDiffOutput:
        label = input.label

        # Handle first run (no previous content)
        if not input.previous:
            if ctx:
                await ctx.log(f"First snapshot{' for ' + label if label else ''} — no previous to compare")
            return ContentDiffOutput(
                has_changes=True,
                change_ratio=1.0,
                added_lines=len(input.current.splitlines()),
                summary=f"Initial snapshot{' for ' + label if label else ''} ({len(input.current)} chars)",
                added_content=input.current[:2000],
                label=label,
            )

        prev_lines = input.previous.splitlines(keepends=True)
        curr_lines = input.current.splitlines(keepends=True)

        # Compute unified diff
        diff = list(difflib.unified_diff(
            prev_lines, curr_lines,
            fromfile="previous", tofile="current",
            n=input.context_lines,
        ))

        added: list[str] = []
        removed: list[str] = []
        sections = 0

        for line in diff:
            if line.startswith("@@"):
                sections += 1
            elif line.startswith("+") and not line.startswith("+++"):
                added.append(line[1:].strip())
            elif line.startswith("-") and not line.startswith("---"):
                removed.append(line[1:].strip())

        # Compute similarity ratio
        ratio = difflib.SequenceMatcher(None, input.previous, input.current).ratio()
        change_ratio = round(1.0 - ratio, 4)

        has_changes = change_ratio >= input.min_change_threshold and len(diff) > 0

        diff_text = "".join(diff)
        added_content = "\n".join(added[:100])  # Cap at 100 lines
        removed_content = "\n".join(removed[:100])

        # Build human-readable summary
        parts = []
        if has_changes:
            parts.append(f"{change_ratio*100:.1f}% changed")
            if added:
                parts.append(f"+{len(added)} lines")
            if removed:
                parts.append(f"-{len(removed)} lines")
            parts.append(f"{sections} section(s)")
        else:
            parts.append("No significant changes")

        summary = f"{label + ': ' if label else ''}{', '.join(parts)}"

        if ctx:
            await ctx.log(summary)

        return ContentDiffOutput(
            has_changes=has_changes,
            change_ratio=change_ratio,
            added_lines=len(added),
            removed_lines=len(removed),
            modified_sections=sections,
            diff_text=diff_text[:10000],  # Cap diff output
            added_content=added_content,
            removed_content=removed_content,
            summary=summary,
            label=label,
        )
