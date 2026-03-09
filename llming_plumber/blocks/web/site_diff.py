"""Site diff block — compare two site snapshots page-by-page.

Takes two lists of crawled pages (previous + current) and produces a structured
change report: new pages, removed pages, modified pages with diff details.
"""

from __future__ import annotations

import difflib
from typing import Any, ClassVar

from pydantic import Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput


class SiteDiffInput(BlockInput):
    previous_pages: list[dict[str, Any]] = Field(
        default_factory=list,
        title="Previous Pages",
        description="Pages from the previous crawl snapshot",
    )
    current_pages: list[dict[str, Any]] = Field(
        default_factory=list,
        title="Current Pages",
        description="Pages from the current crawl",
    )
    min_change_ratio: float = Field(
        default=0.02,
        title="Min Change Ratio",
        description="Minimum text change ratio to report (filters minor noise)",
        json_schema_extra={"min": 0.0, "max": 1.0},
    )
    include_diff_text: bool = Field(
        default=True,
        title="Include Diff Text",
        description="Include the actual diff text in output (can be large)",
        json_schema_extra={"widget": "toggle"},
    )
    label: str = Field(
        default="",
        title="Site Label",
        description="Optional site label for the report",
    )


class PageChange(BlockOutput):
    url: str = ""
    title: str = ""
    change_type: str = ""  # "new", "removed", "modified"
    change_ratio: float = 0.0
    added_lines: int = 0
    removed_lines: int = 0
    diff_text: str = ""
    summary: str = ""


class SiteDiffOutput(BlockOutput):
    has_changes: bool = False
    new_pages: list[dict[str, Any]] = Field(default_factory=list)
    removed_pages: list[dict[str, Any]] = Field(default_factory=list)
    modified_pages: list[dict[str, Any]] = Field(default_factory=list)
    new_count: int = 0
    removed_count: int = 0
    modified_count: int = 0
    unchanged_count: int = 0
    total_previous: int = 0
    total_current: int = 0
    report: str = ""
    label: str = ""


class SiteDiffBlock(BaseBlock[SiteDiffInput, SiteDiffOutput]):
    block_type: ClassVar[str] = "site_diff"
    icon: ClassVar[str] = "tabler/git-compare"
    categories: ClassVar[list[str]] = ["web/monitor"]
    description: ClassVar[str] = "Compare two site crawl snapshots and detect page-level changes"
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self, input: SiteDiffInput, ctx: BlockContext | None = None
    ) -> SiteDiffOutput:
        label = input.label

        # Build URL-keyed dicts
        prev_by_url: dict[str, dict[str, Any]] = {}
        for p in input.previous_pages:
            url = p.get("url", "")
            if url:
                prev_by_url[url] = p

        curr_by_url: dict[str, dict[str, Any]] = {}
        for p in input.current_pages:
            url = p.get("url", "")
            if url:
                curr_by_url[url] = p

        prev_urls = set(prev_by_url.keys())
        curr_urls = set(curr_by_url.keys())

        # New pages
        new_pages: list[dict[str, Any]] = []
        for url in sorted(curr_urls - prev_urls):
            p = curr_by_url[url]
            new_pages.append(PageChange(
                url=url,
                title=p.get("title", ""),
                change_type="new",
                change_ratio=1.0,
                summary=f"New page: {p.get('title', url)}",
            ).model_dump())

        # Removed pages
        removed_pages: list[dict[str, Any]] = []
        for url in sorted(prev_urls - curr_urls):
            p = prev_by_url[url]
            removed_pages.append(PageChange(
                url=url,
                title=p.get("title", ""),
                change_type="removed",
                change_ratio=1.0,
                summary=f"Removed: {p.get('title', url)}",
            ).model_dump())

        # Modified pages
        modified_pages: list[dict[str, Any]] = []
        unchanged = 0
        for url in sorted(curr_urls & prev_urls):
            prev_text = prev_by_url[url].get("text", "")
            curr_text = curr_by_url[url].get("text", "")

            # Quick hash check
            prev_hash = prev_by_url[url].get("content_hash", "")
            curr_hash = curr_by_url[url].get("content_hash", "")
            if prev_hash and curr_hash and prev_hash == curr_hash:
                unchanged += 1
                continue

            # Compute similarity
            ratio = difflib.SequenceMatcher(None, prev_text, curr_text).ratio()
            change_ratio = round(1.0 - ratio, 4)

            if change_ratio < input.min_change_ratio:
                unchanged += 1
                continue

            # Generate diff
            diff_text = ""
            added = 0
            removed = 0
            if input.include_diff_text:
                prev_lines = prev_text.splitlines(keepends=True)
                curr_lines = curr_text.splitlines(keepends=True)
                diff_lines = list(difflib.unified_diff(prev_lines, curr_lines, n=2))
                for line in diff_lines:
                    if line.startswith("+") and not line.startswith("+++"):
                        added += 1
                    elif line.startswith("-") and not line.startswith("---"):
                        removed += 1
                diff_text = "".join(diff_lines)[:5000]

            title = curr_by_url[url].get("title", "")
            modified_pages.append(PageChange(
                url=url,
                title=title,
                change_type="modified",
                change_ratio=change_ratio,
                added_lines=added,
                removed_lines=removed,
                diff_text=diff_text,
                summary=f"Modified ({change_ratio*100:.1f}%): {title or url}",
            ).model_dump())

        has_changes = bool(new_pages or removed_pages or modified_pages)

        # Build report
        report_parts = [f"Site diff report{' for ' + label if label else ''}:"]
        report_parts.append(f"  Previous: {len(prev_by_url)} pages, Current: {len(curr_by_url)} pages")
        if new_pages:
            report_parts.append(f"\n  NEW PAGES ({len(new_pages)}):")
            for p in new_pages:
                report_parts.append(f"    + {p['title'] or p['url']}")
        if removed_pages:
            report_parts.append(f"\n  REMOVED PAGES ({len(removed_pages)}):")
            for p in removed_pages:
                report_parts.append(f"    - {p['title'] or p['url']}")
        if modified_pages:
            report_parts.append(f"\n  MODIFIED PAGES ({len(modified_pages)}):")
            for p in sorted(modified_pages, key=lambda x: x["change_ratio"], reverse=True):
                report_parts.append(f"    ~ {p['change_ratio']*100:.1f}% {p['title'] or p['url']}")
        if not has_changes:
            report_parts.append("  No significant changes detected.")

        report = "\n".join(report_parts)

        if ctx:
            await ctx.log(
                f"Site diff: +{len(new_pages)} new, -{len(removed_pages)} removed, "
                f"~{len(modified_pages)} modified, ={unchanged} unchanged"
            )

        return SiteDiffOutput(
            has_changes=has_changes,
            new_pages=new_pages,
            removed_pages=removed_pages,
            modified_pages=modified_pages,
            new_count=len(new_pages),
            removed_count=len(removed_pages),
            modified_count=len(modified_pages),
            unchanged_count=unchanged,
            total_previous=len(prev_by_url),
            total_current=len(curr_by_url),
            report=report,
            label=label,
        )
