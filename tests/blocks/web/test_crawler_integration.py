"""Integration tests for web blocks — hit real sites."""

from __future__ import annotations

import pathlib
import tempfile
import tomllib

import pytest

from llming_plumber.blocks.web.crawler import WebCrawlerBlock, WebCrawlerInput
from llming_plumber.blocks.web.site_diff import SiteDiffBlock, SiteDiffInput
from llming_plumber.blocks.web.snapshot_store import (
    SnapshotLoadBlock,
    SnapshotLoadInput,
    SnapshotSaveBlock,
    SnapshotSaveInput,
)

_SITES_FILE = pathlib.Path(__file__).resolve().parents[2] / "local_sites.toml"


def _load_sites() -> list[dict[str, str]]:
    """Load crawl targets from local_sites.toml (gitignored)."""
    if not _SITES_FILE.exists():
        pytest.skip(f"No {_SITES_FILE.name} — copy local_sites.example.toml and fill in URLs")
    with _SITES_FILE.open("rb") as f:
        return tomllib.load(f).get("sites", [])



@pytest.mark.integration
@pytest.mark.asyncio
async def test_crawl_configured_sites() -> None:
    """Crawl every site in local_sites.toml (5 pages each) and verify results."""
    sites = _load_sites()
    assert sites, "No sites configured in local_sites.toml"
    for site in sites:
        block = WebCrawlerBlock()
        result = await block.execute(WebCrawlerInput(
            start_url=site["url"],
            max_pages=5,
            max_depth=2,
            delay_seconds=1.0,
        ))
        assert result.page_count >= 1, f"No pages crawled for {site['name']}"
        assert result.errors == [] or len(result.errors) < result.page_count
        for page in result.pages:
            assert page["url"]
            assert page["status_code"] == 200


@pytest.mark.integration
@pytest.mark.asyncio
async def test_full_pipeline_crawl_snapshot_diff() -> None:
    """End-to-end: crawl → save → crawl again → save → load prev → diff."""
    tmp_dir = tempfile.mkdtemp(prefix="plumber_test_")

    crawler = WebCrawlerBlock()
    saver = SnapshotSaveBlock()
    loader = SnapshotLoadBlock()
    differ = SiteDiffBlock()

    # First crawl
    crawl1 = await crawler.execute(WebCrawlerInput(
        start_url="https://httpbin.org/",
        max_pages=3,
        max_depth=1,
        delay_seconds=0.5,
    ))
    assert crawl1.page_count >= 1

    # Save first snapshot
    save1 = await saver.execute(SnapshotSaveInput(
        snapshot_id="httpbin-integration",
        pages=crawl1.pages,
        storage_dir=tmp_dir,
    ))
    assert save1.previous_exists is False

    # Second crawl (same site, should be identical)
    crawl2 = await crawler.execute(WebCrawlerInput(
        start_url="https://httpbin.org/",
        max_pages=3,
        max_depth=1,
        delay_seconds=0.5,
    ))

    # Save second snapshot (rotates previous)
    save2 = await saver.execute(SnapshotSaveInput(
        snapshot_id="httpbin-integration",
        pages=crawl2.pages,
        storage_dir=tmp_dir,
    ))
    assert save2.previous_exists is True

    # Load previous
    prev = await loader.execute(SnapshotLoadInput(
        snapshot_id="httpbin-integration",
        which="previous",
        storage_dir=tmp_dir,
    ))
    assert prev.exists is True

    # Diff
    diff = await differ.execute(SiteDiffInput(
        previous_pages=prev.pages,
        current_pages=crawl2.pages,
        label="httpbin.org",
    ))
    # Same site crawled twice should have minimal changes
    assert diff.label == "httpbin.org"
    assert diff.total_previous >= 1
    assert diff.total_current >= 1

    # Cleanup
    import shutil
    shutil.rmtree(tmp_dir, ignore_errors=True)
