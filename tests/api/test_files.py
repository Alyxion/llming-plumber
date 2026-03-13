"""Unit tests for the file browser API."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from bson import ObjectId
from httpx import ASGITransport, AsyncClient

from llming_plumber.api.files import (
    _find_action_blocks_for_resource,
    _find_resource_block_uid,
    _get_connection_string,
    _resolve_sink_info,
)


# ---------- Helper unit tests ----------


def test_find_resource_block_uid():
    pipeline = {
        "blocks": [
            {"uid": "crawl", "block_type": "web_crawler"},
            {"uid": "store", "block_type": "azure_blob_resource"},
        ],
        "pipes": [
            {
                "source_block_uid": "crawl",
                "target_block_uid": "store",
            },
        ],
    }
    assert _find_resource_block_uid(pipeline, "crawl") == "store"
    assert _find_resource_block_uid(pipeline, "store") is None
    assert _find_resource_block_uid(pipeline, "nonexistent") is None


def test_find_resource_block_uid_no_pipes():
    pipeline = {"blocks": [], "pipes": []}
    assert _find_resource_block_uid(pipeline, "any") is None


def test_get_connection_string_from_config():
    pipeline = {
        "blocks": [
            {"uid": "store", "config": {"connection_string": "test-conn-str"}},
        ],
    }
    assert _get_connection_string(pipeline, "store") == "test-conn-str"


def test_get_connection_string_fallback_env():
    pipeline = {"blocks": [{"uid": "store", "config": {}}]}
    with patch.dict("os.environ", {"AZURE_STORAGE_CONNECTION_STRING": "env-conn"}):
        assert _get_connection_string(pipeline, "store") == "env-conn"


def test_find_action_blocks_for_resource():
    pipeline = {
        "blocks": [
            {"uid": "crawl-de", "block_type": "web_crawler"},
            {"uid": "crawl-en", "block_type": "web_crawler"},
            {"uid": "store", "block_type": "azure_blob_resource"},
        ],
        "pipes": [
            {"source_block_uid": "crawl-de", "target_block_uid": "store"},
            {"source_block_uid": "crawl-en", "target_block_uid": "store"},
        ],
    }
    uids = _find_action_blocks_for_resource(pipeline, "store")
    assert sorted(uids) == ["crawl-de", "crawl-en"]
    assert _find_action_blocks_for_resource(pipeline, "crawl-de") == []


def test_resolve_sink_info_action_block():
    run_doc = {
        "block_states": {
            "crawl": {
                "sink_container": "my-container",
                "sink_base_path": "data/2026-03-09",
            },
        },
    }
    pipeline_doc = {
        "blocks": [
            {"uid": "crawl", "block_type": "web_crawler"},
            {"uid": "store", "block_type": "azure_blob_resource", "config": {"connection_string": "cs"}},
        ],
        "pipes": [{"source_block_uid": "crawl", "target_block_uid": "store"}],
    }
    container, base_path, conn = _resolve_sink_info(run_doc, pipeline_doc, "crawl")
    assert container == "my-container"
    assert base_path == "data/2026-03-09"
    assert conn == "cs"


def test_resolve_sink_info_resource_block():
    run_doc = {
        "block_states": {
            "store": {
                "resource_config": {
                    "container": "my-container",
                    "base_path": "crawls",
                },
            },
        },
    }
    pipeline_doc = {
        "blocks": [
            {"uid": "store", "block_type": "azure_blob_resource", "config": {"connection_string": "cs"}},
        ],
        "pipes": [],
    }
    container, base_path, conn = _resolve_sink_info(run_doc, pipeline_doc, "store")
    assert container == "my-container"
    assert base_path == "crawls"
    assert conn == "cs"


def test_resolve_sink_info_no_data():
    from fastapi import HTTPException

    run_doc = {"block_states": {"crawl": {"status": "completed"}}}
    pipeline_doc = {"blocks": [], "pipes": []}
    with pytest.raises(HTTPException) as exc_info:
        _resolve_sink_info(run_doc, pipeline_doc, "crawl")
    assert exc_info.value.status_code == 404
