"""Unit tests for Azure Blob Storage resource block and sink."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from llming_plumber.blocks.azure.blob_resource import (
    AzureBlobResourceBlock,
    AzureBlobResourceInput,
    AzureBlobSink,
)


def test_resource_block_kind() -> None:
    block = AzureBlobResourceBlock()
    assert block.block_kind == "resource"


@pytest.mark.asyncio
async def test_resource_block_standalone() -> None:
    block = AzureBlobResourceBlock()
    result = await block.execute(AzureBlobResourceInput(
        container="test-container",
        base_path="crawls/{date}",
        retention_days=30,
    ))
    assert result.container == "test-container"
    assert result.retention_days == 30


def test_create_sink() -> None:
    block = AzureBlobResourceBlock()
    sink = block.create_sink({
        "connection_string": "fake",
        "container": "my-container",
        "base_path": "data/test",
        "retention_days": "90",
    })
    assert isinstance(sink, AzureBlobSink)
    assert sink._container == "my-container"
    assert sink._base_path == "data/test"
    assert sink._retention_days == 90


@pytest.mark.asyncio
async def test_sink_write_and_finalize() -> None:
    """Test that sink writes to Azure and tracks stats."""
    mock_blob_client = AsyncMock()
    mock_blob_client.upload_blob = AsyncMock()

    mock_service = AsyncMock()
    mock_service.get_blob_client = MagicMock(return_value=mock_blob_client)
    mock_service.close = AsyncMock()

    sink = AzureBlobSink(
        connection_string="fake",
        container="test",
        base_path="prefix/path",
        retention_days=60,
    )
    sink._service = mock_service  # Inject mock

    await sink.write("html/index.html", "<html>hello</html>")
    await sink.write("text/index.txt", "hello")
    await sink.write("content.json", '{"pages": []}', content_type="application/json")

    assert sink._files_written == 3
    assert sink._total_bytes > 0

    # Check paths include base_path prefix
    calls = mock_service.get_blob_client.call_args_list
    assert calls[0].kwargs["blob"] == "prefix/path/html/index.html"
    assert calls[1].kwargs["blob"] == "prefix/path/text/index.txt"
    assert calls[2].kwargs["blob"] == "prefix/path/content.json"

    # Finalize
    summary = await sink.finalize()
    assert summary["files_written"] == 3
    assert summary["container"] == "test"
    assert summary["base_path"] == "prefix/path"
    assert summary["retention_days"] == 60
    mock_service.close.assert_called_once()


@pytest.mark.asyncio
async def test_sink_content_type_guessing() -> None:
    assert AzureBlobSink._guess_content_type("page.html") == "text/html"
    assert AzureBlobSink._guess_content_type("data.json") == "application/json"
    assert AzureBlobSink._guess_content_type("file.txt") == "text/plain"
    assert AzureBlobSink._guess_content_type("data.csv") == "text/csv"
    assert AzureBlobSink._guess_content_type("binary.bin") == "application/octet-stream"


@pytest.mark.asyncio
async def test_sink_retention_metadata() -> None:
    """Retention days should set expires_at metadata on blobs."""
    mock_blob_client = AsyncMock()
    mock_blob_client.upload_blob = AsyncMock()

    mock_service = AsyncMock()
    mock_service.get_blob_client = MagicMock(return_value=mock_blob_client)
    mock_service.close = AsyncMock()

    sink = AzureBlobSink(
        connection_string="fake",
        container="test",
        base_path="",
        retention_days=30,
    )
    sink._service = mock_service

    await sink.write("test.txt", "content")

    # Check that metadata includes expires_at
    call_kwargs = mock_blob_client.upload_blob.call_args.kwargs
    assert "metadata" in call_kwargs
    assert "expires_at" in call_kwargs["metadata"]


@pytest.mark.asyncio
async def test_sink_no_retention() -> None:
    """Zero retention should not set expires_at metadata."""
    mock_blob_client = AsyncMock()
    mock_blob_client.upload_blob = AsyncMock()

    mock_service = AsyncMock()
    mock_service.get_blob_client = MagicMock(return_value=mock_blob_client)
    mock_service.close = AsyncMock()

    sink = AzureBlobSink(
        connection_string="fake",
        container="test",
        base_path="",
        retention_days=0,
    )
    sink._service = mock_service

    await sink.write("test.txt", "content")

    call_kwargs = mock_blob_client.upload_blob.call_args.kwargs
    # metadata should be None (no expires_at when retention_days=0)
    assert call_kwargs.get("metadata") is None


@pytest.mark.asyncio
async def test_sink_empty_base_path() -> None:
    """Empty base_path should write directly to root."""
    mock_blob_client = AsyncMock()
    mock_blob_client.upload_blob = AsyncMock()

    mock_service = AsyncMock()
    mock_service.get_blob_client = MagicMock(return_value=mock_blob_client)
    mock_service.close = AsyncMock()

    sink = AzureBlobSink(
        connection_string="fake",
        container="test",
        base_path="",
        retention_days=0,
    )
    sink._service = mock_service

    await sink.write("file.txt", "data")

    call = mock_service.get_blob_client.call_args
    assert call.kwargs["blob"] == "file.txt"
