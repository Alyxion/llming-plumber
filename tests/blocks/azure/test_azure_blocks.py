"""Unit tests for Azure Blob Storage blocks — mocked, no real Azure calls.

Every block is tested by mocking ``get_blob_service`` from
``llming_plumber.blocks.azure._storage`` so that no network access is needed.
"""

from __future__ import annotations

import base64
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

_SVC = "get_blob_service"
MOCK_READ = f"llming_plumber.blocks.azure.blob_read.{_SVC}"
MOCK_WRITE = f"llming_plumber.blocks.azure.blob_write.{_SVC}"
MOCK_LIST = f"llming_plumber.blocks.azure.blob_list.{_SVC}"
MOCK_DELETE = f"llming_plumber.blocks.azure.blob_delete.{_SVC}"
MOCK_TRIGGER = f"llming_plumber.blocks.azure.blob_trigger.{_SVC}"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_blob_props(
    *,
    etag: str = '"0x123"',
    content_type: str = "text/plain",
    last_modified: datetime | None = None,
    size: int = 11,
) -> SimpleNamespace:
    """Build a fake blob properties namespace used by list_blobs."""
    last_mod = last_modified or datetime(2026, 1, 1, tzinfo=UTC)
    return SimpleNamespace(
        name="",  # caller sets this
        etag=etag,
        size=size,
        last_modified=last_mod,
        content_settings=SimpleNamespace(content_type=content_type),
    )


class _AsyncBlobIter:
    """Async iterator that yields blobs from a list."""

    def __init__(self, blobs: list[Any]) -> None:
        self._blobs = list(blobs)
        self._idx = 0

    def __aiter__(self) -> _AsyncBlobIter:
        return self

    async def __anext__(self) -> Any:
        if self._idx >= len(self._blobs):
            raise StopAsyncIteration
        blob = self._blobs[self._idx]
        self._idx += 1
        return blob


def _service_ctx(service_mock: MagicMock) -> MagicMock:
    """Make the mock work as ``async with service:``."""
    service_mock.__aenter__ = AsyncMock(return_value=service_mock)
    service_mock.__aexit__ = AsyncMock(return_value=False)
    return service_mock


# ---------------------------------------------------------------------------
# BlobReadBlock
# ---------------------------------------------------------------------------


class TestBlobReadBlock:
    async def test_read_text(self) -> None:
        from llming_plumber.blocks.azure.blob_read import (
            BlobReadBlock,
            BlobReadInput,
            BlobReadOutput,
        )

        raw = b"hello world"
        download_mock = AsyncMock()
        download_mock.readall = AsyncMock(return_value=raw)
        download_mock.properties = {
            "content_settings": {"content_type": "text/plain"},
            "etag": '"0xABC"',
            "last_modified": datetime(2026, 3, 1, tzinfo=UTC),
        }

        blob_client = AsyncMock()
        blob_client.download_blob = AsyncMock(return_value=download_mock)

        service = MagicMock()
        _service_ctx(service)
        service.get_blob_client = MagicMock(return_value=blob_client)

        with patch(MOCK_READ, return_value=service):
            block = BlobReadBlock()
            result = await block.execute(
                BlobReadInput(
                    connection_string="DefaultEndpointsProtocol=https;",
                    container="mycontainer",
                    blob_name="data/file.txt",
                )
            )

        assert isinstance(result, BlobReadOutput)
        assert result.content == "hello world"
        assert result.content_length == 11
        assert result.content_type == "text/plain"
        assert result.blob_name == "data/file.txt"
        assert result.container == "mycontainer"
        assert result.etag == '"0xABC"'
        assert "2026-03-01" in result.last_modified

    async def test_read_binary(self) -> None:
        from llming_plumber.blocks.azure.blob_read import (
            BlobReadBlock,
            BlobReadInput,
        )

        raw = b"\x89PNG\r\n\x1a\n"
        download_mock = AsyncMock()
        download_mock.readall = AsyncMock(return_value=raw)
        download_mock.properties = {
            "content_settings": {"content_type": "image/png"},
            "etag": '"0xDEF"',
        }

        blob_client = AsyncMock()
        blob_client.download_blob = AsyncMock(return_value=download_mock)

        service = MagicMock()
        _service_ctx(service)
        service.get_blob_client = MagicMock(return_value=blob_client)

        with patch(MOCK_READ, return_value=service):
            block = BlobReadBlock()
            result = await block.execute(
                BlobReadInput(
                    connection_string="DefaultEndpointsProtocol=https;",
                    container="images",
                    blob_name="logo.png",
                    encoding="binary",
                )
            )

        assert result.content == base64.b64encode(raw).decode("ascii")
        assert result.content_length == len(raw)
        assert result.content_type == "image/png"

    async def test_read_missing_last_modified(self) -> None:
        from llming_plumber.blocks.azure.blob_read import (
            BlobReadBlock,
            BlobReadInput,
        )

        download_mock = AsyncMock()
        download_mock.readall = AsyncMock(return_value=b"ok")
        download_mock.properties = {}

        blob_client = AsyncMock()
        blob_client.download_blob = AsyncMock(return_value=download_mock)

        service = MagicMock()
        _service_ctx(service)
        service.get_blob_client = MagicMock(return_value=blob_client)

        with patch(MOCK_READ, return_value=service):
            block = BlobReadBlock()
            result = await block.execute(
                BlobReadInput(
                    connection_string="DefaultEndpointsProtocol=https;",
                    container="c",
                    blob_name="b",
                )
            )

        assert result.last_modified == ""
        assert result.etag == ""
        assert result.content_type == "application/octet-stream"


# ---------------------------------------------------------------------------
# BlobWriteBlock
# ---------------------------------------------------------------------------


class TestBlobWriteBlock:
    async def test_write_text(self) -> None:
        from llming_plumber.blocks.azure.blob_write import (
            BlobWriteBlock,
            BlobWriteInput,
            BlobWriteOutput,
        )

        blob_client = AsyncMock()
        blob_client.upload_blob = AsyncMock(
            return_value={"etag": '"0x999"'}
        )
        blob_client.url = "https://myaccount.blob.core.windows.net/c/b.json"

        service = MagicMock()
        _service_ctx(service)
        service.get_blob_client = MagicMock(return_value=blob_client)

        with patch(MOCK_WRITE, return_value=service):
            block = BlobWriteBlock()
            result = await block.execute(
                BlobWriteInput(
                    connection_string="DefaultEndpointsProtocol=https;",
                    container="c",
                    blob_name="b.json",
                    content='{"key": "value"}',
                    content_type="application/json",
                )
            )

        assert isinstance(result, BlobWriteOutput)
        assert result.blob_name == "b.json"
        assert result.container == "c"
        assert result.etag == '"0x999"'
        assert result.content_length == len(b'{"key": "value"}')
        assert "blob.core.windows.net" in result.url

        # Verify upload was called with overwrite=True (default)
        blob_client.upload_blob.assert_called_once()
        call_kwargs = blob_client.upload_blob.call_args
        assert call_kwargs.kwargs["overwrite"] is True

    async def test_write_binary(self) -> None:
        from llming_plumber.blocks.azure.blob_write import (
            BlobWriteBlock,
            BlobWriteInput,
        )

        raw_bytes = b"\x00\x01\x02\x03"
        b64_content = base64.b64encode(raw_bytes).decode("ascii")

        blob_client = AsyncMock()
        blob_client.upload_blob = AsyncMock(
            return_value={"etag": '"0xBIN"'}
        )
        blob_client.url = "https://a.blob.core.windows.net/c/bin"

        service = MagicMock()
        _service_ctx(service)
        service.get_blob_client = MagicMock(return_value=blob_client)

        with patch(MOCK_WRITE, return_value=service):
            block = BlobWriteBlock()
            result = await block.execute(
                BlobWriteInput(
                    connection_string="DefaultEndpointsProtocol=https;",
                    container="c",
                    blob_name="data.bin",
                    content=b64_content,
                    encoding="binary",
                )
            )

        assert result.content_length == len(raw_bytes)
        # Verify the actual bytes (not base64) were uploaded
        uploaded_data = blob_client.upload_blob.call_args.args[0]
        assert uploaded_data == raw_bytes

    async def test_write_no_overwrite(self) -> None:
        from llming_plumber.blocks.azure.blob_write import (
            BlobWriteBlock,
            BlobWriteInput,
        )

        blob_client = AsyncMock()
        blob_client.upload_blob = AsyncMock(return_value={"etag": '"0x1"'})
        blob_client.url = "https://a.blob.core.windows.net/c/f"

        service = MagicMock()
        _service_ctx(service)
        service.get_blob_client = MagicMock(return_value=blob_client)

        with patch(MOCK_WRITE, return_value=service):
            block = BlobWriteBlock()
            await block.execute(
                BlobWriteInput(
                    connection_string="DefaultEndpointsProtocol=https;",
                    container="c",
                    blob_name="f",
                    content="x",
                    overwrite=False,
                )
            )

        call_kwargs = blob_client.upload_blob.call_args
        assert call_kwargs.kwargs["overwrite"] is False


# ---------------------------------------------------------------------------
# BlobListBlock
# ---------------------------------------------------------------------------


class TestBlobListBlock:
    async def test_list_blobs(self) -> None:
        from llming_plumber.blocks.azure.blob_list import (
            BlobListBlock,
            BlobListInput,
            BlobListOutput,
        )

        blob_a = _make_blob_props(
            etag='"0xA"', content_type="text/plain", size=100
        )
        blob_a.name = "docs/readme.txt"
        blob_b = _make_blob_props(
            etag='"0xB"', content_type="application/json", size=200
        )
        blob_b.name = "data/records.json"

        container_client = MagicMock()
        container_client.list_blobs = MagicMock(
            return_value=_AsyncBlobIter([blob_a, blob_b])
        )

        service = MagicMock()
        _service_ctx(service)
        service.get_container_client = MagicMock(
            return_value=container_client
        )

        with patch(MOCK_LIST, return_value=service):
            block = BlobListBlock()
            result = await block.execute(
                BlobListInput(
                    connection_string="DefaultEndpointsProtocol=https;",
                    container="mycontainer",
                )
            )

        assert isinstance(result, BlobListOutput)
        assert result.count == 2
        assert result.container == "mycontainer"
        assert result.blobs[0]["name"] == "docs/readme.txt"
        assert result.blobs[0]["size"] == 100
        assert result.blobs[1]["content_type"] == "application/json"

    async def test_list_with_prefix(self) -> None:
        from llming_plumber.blocks.azure.blob_list import (
            BlobListBlock,
            BlobListInput,
        )

        blob = _make_blob_props()
        blob.name = "logs/2026-03-07.log"

        container_client = MagicMock()
        container_client.list_blobs = MagicMock(
            return_value=_AsyncBlobIter([blob])
        )

        service = MagicMock()
        _service_ctx(service)
        service.get_container_client = MagicMock(
            return_value=container_client
        )

        with patch(MOCK_LIST, return_value=service):
            block = BlobListBlock()
            result = await block.execute(
                BlobListInput(
                    connection_string="DefaultEndpointsProtocol=https;",
                    container="c",
                    prefix="logs/",
                )
            )

        assert result.count == 1
        assert result.blobs[0]["name"] == "logs/2026-03-07.log"
        container_client.list_blobs.assert_called_once_with(
            name_starts_with="logs/"
        )

    async def test_list_empty_prefix_passes_none(self) -> None:
        from llming_plumber.blocks.azure.blob_list import (
            BlobListBlock,
            BlobListInput,
        )

        container_client = MagicMock()
        container_client.list_blobs = MagicMock(
            return_value=_AsyncBlobIter([])
        )

        service = MagicMock()
        _service_ctx(service)
        service.get_container_client = MagicMock(
            return_value=container_client
        )

        with patch(MOCK_LIST, return_value=service):
            block = BlobListBlock()
            result = await block.execute(
                BlobListInput(
                    connection_string="DefaultEndpointsProtocol=https;",
                    container="c",
                )
            )

        assert result.count == 0
        container_client.list_blobs.assert_called_once_with(
            name_starts_with=None
        )

    async def test_list_max_results(self) -> None:
        from llming_plumber.blocks.azure.blob_list import (
            BlobListBlock,
            BlobListInput,
        )

        blobs = []
        for i in range(5):
            b = _make_blob_props()
            b.name = f"file_{i}.txt"
            blobs.append(b)

        container_client = MagicMock()
        container_client.list_blobs = MagicMock(
            return_value=_AsyncBlobIter(blobs)
        )

        service = MagicMock()
        _service_ctx(service)
        service.get_container_client = MagicMock(
            return_value=container_client
        )

        with patch(MOCK_LIST, return_value=service):
            block = BlobListBlock()
            result = await block.execute(
                BlobListInput(
                    connection_string="DefaultEndpointsProtocol=https;",
                    container="c",
                    max_results=3,
                )
            )

        assert result.count == 3

    async def test_list_blob_without_content_settings(self) -> None:
        from llming_plumber.blocks.azure.blob_list import (
            BlobListBlock,
            BlobListInput,
        )

        blob = SimpleNamespace(
            name="raw.bin",
            etag='"0x1"',
            size=42,
            last_modified=None,
            content_settings=None,
        )

        container_client = MagicMock()
        container_client.list_blobs = MagicMock(
            return_value=_AsyncBlobIter([blob])
        )

        service = MagicMock()
        _service_ctx(service)
        service.get_container_client = MagicMock(
            return_value=container_client
        )

        with patch(MOCK_LIST, return_value=service):
            block = BlobListBlock()
            result = await block.execute(
                BlobListInput(
                    connection_string="DefaultEndpointsProtocol=https;",
                    container="c",
                )
            )

        assert result.blobs[0]["content_type"] == "application/octet-stream"
        assert result.blobs[0]["last_modified"] == ""


# ---------------------------------------------------------------------------
# BlobDeleteBlock
# ---------------------------------------------------------------------------


class TestBlobDeleteBlock:
    async def test_delete_blob(self) -> None:
        from llming_plumber.blocks.azure.blob_delete import (
            BlobDeleteBlock,
            BlobDeleteInput,
            BlobDeleteOutput,
        )

        blob_client = AsyncMock()
        blob_client.delete_blob = AsyncMock(return_value=None)

        service = MagicMock()
        _service_ctx(service)
        service.get_blob_client = MagicMock(return_value=blob_client)

        with patch(MOCK_DELETE, return_value=service):
            block = BlobDeleteBlock()
            result = await block.execute(
                BlobDeleteInput(
                    connection_string="DefaultEndpointsProtocol=https;",
                    container="mycontainer",
                    blob_name="old/file.txt",
                )
            )

        assert isinstance(result, BlobDeleteOutput)
        assert result.blob_name == "old/file.txt"
        assert result.container == "mycontainer"
        assert result.deleted is True
        blob_client.delete_blob.assert_called_once_with(
            delete_snapshots="include"
        )

    async def test_delete_snapshots_only(self) -> None:
        from llming_plumber.blocks.azure.blob_delete import (
            BlobDeleteBlock,
            BlobDeleteInput,
        )

        blob_client = AsyncMock()
        blob_client.delete_blob = AsyncMock(return_value=None)

        service = MagicMock()
        _service_ctx(service)
        service.get_blob_client = MagicMock(return_value=blob_client)

        with patch(MOCK_DELETE, return_value=service):
            block = BlobDeleteBlock()
            await block.execute(
                BlobDeleteInput(
                    connection_string="DefaultEndpointsProtocol=https;",
                    container="c",
                    blob_name="b",
                    delete_snapshots="only",
                )
            )

        blob_client.delete_blob.assert_called_once_with(
            delete_snapshots="only"
        )


# ---------------------------------------------------------------------------
# BlobTriggerBlock
# ---------------------------------------------------------------------------


class TestBlobTriggerBlock:
    def _make_trigger_service(
        self, blobs: list[SimpleNamespace]
    ) -> MagicMock:
        container_client = MagicMock()
        container_client.list_blobs = MagicMock(
            return_value=_AsyncBlobIter(blobs)
        )
        service = MagicMock()
        _service_ctx(service)
        service.get_container_client = MagicMock(
            return_value=container_client
        )
        return service

    async def test_first_run_all_created(self) -> None:
        """First run with empty previous_state: all blobs are 'created'."""
        from llming_plumber.blocks.azure.blob_trigger import (
            BlobTriggerBlock,
            BlobTriggerInput,
            BlobTriggerOutput,
        )

        blob_a = _make_blob_props(etag='"0xA"', size=100)
        blob_a.name = "file_a.txt"
        blob_b = _make_blob_props(etag='"0xB"', size=200)
        blob_b.name = "file_b.txt"

        service = self._make_trigger_service([blob_a, blob_b])

        with patch(MOCK_TRIGGER, return_value=service):
            block = BlobTriggerBlock()
            result = await block.execute(
                BlobTriggerInput(
                    connection_string="DefaultEndpointsProtocol=https;",
                    container="watched",
                    events=["created", "modified", "deleted"],
                )
            )

        assert isinstance(result, BlobTriggerOutput)
        assert result.container == "watched"
        assert len(result.events) == 2
        assert all(e["event"] == "created" for e in result.events)
        names = {e["blob_name"] for e in result.events}
        assert names == {"file_a.txt", "file_b.txt"}

        # current_state should map names to etags
        assert result.current_state == {
            "file_a.txt": '"0xA"',
            "file_b.txt": '"0xB"',
        }
        assert result.checked_at != ""

    async def test_modified_blob(self) -> None:
        """Blob with changed etag is detected as 'modified'."""
        from llming_plumber.blocks.azure.blob_trigger import (
            BlobTriggerBlock,
            BlobTriggerInput,
        )

        blob = _make_blob_props(etag='"0xNEW"', size=150)
        blob.name = "data.csv"

        service = self._make_trigger_service([blob])

        previous_state = {"data.csv": '"0xOLD"'}

        with patch(MOCK_TRIGGER, return_value=service):
            block = BlobTriggerBlock()
            result = await block.execute(
                BlobTriggerInput(
                    connection_string="DefaultEndpointsProtocol=https;",
                    container="c",
                    events=["created", "modified", "deleted"],
                    previous_state=previous_state,
                )
            )

        assert len(result.events) == 1
        assert result.events[0]["event"] == "modified"
        assert result.events[0]["blob_name"] == "data.csv"
        assert result.events[0]["etag"] == '"0xNEW"'

    async def test_deleted_blob(self) -> None:
        """Blob in previous_state but not in current listing = 'deleted'."""
        from llming_plumber.blocks.azure.blob_trigger import (
            BlobTriggerBlock,
            BlobTriggerInput,
        )

        # Current listing is empty — the blob was deleted
        service = self._make_trigger_service([])

        previous_state = {"gone.txt": '"0xWAS"'}

        with patch(MOCK_TRIGGER, return_value=service):
            block = BlobTriggerBlock()
            result = await block.execute(
                BlobTriggerInput(
                    connection_string="DefaultEndpointsProtocol=https;",
                    container="c",
                    events=["created", "modified", "deleted"],
                    previous_state=previous_state,
                )
            )

        assert len(result.events) == 1
        assert result.events[0]["event"] == "deleted"
        assert result.events[0]["blob_name"] == "gone.txt"
        assert result.events[0]["etag"] == '"0xWAS"'
        assert result.events[0]["size"] == 0

        # Deleted blob should not be in current_state
        assert "gone.txt" not in result.current_state

    async def test_unchanged_blob_no_events(self) -> None:
        """Blob with same etag produces no events."""
        from llming_plumber.blocks.azure.blob_trigger import (
            BlobTriggerBlock,
            BlobTriggerInput,
        )

        blob = _make_blob_props(etag='"0xSAME"')
        blob.name = "stable.txt"

        service = self._make_trigger_service([blob])

        with patch(MOCK_TRIGGER, return_value=service):
            block = BlobTriggerBlock()
            result = await block.execute(
                BlobTriggerInput(
                    connection_string="DefaultEndpointsProtocol=https;",
                    container="c",
                    events=["created", "modified", "deleted"],
                    previous_state={"stable.txt": '"0xSAME"'},
                )
            )

        assert len(result.events) == 0

    async def test_event_filtering(self) -> None:
        """Only requested event types are emitted."""
        from llming_plumber.blocks.azure.blob_trigger import (
            BlobTriggerBlock,
            BlobTriggerInput,
        )

        # New blob (would be "created") and deleted blob
        blob = _make_blob_props(etag='"0xNEW"')
        blob.name = "new.txt"

        service = self._make_trigger_service([blob])

        # previous_state has a blob that is now gone (= deleted)
        # and new.txt is not in previous_state (= created)
        previous_state = {"old.txt": '"0xOLD"'}

        # Only subscribe to "deleted" events
        with patch(MOCK_TRIGGER, return_value=service):
            block = BlobTriggerBlock()
            result = await block.execute(
                BlobTriggerInput(
                    connection_string="DefaultEndpointsProtocol=https;",
                    container="c",
                    events=["deleted"],
                    previous_state=previous_state,
                )
            )

        # Only the deleted event should be emitted, not "created"
        assert len(result.events) == 1
        assert result.events[0]["event"] == "deleted"
        assert result.events[0]["blob_name"] == "old.txt"

    async def test_mixed_events(self) -> None:
        """Combination of created, modified, deleted in one poll."""
        from llming_plumber.blocks.azure.blob_trigger import (
            BlobTriggerBlock,
            BlobTriggerInput,
        )

        blob_new = _make_blob_props(etag='"0xNEW"')
        blob_new.name = "brand_new.txt"
        blob_mod = _make_blob_props(etag='"0xCHANGED"')
        blob_mod.name = "existing.txt"
        blob_same = _make_blob_props(etag='"0xSAME"')
        blob_same.name = "stable.txt"

        service = self._make_trigger_service(
            [blob_new, blob_mod, blob_same]
        )

        previous_state = {
            "existing.txt": '"0xORIGINAL"',
            "stable.txt": '"0xSAME"',
            "removed.txt": '"0xGONE"',
        }

        with patch(MOCK_TRIGGER, return_value=service):
            block = BlobTriggerBlock()
            result = await block.execute(
                BlobTriggerInput(
                    connection_string="DefaultEndpointsProtocol=https;",
                    container="c",
                    events=["created", "modified", "deleted"],
                    previous_state=previous_state,
                )
            )

        events_by_type = {}
        for e in result.events:
            events_by_type[e["blob_name"]] = e["event"]

        assert events_by_type == {
            "brand_new.txt": "created",
            "existing.txt": "modified",
            "removed.txt": "deleted",
        }
        assert len(result.events) == 3

    async def test_trigger_with_prefix(self) -> None:
        """Prefix is forwarded to list_blobs."""
        from llming_plumber.blocks.azure.blob_trigger import (
            BlobTriggerBlock,
            BlobTriggerInput,
        )

        service = self._make_trigger_service([])
        container_client = service.get_container_client.return_value

        with patch(MOCK_TRIGGER, return_value=service):
            block = BlobTriggerBlock()
            await block.execute(
                BlobTriggerInput(
                    connection_string="DefaultEndpointsProtocol=https;",
                    container="c",
                    prefix="inbox/",
                )
            )

        container_client.list_blobs.assert_called_once_with(
            name_starts_with="inbox/"
        )


# ---------------------------------------------------------------------------
# _storage.get_blob_service
# ---------------------------------------------------------------------------


class TestGetBlobService:
    def test_missing_connection_string_raises(self) -> None:
        from llming_plumber.blocks.azure._storage import get_blob_service

        with (
            patch.dict("os.environ", {}, clear=True),
            pytest.raises(ValueError, match="No connection string"),
        ):
            get_blob_service("")

    def test_env_fallback(self) -> None:
        from llming_plumber.blocks.azure._storage import get_blob_service

        with (
            patch.dict(
                "os.environ",
                {"AZURE_STORAGE_CONNECTION_STRING": "FakeConn"},
            ),
            patch(
                "llming_plumber.blocks.azure._storage"
                ".BlobServiceClient.from_connection_string",
            ) as mock_from,
        ):
            mock_from.return_value = MagicMock()
            get_blob_service("")
            mock_from.assert_called_once_with("FakeConn")
