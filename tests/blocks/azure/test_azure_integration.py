"""Integration tests for Azure Blob Storage blocks — calls real Azure APIs.

Run with:
    pytest tests/blocks/azure/test_azure_integration.py -m integration -v

Requires AZURE_STORAGE_CONNECTION_STRING in .env.
These tests are NOT run in CI — they are for local verification only.

Test containers (must already exist):
    - plumber-test-data:    has test-data.json and sample.csv
    - plumber-test-triggers: empty, used for trigger lifecycle
    - plumber-test-output:   empty, used for write/delete tests
"""

from __future__ import annotations

import json
import os
import uuid

import pytest

from llming_plumber.blocks.azure.blob_delete import (
    BlobDeleteBlock,
    BlobDeleteInput,
)
from llming_plumber.blocks.azure.blob_list import (
    BlobListBlock,
    BlobListInput,
)
from llming_plumber.blocks.azure.blob_read import (
    BlobReadBlock,
    BlobReadInput,
)
from llming_plumber.blocks.azure.blob_trigger import (
    BlobTriggerBlock,
    BlobTriggerInput,
)
from llming_plumber.blocks.azure.blob_write import (
    BlobWriteBlock,
    BlobWriteInput,
)

pytestmark = pytest.mark.integration

CONTAINER_DATA = "plumber-test-data"
CONTAINER_OUTPUT = "plumber-test-output"
CONTAINER_TRIGGERS = "plumber-test-triggers"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def conn_str() -> str:
    val = os.environ.get("AZURE_STORAGE_CONNECTION_STRING", "")
    if not val:
        pytest.skip("AZURE_STORAGE_CONNECTION_STRING not set")
    return val


@pytest.fixture()
def unique_blob_name() -> str:
    """Return a unique blob name to avoid collisions between test runs."""
    return f"test-{uuid.uuid4().hex[:12]}.txt"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _write_blob(
    conn_str: str,
    container: str,
    blob_name: str,
    content: str,
    *,
    content_type: str = "text/plain",
    overwrite: bool = True,
) -> None:
    block = BlobWriteBlock()
    await block.execute(
        BlobWriteInput(
            connection_string=conn_str,
            container=container,
            blob_name=blob_name,
            content=content,
            content_type=content_type,
            overwrite=overwrite,
        ),
    )


async def _delete_blob_safe(
    conn_str: str,
    container: str,
    blob_name: str,
) -> None:
    """Delete a blob, ignoring errors if it does not exist."""
    try:
        block = BlobDeleteBlock()
        await block.execute(
            BlobDeleteInput(
                connection_string=conn_str,
                container=container,
                blob_name=blob_name,
            ),
        )
    except Exception:  # noqa: BLE001
        pass


async def _list_blob_names(
    conn_str: str,
    container: str,
    prefix: str = "",
) -> list[str]:
    block = BlobListBlock()
    result = await block.execute(
        BlobListInput(
            connection_string=conn_str,
            container=container,
            prefix=prefix,
        ),
    )
    return [b["name"] for b in result.blobs]


# ---------------------------------------------------------------------------
# Read tests
# ---------------------------------------------------------------------------


class TestBlobRead:
    async def test_read_json(self, conn_str: str) -> None:
        block = BlobReadBlock()
        result = await block.execute(
            BlobReadInput(
                connection_string=conn_str,
                container=CONTAINER_DATA,
                blob_name="test-data.json",
            ),
        )

        assert result.blob_name == "test-data.json"
        assert result.container == CONTAINER_DATA
        assert result.content_length > 0
        # Verify it is valid JSON
        parsed = json.loads(result.content)
        assert isinstance(parsed, (dict, list))

    async def test_read_csv(self, conn_str: str) -> None:
        block = BlobReadBlock()
        result = await block.execute(
            BlobReadInput(
                connection_string=conn_str,
                container=CONTAINER_DATA,
                blob_name="sample.csv",
            ),
        )

        assert result.blob_name == "sample.csv"
        assert result.container == CONTAINER_DATA
        assert result.content_length > 0
        # CSV should have at least a header line
        lines = result.content.strip().splitlines()
        assert len(lines) >= 1

    async def test_read_metadata_fields(self, conn_str: str) -> None:
        block = BlobReadBlock()
        result = await block.execute(
            BlobReadInput(
                connection_string=conn_str,
                container=CONTAINER_DATA,
                blob_name="test-data.json",
            ),
        )

        assert result.etag != ""
        assert result.content_length > 0
        assert result.last_modified != ""


# ---------------------------------------------------------------------------
# Write tests
# ---------------------------------------------------------------------------


class TestBlobWrite:
    async def test_write_json_roundtrip(
        self, conn_str: str, unique_blob_name: str,
    ) -> None:
        blob_name = unique_blob_name.replace(".txt", ".json")
        payload = {"key": "value", "number": 42}
        try:
            write_block = BlobWriteBlock()
            write_result = await write_block.execute(
                BlobWriteInput(
                    connection_string=conn_str,
                    container=CONTAINER_OUTPUT,
                    blob_name=blob_name,
                    content=json.dumps(payload),
                    content_type="application/json",
                ),
            )

            assert write_result.etag != ""
            assert write_result.url != ""
            assert write_result.content_length > 0
            assert "llmingdatastore" in write_result.url

            # Read it back
            read_block = BlobReadBlock()
            read_result = await read_block.execute(
                BlobReadInput(
                    connection_string=conn_str,
                    container=CONTAINER_OUTPUT,
                    blob_name=blob_name,
                ),
            )

            assert json.loads(read_result.content) == payload
        finally:
            await _delete_blob_safe(conn_str, CONTAINER_OUTPUT, blob_name)

    async def test_write_text_content_type(
        self, conn_str: str, unique_blob_name: str,
    ) -> None:
        try:
            block = BlobWriteBlock()
            result = await block.execute(
                BlobWriteInput(
                    connection_string=conn_str,
                    container=CONTAINER_OUTPUT,
                    blob_name=unique_blob_name,
                    content="Hello, Azure!",
                    content_type="text/plain",
                ),
            )

            assert result.etag != ""
            assert result.url != ""

            # Verify content type via read
            read_block = BlobReadBlock()
            read_result = await read_block.execute(
                BlobReadInput(
                    connection_string=conn_str,
                    container=CONTAINER_OUTPUT,
                    blob_name=unique_blob_name,
                ),
            )
            assert read_result.content_type == "text/plain"
        finally:
            await _delete_blob_safe(
                conn_str, CONTAINER_OUTPUT, unique_blob_name,
            )

    async def test_write_overwrite(
        self, conn_str: str, unique_blob_name: str,
    ) -> None:
        try:
            await _write_blob(
                conn_str,
                CONTAINER_OUTPUT,
                unique_blob_name,
                "first version",
            )

            block = BlobWriteBlock()
            result = await block.execute(
                BlobWriteInput(
                    connection_string=conn_str,
                    container=CONTAINER_OUTPUT,
                    blob_name=unique_blob_name,
                    content="second version",
                    overwrite=True,
                ),
            )

            assert result.etag != ""

            read_block = BlobReadBlock()
            read_result = await read_block.execute(
                BlobReadInput(
                    connection_string=conn_str,
                    container=CONTAINER_OUTPUT,
                    blob_name=unique_blob_name,
                ),
            )
            assert read_result.content == "second version"
        finally:
            await _delete_blob_safe(
                conn_str, CONTAINER_OUTPUT, unique_blob_name,
            )

    async def test_write_output_fields(
        self, conn_str: str, unique_blob_name: str,
    ) -> None:
        try:
            block = BlobWriteBlock()
            result = await block.execute(
                BlobWriteInput(
                    connection_string=conn_str,
                    container=CONTAINER_OUTPUT,
                    blob_name=unique_blob_name,
                    content="check output fields",
                ),
            )

            assert result.url != ""
            assert result.etag != ""
            assert result.blob_name == unique_blob_name
            assert result.container == CONTAINER_OUTPUT
            assert result.content_length == len(
                b"check output fields",
            )
        finally:
            await _delete_blob_safe(
                conn_str, CONTAINER_OUTPUT, unique_blob_name,
            )


# ---------------------------------------------------------------------------
# List tests
# ---------------------------------------------------------------------------


class TestBlobList:
    async def test_list_data_container(self, conn_str: str) -> None:
        block = BlobListBlock()
        result = await block.execute(
            BlobListInput(
                connection_string=conn_str,
                container=CONTAINER_DATA,
            ),
        )

        names = [b["name"] for b in result.blobs]
        assert "test-data.json" in names
        assert "sample.csv" in names
        assert result.container == CONTAINER_DATA

    async def test_list_with_prefix(self, conn_str: str) -> None:
        block = BlobListBlock()
        result = await block.execute(
            BlobListInput(
                connection_string=conn_str,
                container=CONTAINER_DATA,
                prefix="test-",
            ),
        )

        names = [b["name"] for b in result.blobs]
        assert "test-data.json" in names
        assert "sample.csv" not in names

    async def test_list_max_results(self, conn_str: str) -> None:
        block = BlobListBlock()
        result = await block.execute(
            BlobListInput(
                connection_string=conn_str,
                container=CONTAINER_DATA,
                max_results=1,
            ),
        )

        assert result.count == 1
        assert len(result.blobs) == 1


# ---------------------------------------------------------------------------
# Delete tests
# ---------------------------------------------------------------------------


class TestBlobDelete:
    async def test_write_then_delete(
        self, conn_str: str, unique_blob_name: str,
    ) -> None:
        # Write a blob
        await _write_blob(
            conn_str,
            CONTAINER_OUTPUT,
            unique_blob_name,
            "to be deleted",
        )

        # Confirm it exists
        names = await _list_blob_names(conn_str, CONTAINER_OUTPUT)
        assert unique_blob_name in names

        # Delete it
        block = BlobDeleteBlock()
        result = await block.execute(
            BlobDeleteInput(
                connection_string=conn_str,
                container=CONTAINER_OUTPUT,
                blob_name=unique_blob_name,
            ),
        )

        assert result.deleted is True
        assert result.blob_name == unique_blob_name

        # Confirm it is gone
        names = await _list_blob_names(conn_str, CONTAINER_OUTPUT)
        assert unique_blob_name not in names


# ---------------------------------------------------------------------------
# Trigger tests — full lifecycle
# ---------------------------------------------------------------------------


class TestBlobTrigger:
    async def test_trigger_lifecycle(self, conn_str: str) -> None:
        """Full trigger lifecycle: empty -> create -> modify -> delete."""
        blob_name = f"trigger-{uuid.uuid4().hex[:12]}.txt"
        trigger = BlobTriggerBlock()

        try:
            # --- Step 1: First poll on empty container → no events ---------
            result_empty = await trigger.execute(
                BlobTriggerInput(
                    connection_string=conn_str,
                    container=CONTAINER_TRIGGERS,
                    events=["created", "modified", "deleted"],
                    previous_state={},
                ),
            )

            assert result_empty.events == []
            assert result_empty.container == CONTAINER_TRIGGERS
            assert result_empty.checked_at != ""
            state_0 = result_empty.current_state

            # --- Step 2: Upload a blob → "created" event -------------------
            await _write_blob(
                conn_str,
                CONTAINER_TRIGGERS,
                blob_name,
                "version 1",
            )

            result_created = await trigger.execute(
                BlobTriggerInput(
                    connection_string=conn_str,
                    container=CONTAINER_TRIGGERS,
                    events=["created", "modified", "deleted"],
                    previous_state=state_0,
                ),
            )

            created_events = [
                e for e in result_created.events
                if e["event"] == "created"
            ]
            assert len(created_events) == 1
            assert created_events[0]["blob_name"] == blob_name
            state_1 = result_created.current_state

            # --- Step 3: Modify the blob → "modified" event ----------------
            await _write_blob(
                conn_str,
                CONTAINER_TRIGGERS,
                blob_name,
                "version 2",
            )

            result_modified = await trigger.execute(
                BlobTriggerInput(
                    connection_string=conn_str,
                    container=CONTAINER_TRIGGERS,
                    events=["created", "modified", "deleted"],
                    previous_state=state_1,
                ),
            )

            modified_events = [
                e for e in result_modified.events
                if e["event"] == "modified"
            ]
            assert len(modified_events) == 1
            assert modified_events[0]["blob_name"] == blob_name
            state_2 = result_modified.current_state

            # --- Step 4: Delete the blob → "deleted" event -----------------
            delete_block = BlobDeleteBlock()
            await delete_block.execute(
                BlobDeleteInput(
                    connection_string=conn_str,
                    container=CONTAINER_TRIGGERS,
                    blob_name=blob_name,
                ),
            )

            result_deleted = await trigger.execute(
                BlobTriggerInput(
                    connection_string=conn_str,
                    container=CONTAINER_TRIGGERS,
                    events=["created", "modified", "deleted"],
                    previous_state=state_2,
                ),
            )

            deleted_events = [
                e for e in result_deleted.events
                if e["event"] == "deleted"
            ]
            assert len(deleted_events) == 1
            assert deleted_events[0]["blob_name"] == blob_name
        finally:
            # Safety cleanup in case test failed before deletion step
            await _delete_blob_safe(
                conn_str, CONTAINER_TRIGGERS, blob_name,
            )
