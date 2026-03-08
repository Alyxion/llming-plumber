"""Integration tests for FileRef flowing through block chains.

These tests exercise real block execution with FileRef objects passing
between blocks — file_ops, archive, and redis_ops.

Requires: Redis running (for redis file store/load tests).
"""

from __future__ import annotations

import base64
import io
import json
import zipfile

import pytest

from llming_plumber.models.file_ref import FileRef


# ── FileRead → ZipCreate via FileRef ──


@pytest.mark.integration
@pytest.mark.asyncio
async def test_file_read_produces_file_ref(tmp_path: object) -> None:
    """FileRead should populate file_ref in its output."""
    import pathlib

    from llming_plumber.blocks.data.file_ops import FileReadBlock, FileReadInput

    p = pathlib.Path(str(tmp_path)) / "hello.txt"
    p.write_text("Hello, FileRef!")

    block = FileReadBlock()
    result = await block.execute(FileReadInput(path=str(p)))
    assert result.content == "Hello, FileRef!"
    assert result.file_ref is not None
    ref = FileRef(**result.file_ref) if isinstance(result.file_ref, dict) else result.file_ref
    assert ref.filename == "hello.txt"
    assert ref.mime_type == "text/plain"
    assert ref.decode() == b"Hello, FileRef!"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_file_write_from_file_ref(tmp_path: object) -> None:
    """FileWrite should accept a file_ref and write its content."""
    import pathlib

    from llming_plumber.blocks.data.file_ops import FileWriteBlock, FileWriteInput

    ref = FileRef.from_bytes(b"Written via FileRef", "output.txt")
    out_path = pathlib.Path(str(tmp_path)) / "output.txt"

    block = FileWriteBlock()
    result = await block.execute(FileWriteInput(
        path=str(out_path),
        content="",
        file_ref=ref.model_dump(),
    ))
    assert result.created
    assert out_path.read_text() == "Written via FileRef"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_zip_create_from_file_refs() -> None:
    """ZipCreate should accept file_refs list."""
    from llming_plumber.blocks.data.archive import ZipCreateBlock, ZipCreateInput

    refs = [
        FileRef.from_bytes(b"File A content", "a.txt").model_dump(),
        FileRef.from_bytes(b"File B content", "b.txt").model_dump(),
    ]
    block = ZipCreateBlock()
    result = await block.execute(ZipCreateInput(
        files="[]",
        archive_name="test.zip",
        file_refs=refs,
    ))
    assert result.file_count == 2
    raw = base64.b64decode(result.archive_base64)
    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
        assert zf.read("a.txt") == b"File A content"
        assert zf.read("b.txt") == b"File B content"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_zip_extract_produces_file_refs() -> None:
    """ZipExtract should populate file_refs in output."""
    from llming_plumber.blocks.data.archive import (
        ZipCreateBlock,
        ZipCreateInput,
        ZipExtractBlock,
        ZipExtractInput,
    )

    # Create a zip first
    refs = [
        FileRef.from_bytes(b"Content A", "doc.txt").model_dump(),
        FileRef.from_bytes(b"Content B", "img.png").model_dump(),
    ]
    create_block = ZipCreateBlock()
    created = await create_block.execute(ZipCreateInput(
        files="[]", archive_name="test.zip", file_refs=refs,
    ))

    # Extract it
    extract_block = ZipExtractBlock()
    extracted = await extract_block.execute(ZipExtractInput(
        archive_base64=created.archive_base64,
    ))
    assert extracted.file_count == 2
    assert len(extracted.file_refs) == 2

    # Verify file_refs are valid FileRef dicts
    for ref_dict in extracted.file_refs:
        ref = FileRef(**ref_dict)
        assert ref.filename
        assert ref.data
        assert ref.size_bytes > 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_zip_roundtrip_via_file_refs() -> None:
    """Create from FileRefs, extract, verify content matches."""
    from llming_plumber.blocks.data.archive import (
        ZipCreateBlock,
        ZipCreateInput,
        ZipExtractBlock,
        ZipExtractInput,
    )

    originals = {
        "readme.md": b"# Hello\nThis is a readme.",
        "data.json": json.dumps({"key": "value"}).encode(),
    }
    refs = [FileRef.from_bytes(v, k).model_dump() for k, v in originals.items()]

    create = ZipCreateBlock()
    archive = await create.execute(ZipCreateInput(
        files="[]", archive_name="roundtrip.zip", file_refs=refs,
    ))

    extract = ZipExtractBlock()
    result = await extract.execute(ZipExtractInput(
        archive_base64=archive.archive_base64,
    ))

    extracted_map = {
        FileRef(**r).filename: FileRef(**r).decode()
        for r in result.file_refs
    }
    for name, content in originals.items():
        assert extracted_map[name] == content


# ── Redis FileRef store/load ──


def _reset_redis_client() -> None:
    """Reset the cached Redis singleton so each test gets a fresh connection."""
    import llming_plumber.db as db_mod
    db_mod._redis_client = None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_file_store_and_load() -> None:
    """Store a FileRef in Redis, load it back, verify content."""
    _reset_redis_client()
    from llming_plumber.blocks.data.redis_ops import (
        RedisFileLoadBlock,
        RedisFileLoadInput,
        RedisFileStoreBlock,
        RedisFileStoreInput,
    )

    ref = FileRef.from_bytes(b"cached file content", "cached.txt")
    key = "plumber:test:fileref:integration"

    # Store
    store_block = RedisFileStoreBlock()
    store_result = await store_block.execute(RedisFileStoreInput(
        key=key, file_ref=ref.model_dump(), ttl_seconds=60,
    ))
    assert store_result.success
    assert store_result.filename == "cached.txt"

    # Load
    load_block = RedisFileLoadBlock()
    load_result = await load_block.execute(RedisFileLoadInput(key=key))
    assert load_result.found
    assert load_result.filename == "cached.txt"

    loaded_ref = FileRef(**load_result.file_ref)
    assert loaded_ref.decode() == b"cached file content"
    assert loaded_ref.mime_type == "text/plain"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_file_load_not_found() -> None:
    """Loading a non-existent key returns found=False."""
    _reset_redis_client()
    from llming_plumber.blocks.data.redis_ops import (
        RedisFileLoadBlock,
        RedisFileLoadInput,
    )

    block = RedisFileLoadBlock()
    result = await block.execute(RedisFileLoadInput(
        key="plumber:test:fileref:nonexistent",
    ))
    assert not result.found
    assert result.file_ref == {}


# ── End-to-end: FileRead → Zip → Redis → Load → Extract ──


@pytest.mark.integration
@pytest.mark.asyncio
async def test_full_pipeline_file_to_zip_to_redis(tmp_path: object) -> None:
    """Simulate a pipeline: read files, zip them, store in Redis, load back, extract."""
    _reset_redis_client()
    import pathlib

    from llming_plumber.blocks.data.archive import (
        ZipCreateBlock,
        ZipCreateInput,
        ZipExtractBlock,
        ZipExtractInput,
    )
    from llming_plumber.blocks.data.file_ops import FileReadBlock, FileReadInput
    from llming_plumber.blocks.data.redis_ops import (
        RedisFileLoadBlock,
        RedisFileLoadInput,
        RedisFileStoreBlock,
        RedisFileStoreInput,
    )

    # 1. Create test files
    d = pathlib.Path(str(tmp_path))
    (d / "a.txt").write_text("Alpha")
    (d / "b.txt").write_text("Bravo")

    # 2. Read files into FileRefs
    read_block = FileReadBlock()
    file_refs = []
    for name in ["a.txt", "b.txt"]:
        result = await read_block.execute(FileReadInput(path=str(d / name)))
        assert result.file_ref is not None
        file_refs.append(result.file_ref.model_dump())

    # 3. Zip the FileRefs
    zip_block = ZipCreateBlock()
    zip_result = await zip_block.execute(ZipCreateInput(
        files="[]", archive_name="bundle.zip", file_refs=file_refs,
    ))
    assert zip_result.file_count == 2

    # 4. Create a FileRef for the zip itself
    zip_ref = FileRef(
        filename="bundle.zip",
        mime_type="application/zip",
        data=zip_result.archive_base64,
        size_bytes=zip_result.size_bytes,
    )

    # 5. Store in Redis
    store_block = RedisFileStoreBlock()
    store_result = await store_block.execute(RedisFileStoreInput(
        key="plumber:test:pipeline:bundle",
        file_ref=zip_ref.model_dump(),
        ttl_seconds=60,
    ))
    assert store_result.success

    # 6. Load from Redis
    load_block = RedisFileLoadBlock()
    load_result = await load_block.execute(RedisFileLoadInput(
        key="plumber:test:pipeline:bundle",
    ))
    assert load_result.found
    loaded_ref = FileRef(**load_result.file_ref)
    assert loaded_ref.filename == "bundle.zip"

    # 7. Extract the zip
    extract_block = ZipExtractBlock()
    extract_result = await extract_block.execute(ZipExtractInput(
        archive_base64=loaded_ref.data,
    ))
    assert extract_result.file_count == 2

    # Verify content
    extracted = {
        FileRef(**r).filename: FileRef(**r).decode()
        for r in extract_result.file_refs
    }
    assert extracted["a.txt"] == b"Alpha"
    assert extracted["b.txt"] == b"Bravo"
