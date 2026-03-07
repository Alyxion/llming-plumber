from __future__ import annotations

import base64

import pytest

from llming_plumber.blocks.core.static_data import (
    StaticDataBlock,
    StaticDataInput,
)


async def test_text_passthrough() -> None:
    block = StaticDataBlock()
    result = await block.execute(
        StaticDataInput(content="hello world", mime_type="text/plain")
    )
    assert result.content == "hello world"
    assert result.mime_type == "text/plain"
    assert result.size_bytes == 11


async def test_csv_content() -> None:
    csv = "id,name\n1,Alice\n2,Bob\n"
    block = StaticDataBlock()
    result = await block.execute(
        StaticDataInput(content=csv, mime_type="text/csv", filename="data.csv")
    )
    assert result.content == csv
    assert result.filename == "data.csv"
    assert result.size_bytes == len(csv.encode())


async def test_base64_binary() -> None:
    raw = b"\x00\x01\x02\x03" * 10
    encoded = base64.b64encode(raw).decode()
    block = StaticDataBlock()
    result = await block.execute(
        StaticDataInput(
            content=encoded,
            mime_type="application/octet-stream",
            is_base64=True,
        )
    )
    assert result.content == encoded
    assert result.size_bytes == 40


async def test_json_content() -> None:
    block = StaticDataBlock()
    json_str = '{"key": "value"}'
    result = await block.execute(
        StaticDataInput(content=json_str, mime_type="application/json")
    )
    assert result.content == json_str
    assert result.mime_type == "application/json"


async def test_exceeds_default_limit() -> None:
    block = StaticDataBlock()
    big = "x" * (256 * 1024 + 1)
    with pytest.raises(ValueError, match="exceeds limit"):
        await block.execute(StaticDataInput(content=big))


async def test_custom_limit_rejects() -> None:
    block = StaticDataBlock()
    content = "x" * 2048
    with pytest.raises(ValueError, match="exceeds limit"):
        await block.execute(
            StaticDataInput(content=content, max_size_kb=1)
        )


async def test_custom_limit_allows() -> None:
    block = StaticDataBlock()
    content = "x" * 1024
    result = await block.execute(
        StaticDataInput(content=content, max_size_kb=1)
    )
    assert result.size_bytes == 1024


async def test_base64_size_check_uses_decoded_size() -> None:
    raw = b"A" * 2048
    encoded = base64.b64encode(raw).decode()
    block = StaticDataBlock()
    with pytest.raises(ValueError, match="exceeds limit"):
        await block.execute(
            StaticDataInput(
                content=encoded,
                is_base64=True,
                max_size_kb=1,
            )
        )


async def test_exactly_at_limit() -> None:
    block = StaticDataBlock()
    content = "x" * 1024
    result = await block.execute(
        StaticDataInput(content=content, max_size_kb=1)
    )
    assert result.size_bytes == 1024


async def test_xlsx_mime_type() -> None:
    block = StaticDataBlock()
    fake_xlsx = base64.b64encode(b"PK\x03\x04tiny").decode()
    xlsx_mime = (
        "application/vnd.openxmlformats-officedocument"
        ".spreadsheetml.sheet"
    )
    result = await block.execute(
        StaticDataInput(
            content=fake_xlsx,
            mime_type=xlsx_mime,
            is_base64=True,
            filename="report.xlsx",
        )
    )
    assert result.filename == "report.xlsx"
    assert result.mime_type == xlsx_mime
