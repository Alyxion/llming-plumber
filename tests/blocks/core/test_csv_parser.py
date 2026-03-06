from __future__ import annotations

from llming_plumber.blocks.core.csv_parser import (
    CsvParserBlock,
    CsvParserInput,
)


async def test_parse_with_header() -> None:
    block = CsvParserBlock()
    result = await block.execute(
        CsvParserInput(csv_text="name,age\nAlice,30\nBob,25")
    )
    assert result.column_names == ["name", "age"]
    assert len(result.rows) == 2
    assert result.rows[0] == {"name": "Alice", "age": "30"}
    assert result.rows[1] == {"name": "Bob", "age": "25"}


async def test_parse_without_header() -> None:
    block = CsvParserBlock()
    result = await block.execute(
        CsvParserInput(csv_text="Alice,30\nBob,25", has_header=False)
    )
    assert result.column_names == ["col_0", "col_1"]
    assert result.rows[0] == {"col_0": "Alice", "col_1": "30"}


async def test_custom_delimiter() -> None:
    block = CsvParserBlock()
    result = await block.execute(
        CsvParserInput(csv_text="name;age\nAlice;30", delimiter=";")
    )
    assert result.rows[0] == {"name": "Alice", "age": "30"}


async def test_empty_csv() -> None:
    block = CsvParserBlock()
    result = await block.execute(CsvParserInput(csv_text=""))
    assert result.rows == []
    assert result.column_names == []


async def test_header_only() -> None:
    block = CsvParserBlock()
    result = await block.execute(CsvParserInput(csv_text="name,age\n"))
    assert result.column_names == ["name", "age"]
    assert result.rows == []


async def test_block_type() -> None:
    assert CsvParserBlock.block_type == "csv_parser"
