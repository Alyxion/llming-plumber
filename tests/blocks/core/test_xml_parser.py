from __future__ import annotations

import pytest

from llming_plumber.blocks.core.xml_parser import (
    XmlParserBlock,
    XmlParserInput,
)


async def test_extract_elements() -> None:
    xml = "<root><item>A</item><item>B</item></root>"
    block = XmlParserBlock()
    result = await block.execute(
        XmlParserInput(xml_text=xml, element_tag="item")
    )
    assert result.element_count == 2
    assert result.elements[0] == {"text": "A"}
    assert result.elements[1] == {"text": "B"}


async def test_extract_with_attributes() -> None:
    xml = (
        '<root><item id="1" color="red">A</item>'
        '<item id="2" color="blue">B</item></root>'
    )
    block = XmlParserBlock()
    result = await block.execute(
        XmlParserInput(xml_text=xml, element_tag="item", attributes=["id", "color"])
    )
    assert result.elements[0] == {"text": "A", "id": "1", "color": "red"}
    assert result.elements[1] == {"text": "B", "id": "2", "color": "blue"}


async def test_no_matching_elements() -> None:
    xml = "<root><other>X</other></root>"
    block = XmlParserBlock()
    result = await block.execute(
        XmlParserInput(xml_text=xml, element_tag="item")
    )
    assert result.element_count == 0
    assert result.elements == []


async def test_missing_attribute_returns_empty_string() -> None:
    xml = "<root><item>A</item></root>"
    block = XmlParserBlock()
    result = await block.execute(
        XmlParserInput(xml_text=xml, element_tag="item", attributes=["id"])
    )
    assert result.elements[0] == {"text": "A", "id": ""}


async def test_invalid_xml_raises() -> None:
    block = XmlParserBlock()
    with pytest.raises(Exception):
        await block.execute(
            XmlParserInput(xml_text="<not-closed>", element_tag="x")
        )


async def test_element_with_no_text() -> None:
    xml = '<root><item id="1"/></root>'
    block = XmlParserBlock()
    result = await block.execute(
        XmlParserInput(xml_text=xml, element_tag="item", attributes=["id"])
    )
    assert result.elements[0] == {"text": "", "id": "1"}
