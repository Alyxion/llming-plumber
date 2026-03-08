"""Unit tests for the FileRef model."""

from __future__ import annotations

import base64

import pytest

from llming_plumber.models.file_ref import (
    FileRef,
    file_ref_from_attachment,
    file_ref_to_attachment,
)
from llming_plumber.models.parcel import Attachment


# ── Construction ──


def test_from_bytes() -> None:
    raw = b"Hello, World!"
    ref = FileRef.from_bytes(raw, "greeting.txt")
    assert ref.filename == "greeting.txt"
    assert ref.mime_type == "text/plain"
    assert ref.size_bytes == len(raw)
    assert ref.data == base64.b64encode(raw).decode()


def test_from_bytes_with_explicit_mime() -> None:
    ref = FileRef.from_bytes(b"\x89PNG", "image.png", mime_type="image/png")
    assert ref.mime_type == "image/png"


def test_from_bytes_unknown_extension() -> None:
    ref = FileRef.from_bytes(b"data", "file.xyz123")
    assert ref.mime_type == "application/octet-stream"


def test_auto_detect_mime_from_extension() -> None:
    ref = FileRef(filename="report.pdf", data=base64.b64encode(b"fake").decode())
    assert ref.mime_type == "application/pdf"


def test_auto_detect_size_from_data() -> None:
    raw = b"test content"
    b64 = base64.b64encode(raw).decode()
    ref = FileRef(filename="test.txt", data=b64)
    assert ref.size_bytes == len(raw)


def test_default_mime_preserved_when_no_extension() -> None:
    ref = FileRef(filename="noext", data=base64.b64encode(b"x").decode())
    assert ref.mime_type == "application/octet-stream"


def test_empty_fileref() -> None:
    ref = FileRef()
    assert ref.filename == ""
    assert ref.data == ""
    assert ref.size_bytes == 0
    assert ref.decode() == b""


# ── Decode ──


def test_decode_roundtrip() -> None:
    raw = b"binary \x00\x01\x02 content"
    ref = FileRef.from_bytes(raw, "bin.dat")
    assert ref.decode() == raw


# ── Extension ──


def test_extension() -> None:
    assert FileRef(filename="photo.JPG").extension == ".jpg"
    assert FileRef(filename="noext").extension == ""
    assert FileRef(filename="archive.tar.gz").extension == ".gz"


# ── Human size ──


def test_human_size() -> None:
    ref = FileRef(filename="f", size_bytes=0)
    assert ref.human_size() == "0 B"
    ref = FileRef(filename="f", size_bytes=1500)
    assert "KB" in ref.human_size()
    ref = FileRef(filename="f", size_bytes=5 * 1024 * 1024)
    assert "MB" in ref.human_size()


# ── from_dict ──


def test_from_dict_native_format() -> None:
    d = {"filename": "test.txt", "data": base64.b64encode(b"hi").decode(), "mime_type": "text/plain"}
    ref = FileRef.from_dict(d)
    assert ref.filename == "test.txt"
    assert ref.decode() == b"hi"


def test_from_dict_legacy_archive_format() -> None:
    d = {"name": "old.txt", "content_base64": base64.b64encode(b"legacy").decode(), "size_bytes": 6}
    ref = FileRef.from_dict(d)
    assert ref.filename == "old.txt"
    assert ref.decode() == b"legacy"
    assert ref.size_bytes == 6


def test_from_dict_invalid_raises() -> None:
    with pytest.raises(ValueError, match="Cannot coerce"):
        FileRef.from_dict({"random": "keys"})


# ── Attachment conversion ──


def test_to_attachment_roundtrip() -> None:
    ref = FileRef.from_bytes(b"payload", "doc.pdf", mime_type="application/pdf")
    att = file_ref_to_attachment(ref, uid="att-1")
    assert isinstance(att, Attachment)
    assert att.uid == "att-1"
    assert att.filename == "doc.pdf"
    assert att.mime_type == "application/pdf"
    assert att.size_bytes == 7
    assert att.data_b64 == ref.data

    # Convert back
    ref2 = file_ref_from_attachment(att)
    assert ref2.filename == ref.filename
    assert ref2.mime_type == ref.mime_type
    assert ref2.data == ref.data
    assert ref2.size_bytes == ref.size_bytes


def test_to_attachment_default_uid() -> None:
    ref = FileRef(filename="file.zip")
    att = file_ref_to_attachment(ref)
    assert att.uid == "file.zip"


# ── Serialization ──


def test_model_dump_roundtrip() -> None:
    ref = FileRef.from_bytes(b"content", "test.json", mime_type="application/json")
    d = ref.model_dump()
    assert isinstance(d, dict)
    ref2 = FileRef(**d)
    assert ref2.filename == ref.filename
    assert ref2.data == ref.data
    assert ref2.mime_type == ref.mime_type
