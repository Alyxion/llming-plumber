"""Resource limits and safety guards for block execution.

Centralises all memory, size, and iteration budgets so they are
easy to tune from one place.  Every constant can be overridden via
an environment variable of the same name prefixed with ``PLUMBER_``,
e.g. ``PLUMBER_MAX_FILE_BYTES=104857600`` doubles the file limit.
"""

from __future__ import annotations

import os

# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(f"PLUMBER_{name}")
    if raw is not None:
        return int(raw)
    return default


# ------------------------------------------------------------------
# File I/O
# ------------------------------------------------------------------

MAX_FILE_BYTES: int = _env_int("MAX_FILE_BYTES", 50 * 1024 * 1024)
"""Hard ceiling for any file loaded into memory (default 50 MB)."""

MAX_BASE64_INPUT_BYTES: int = _env_int("MAX_BASE64_INPUT_BYTES", 70 * 1024 * 1024)
"""Max base64-encoded string size accepted (≈ 50 MB decoded).
base64 adds ~33 % overhead."""

# ------------------------------------------------------------------
# Lists / records
# ------------------------------------------------------------------

MAX_LIST_ITEMS: int = _env_int("MAX_LIST_ITEMS", 100_000)
"""Max items accepted by list-processing blocks (filter, sort, …)."""

MAX_RECORDS: int = _env_int("MAX_RECORDS", 500_000)
"""Max rows read from CSV / Excel / Parquet readers."""

# ------------------------------------------------------------------
# Fan-out / iteration
# ------------------------------------------------------------------

MAX_FAN_OUT_ITEMS: int = _env_int("MAX_FAN_OUT_ITEMS", 10_000)
"""Max items a SplitBlock may fan out over."""

DEFAULT_FAN_OUT_CONCURRENCY: int = _env_int("DEFAULT_FAN_OUT_CONCURRENCY", 10)
"""Default concurrent tasks during fan-out execution."""

FAN_OUT_BATCH_SIZE: int = _env_int("FAN_OUT_BATCH_SIZE", 200)
"""Process fan-out parcels in batches of this size to cap memory."""

# ------------------------------------------------------------------
# Documents
# ------------------------------------------------------------------

MAX_PAGES: int = _env_int("MAX_PAGES", 500)
"""Max pages for PDF / PPTX builders and extractors."""

MAX_SHEETS: int = _env_int("MAX_SHEETS", 50)
"""Max sheets in an Excel workbook."""

MAX_ROWS_PER_SHEET: int = _env_int("MAX_ROWS_PER_SHEET", 200_000)
"""Max rows per Excel sheet."""

MAX_ELEMENTS_PER_PAGE: int = _env_int("MAX_ELEMENTS_PER_PAGE", 5_000)
"""Max geometric elements per PDF page."""

MAX_SLIDES: int = _env_int("MAX_SLIDES", 200)
"""Max slides in a PowerPoint presentation."""

MAX_SECTIONS: int = _env_int("MAX_SECTIONS", 500)
"""Max sections in a Word document."""

MAX_ELEMENTS_PER_SECTION: int = _env_int("MAX_ELEMENTS_PER_SECTION", 2_000)
"""Max elements per Word document section."""

# ------------------------------------------------------------------
# Recursion / depth
# ------------------------------------------------------------------

MAX_SUBCLASS_DEPTH: int = _env_int("MAX_SUBCLASS_DEPTH", 200)
"""Safety limit for recursive subclass walking in the registry."""

# ------------------------------------------------------------------
# Validation helpers
# ------------------------------------------------------------------


class ResourceLimitError(ValueError):
    """Raised when a resource limit is exceeded."""


def check_file_size(size_bytes: int, *, label: str = "file") -> None:
    """Raise if *size_bytes* exceeds ``MAX_FILE_BYTES``."""
    if size_bytes > MAX_FILE_BYTES:
        mb = MAX_FILE_BYTES / (1024 * 1024)
        got = size_bytes / (1024 * 1024)
        msg = (
            f"{label} is {got:.1f} MB, exceeds the "
            f"{mb:.0f} MB limit (PLUMBER_MAX_FILE_BYTES)"
        )
        raise ResourceLimitError(msg)


def check_base64_size(b64_string: str, *, label: str = "content") -> None:
    """Raise if the base64 string is too large to decode safely."""
    size = len(b64_string)
    if size > MAX_BASE64_INPUT_BYTES:
        mb = MAX_BASE64_INPUT_BYTES / (1024 * 1024)
        got = size / (1024 * 1024)
        msg = (
            f"{label} base64 payload is {got:.1f} MB, exceeds the "
            f"{mb:.0f} MB limit (PLUMBER_MAX_BASE64_INPUT_BYTES)"
        )
        raise ResourceLimitError(msg)


def estimate_decoded_size(b64_string: str) -> int:
    """Estimate decoded byte count from a base64 string without decoding."""
    n = len(b64_string)
    padding = b64_string.count("=") if n else 0
    return (n * 3) // 4 - padding


def check_list_size(
    items: list | int,  # type: ignore[type-arg]
    *,
    limit: int = MAX_LIST_ITEMS,
    label: str = "items",
) -> None:
    """Raise if list length exceeds *limit*."""
    count = items if isinstance(items, int) else len(items)
    if count > limit:
        msg = (
            f"{label} has {count:,} entries, exceeds the "
            f"{limit:,} limit"
        )
        raise ResourceLimitError(msg)


def check_page_count(
    count: int,
    *,
    limit: int = MAX_PAGES,
    label: str = "pages",
) -> None:
    """Raise if page / slide count exceeds *limit*."""
    if count > limit:
        msg = f"{label} has {count:,} items, exceeds the {limit:,} limit"
        raise ResourceLimitError(msg)
