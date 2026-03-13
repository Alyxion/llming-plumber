"""Unit tests for content_summarizer block (and legacy sink_scanner alias)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

MOCK_PROMPT = "llming_plumber.blocks.llm._client.prompt"


# ---------------------------------------------------------------------------
# _truncate()
# ---------------------------------------------------------------------------


def test_truncate_short_text_unchanged() -> None:
    from llming_plumber.blocks.llm.sink_scanner import _truncate

    text = "Hello world"
    result, was_truncated = _truncate(text, 100)
    assert result == text
    assert was_truncated is False


def test_truncate_exact_boundary() -> None:
    from llming_plumber.blocks.llm.sink_scanner import _truncate

    text = "x" * 100
    result, was_truncated = _truncate(text, 100)
    assert result == text
    assert was_truncated is False


def test_truncate_long_text_has_marker() -> None:
    from llming_plumber.blocks.llm.sink_scanner import _truncate

    text = "A" * 5000 + "B" * 5000
    result, was_truncated = _truncate(text, 300)
    assert "[...truncated...]" in result
    assert len(result) < len(text)
    assert was_truncated is True


def test_truncate_preserves_start_and_end() -> None:
    from llming_plumber.blocks.llm.sink_scanner import _truncate

    text = "START" + "x" * 10000 + "END"
    result, was_truncated = _truncate(text, 300)
    assert result.startswith("START")
    assert result.endswith("END")
    assert was_truncated is True


def test_truncate_ratio() -> None:
    """Head gets ~2/3 of max_chars, tail gets ~1/3."""
    from llming_plumber.blocks.llm.sink_scanner import _truncate

    max_chars = 300
    text = "H" * 1000 + "T" * 1000
    result, _ = _truncate(text, max_chars)
    marker = "\n\n[...truncated...]\n\n"
    head, tail = result.split(marker)
    assert len(head) == max_chars * 2 // 3
    assert len(tail) == max_chars - len(head)


# ---------------------------------------------------------------------------
# ContentSummarizerBlock — basic execution
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_summarize_basic() -> None:
    """Simple text in, summary out."""
    from llming_plumber.blocks.llm.sink_scanner import (
        ContentSummarizerBlock,
        ContentSummarizerInput,
    )

    with patch(MOCK_PROMPT, new_callable=AsyncMock) as mock:
        mock.return_value = "This is a summary."
        block = ContentSummarizerBlock()
        result = await block.execute(
            ContentSummarizerInput(
                provider="openai", model="gpt-5-nano", text="Some article text.",
            )
        )

    assert result.summary == "This is a summary."
    assert result.source_length == len("Some article text.")
    assert result.was_truncated is False
    assert result.model == "gpt-5-nano"
    mock.assert_called_once()


@pytest.mark.asyncio
async def test_summarize_truncates_long_input() -> None:
    """Long text is truncated before being sent to the LLM."""
    from llming_plumber.blocks.llm.sink_scanner import (
        ContentSummarizerBlock,
        ContentSummarizerInput,
    )

    long_text = "x" * 20000

    with patch(MOCK_PROMPT, new_callable=AsyncMock) as mock:
        mock.return_value = "Short summary."
        block = ContentSummarizerBlock()
        result = await block.execute(
            ContentSummarizerInput(
                provider="openai",
                model="gpt-5-nano",
                text=long_text,
                max_input_chars=1000,
            )
        )

    assert result.was_truncated is True
    assert result.source_length == 20000
    # The user text sent to LLM should be truncated
    call_kwargs = mock.call_args.kwargs
    assert "[...truncated...]" in call_kwargs["user"]


@pytest.mark.asyncio
async def test_summarize_custom_system_prompt() -> None:
    """Custom system prompt overrides the default."""
    from llming_plumber.blocks.llm.sink_scanner import (
        ContentSummarizerBlock,
        ContentSummarizerInput,
    )

    with patch(MOCK_PROMPT, new_callable=AsyncMock) as mock:
        mock.return_value = "Custom summary."
        block = ContentSummarizerBlock()
        await block.execute(
            ContentSummarizerInput(
                provider="openai",
                model="gpt-5-nano",
                text="Some text.",
                system_prompt="Be very brief.",
            )
        )

    call_kwargs = mock.call_args.kwargs
    assert call_kwargs["system"] == "Be very brief."


@pytest.mark.asyncio
async def test_summarize_default_system_prompt() -> None:
    """Empty system_prompt falls back to the loaded prompt file."""
    from llming_plumber.blocks.llm.sink_scanner import (
        ContentSummarizerBlock,
        ContentSummarizerInput,
        _DEFAULT_PROMPT,
    )

    with patch(MOCK_PROMPT, new_callable=AsyncMock) as mock:
        mock.return_value = "Summary."
        block = ContentSummarizerBlock()
        await block.execute(
            ContentSummarizerInput(
                provider="openai",
                model="gpt-5-nano",
                text="Some text.",
                system_prompt="",
            )
        )

    call_kwargs = mock.call_args.kwargs
    assert call_kwargs["system"] == _DEFAULT_PROMPT


@pytest.mark.asyncio
async def test_summarize_standalone_no_ctx() -> None:
    """Block works without a BlockContext."""
    from llming_plumber.blocks.llm.sink_scanner import (
        ContentSummarizerBlock,
        ContentSummarizerInput,
    )

    with patch(MOCK_PROMPT, new_callable=AsyncMock) as mock:
        mock.return_value = "Summary."
        block = ContentSummarizerBlock()
        result = await block.execute(
            ContentSummarizerInput(
                provider="openai", model="gpt-5-nano", text="Hello.",
            ),
            ctx=None,
        )

    assert result.summary == "Summary."


@pytest.mark.asyncio
async def test_ctx_logging_on_truncation() -> None:
    """Block logs a message when input is truncated and ctx is provided."""
    from llming_plumber.blocks.base import BlockContext
    from llming_plumber.blocks.llm.sink_scanner import (
        ContentSummarizerBlock,
        ContentSummarizerInput,
    )

    mock_console = AsyncMock()
    mock_console.write = AsyncMock()
    ctx = BlockContext(
        pipeline_id="pl1", run_id="run1", block_id="b1", console=mock_console,
    )

    with patch(MOCK_PROMPT, new_callable=AsyncMock) as mock:
        mock.return_value = "Summary."
        block = ContentSummarizerBlock()
        await block.execute(
            ContentSummarizerInput(
                provider="openai",
                model="gpt-5-nano",
                text="x" * 20000,
                max_input_chars=1000,
            ),
            ctx=ctx,
        )

    assert mock_console.write.call_count >= 1
    log_messages = [str(call.args[1]) for call in mock_console.write.call_args_list]
    assert any("truncated" in m.lower() for m in log_messages)


@pytest.mark.asyncio
async def test_no_log_when_not_truncated() -> None:
    """No truncation log when input fits within limit."""
    from llming_plumber.blocks.base import BlockContext
    from llming_plumber.blocks.llm.sink_scanner import (
        ContentSummarizerBlock,
        ContentSummarizerInput,
    )

    mock_console = AsyncMock()
    mock_console.write = AsyncMock()
    ctx = BlockContext(
        pipeline_id="pl1", run_id="run1", block_id="b1", console=mock_console,
    )

    with patch(MOCK_PROMPT, new_callable=AsyncMock) as mock:
        mock.return_value = "Summary."
        block = ContentSummarizerBlock()
        await block.execute(
            ContentSummarizerInput(
                provider="openai",
                model="gpt-5-nano",
                text="Short text.",
            ),
            ctx=ctx,
        )

    # No truncation log expected
    for call in mock_console.write.call_args_list:
        assert "truncated" not in str(call.args[1]).lower()


# ---------------------------------------------------------------------------
# Block metadata
# ---------------------------------------------------------------------------


def test_content_summarizer_block_type() -> None:
    from llming_plumber.blocks.llm.sink_scanner import ContentSummarizerBlock

    assert ContentSummarizerBlock.block_type == "content_summarizer"


def test_content_summarizer_categories() -> None:
    from llming_plumber.blocks.llm.sink_scanner import ContentSummarizerBlock

    assert "llm/text" in ContentSummarizerBlock.categories


def test_content_summarizer_icon() -> None:
    from llming_plumber.blocks.llm.sink_scanner import ContentSummarizerBlock

    assert ContentSummarizerBlock.icon == "tabler/file-text-ai"


# ---------------------------------------------------------------------------
# SinkScannerBlock — backwards-compatible alias
# ---------------------------------------------------------------------------


def test_sink_scanner_block_type() -> None:
    from llming_plumber.blocks.llm.sink_scanner import SinkScannerBlock

    assert SinkScannerBlock.block_type == "sink_scanner"


@pytest.mark.asyncio
async def test_sink_scanner_delegates_to_content_summarizer() -> None:
    """SinkScannerBlock delegates to ContentSummarizerBlock.execute."""
    from llming_plumber.blocks.llm.sink_scanner import (
        ContentSummarizerInput,
        SinkScannerBlock,
    )

    with patch(MOCK_PROMPT, new_callable=AsyncMock) as mock:
        mock.return_value = "Legacy summary."
        block = SinkScannerBlock()
        result = await block.execute(
            ContentSummarizerInput(
                provider="openai", model="gpt-5-nano", text="Legacy text.",
            )
        )

    assert result.summary == "Legacy summary."
    assert result.model == "gpt-5-nano"


# ---------------------------------------------------------------------------
# Provider / model passthrough
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_provider_and_model_passed_to_client() -> None:
    """provider and model are forwarded to _client.prompt."""
    from llming_plumber.blocks.llm.sink_scanner import (
        ContentSummarizerBlock,
        ContentSummarizerInput,
    )

    with patch(MOCK_PROMPT, new_callable=AsyncMock) as mock:
        mock.return_value = "Summary."
        block = ContentSummarizerBlock()
        await block.execute(
            ContentSummarizerInput(
                provider="anthropic",
                model="claude-haiku-4-5-20251001",
                text="Hello.",
            )
        )

    call_kwargs = mock.call_args.kwargs
    assert call_kwargs["provider"] == "anthropic"
    assert call_kwargs["model"] == "claude-haiku-4-5-20251001"


# ---------------------------------------------------------------------------
# Output model fields
# ---------------------------------------------------------------------------


def test_output_defaults() -> None:
    from llming_plumber.blocks.llm.sink_scanner import ContentSummarizerOutput

    out = ContentSummarizerOutput()
    assert out.summary == ""
    assert out.source_length == 0
    assert out.was_truncated is False
    assert out.model == ""
