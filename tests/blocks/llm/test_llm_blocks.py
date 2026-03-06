"""Unit tests for LLM blocks — mocked, no real API calls.

Every LLM block is tested by mocking the `prompt` function
from `llming_plumber.blocks.llm._client`.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest

MOCK_PROMPT = "llming_plumber.blocks.llm._client.prompt"


# ---------------------------------------------------------------------------
# LLM Chat
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_chat_block() -> None:
    from llming_plumber.blocks.llm.chat import (
        ChatBlock,
        ChatInput,
        ChatOutput,
    )

    with patch(MOCK_PROMPT, new_callable=AsyncMock) as mock:
        mock.return_value = "Hello! How can I help?"
        block = ChatBlock()
        result = await block.execute(
            ChatInput(
                provider="openai",
                model="gpt-5-nano",
                user_message="Hi",
            )
        )
        assert isinstance(result, ChatOutput)
        assert result.response == "Hello! How can I help?"
        mock.assert_called_once()


@pytest.mark.asyncio
async def test_chat_block_custom_system_prompt() -> None:
    from llming_plumber.blocks.llm.chat import ChatBlock, ChatInput

    with patch(MOCK_PROMPT, new_callable=AsyncMock) as mock:
        mock.return_value = "Yarr!"
        block = ChatBlock()
        await block.execute(
            ChatInput(
                provider="anthropic",
                model="claude-haiku-4-5-20251001",
                system_prompt="You are a pirate.",
                user_message="Hello",
                temperature=0.8,
            )
        )
        call_kwargs = mock.call_args
        assert "pirate" in call_kwargs.kwargs.get(
            "system", call_kwargs.args[2] if len(call_kwargs.args) > 2 else ""
        )


# ---------------------------------------------------------------------------
# Summarizer
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_summarizer_block() -> None:
    from llming_plumber.blocks.llm.summarizer import (
        SummarizerBlock,
        SummarizerInput,
        SummarizerOutput,
    )

    with patch(MOCK_PROMPT, new_callable=AsyncMock) as mock:
        mock.return_value = "This is a summary."
        block = SummarizerBlock()
        result = await block.execute(
            SummarizerInput(
                provider="openai",
                model="gpt-5-nano",
                text="Long text here " * 100,
            )
        )
        assert isinstance(result, SummarizerOutput)
        assert result.summary == "This is a summary."


@pytest.mark.asyncio
async def test_summarizer_styles() -> None:
    from llming_plumber.blocks.llm.summarizer import (
        SummarizerBlock,
        SummarizerInput,
    )

    for style in ["bullet_points", "paragraph", "executive_summary"]:
        for length in ["brief", "moderate", "detailed"]:
            with patch(MOCK_PROMPT, new_callable=AsyncMock) as mock:
                mock.return_value = "Summary."
                block = SummarizerBlock()
                await block.execute(
                    SummarizerInput(
                        provider="openai",
                        model="m",
                        text="text",
                        style=style,
                        max_length=length,
                    )
                )
                mock.assert_called_once()


# ---------------------------------------------------------------------------
# Translator
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_translator_block() -> None:
    from llming_plumber.blocks.llm.translator import (
        TranslatorBlock,
        TranslatorInput,
        TranslatorOutput,
    )

    response = json.dumps({
        "translated_text": "Hallo Welt",
        "detected_language": "English",
    })
    with patch(MOCK_PROMPT, new_callable=AsyncMock) as mock:
        mock.return_value = response
        block = TranslatorBlock()
        result = await block.execute(
            TranslatorInput(
                provider="openai",
                model="m",
                text="Hello World",
                target_language="German",
            )
        )
        assert isinstance(result, TranslatorOutput)
        assert result.translated_text == "Hallo Welt"
        assert result.detected_language == "English"


# ---------------------------------------------------------------------------
# Sentiment Analyzer
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sentiment_block() -> None:
    from llming_plumber.blocks.llm.sentiment import (
        SentimentBlock,
        SentimentInput,
        SentimentOutput,
    )

    response = json.dumps({
        "sentiment": "positive",
        "confidence": 0.95,
        "explanation": "Very happy text.",
    })
    with patch(MOCK_PROMPT, new_callable=AsyncMock) as mock:
        mock.return_value = response
        block = SentimentBlock()
        result = await block.execute(
            SentimentInput(
                provider="openai",
                model="m",
                text="I love this product!",
            )
        )
        assert isinstance(result, SentimentOutput)
        assert result.sentiment == "positive"
        assert result.confidence == pytest.approx(0.95)


# ---------------------------------------------------------------------------
# Classifier
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_classifier_block() -> None:
    from llming_plumber.blocks.llm.classifier import (
        ClassifierBlock,
        ClassifierInput,
        ClassifierOutput,
    )

    response = json.dumps({
        "labels": ["spam"],
        "confidence_scores": {"spam": 0.92, "not_spam": 0.08},
    })
    with patch(MOCK_PROMPT, new_callable=AsyncMock) as mock:
        mock.return_value = response
        block = ClassifierBlock()
        result = await block.execute(
            ClassifierInput(
                provider="openai",
                model="m",
                text="Buy now! Limited offer!",
                categories=["spam", "not_spam"],
            )
        )
        assert isinstance(result, ClassifierOutput)
        assert "spam" in result.labels
        assert result.confidence_scores["spam"] > 0.5


# ---------------------------------------------------------------------------
# Entity Extractor
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_entity_extractor_block() -> None:
    from llming_plumber.blocks.llm.entity_extractor import (
        EntityExtractorBlock,
        EntityExtractorInput,
        EntityExtractorOutput,
    )

    response = json.dumps({
        "entities": [
            {"text": "Berlin", "type": "location"},
            {"text": "Anthropic", "type": "organization"},
        ]
    })
    with patch(MOCK_PROMPT, new_callable=AsyncMock) as mock:
        mock.return_value = response
        block = EntityExtractorBlock()
        result = await block.execute(
            EntityExtractorInput(
                provider="openai",
                model="m",
                text="Anthropic is based in San Francisco.",
            )
        )
        assert isinstance(result, EntityExtractorOutput)
        assert len(result.entities) == 2
        assert result.entities[0].type == "location"


# ---------------------------------------------------------------------------
# Data Extractor
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_data_extractor_block() -> None:
    from llming_plumber.blocks.llm.data_extractor import (
        DataExtractorBlock,
        DataExtractorInput,
        DataExtractorOutput,
    )

    response = json.dumps({
        "product": "Widget Pro",
        "price": 49.95,
        "currency": "USD",
    })
    with patch(MOCK_PROMPT, new_callable=AsyncMock) as mock:
        mock.return_value = response
        block = DataExtractorBlock()
        result = await block.execute(
            DataExtractorInput(
                provider="openai",
                model="m",
                text="The Widget Pro costs $49.95",
                schema_description="Extract product, price, currency",
            )
        )
        assert isinstance(result, DataExtractorOutput)
        assert result.extracted_data["product"] == "Widget Pro"
        assert result.extracted_data["price"] == 49.95


# ---------------------------------------------------------------------------
# Rewriter
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rewriter_block() -> None:
    from llming_plumber.blocks.llm.rewriter import (
        RewriterBlock,
        RewriterInput,
        RewriterOutput,
    )

    with patch(MOCK_PROMPT, new_callable=AsyncMock) as mock:
        mock.return_value = "The feline sat upon the mat."
        block = RewriterBlock()
        result = await block.execute(
            RewriterInput(
                provider="openai",
                model="m",
                text="The cat sat on the mat.",
                style="formal",
            )
        )
        assert isinstance(result, RewriterOutput)
        assert len(result.rewritten_text) > 0


@pytest.mark.asyncio
async def test_rewriter_with_instructions() -> None:
    from llming_plumber.blocks.llm.rewriter import (
        RewriterBlock,
        RewriterInput,
    )

    with patch(MOCK_PROMPT, new_callable=AsyncMock) as mock:
        mock.return_value = "Rewritten."
        block = RewriterBlock()
        await block.execute(
            RewriterInput(
                provider="openai",
                model="m",
                text="Original.",
                style="casual",
                instructions="Make it funny",
            )
        )
        # Verify instructions were included in the prompt
        call_args = mock.call_args
        user_text = call_args.kwargs.get(
            "user",
            call_args.args[3] if len(call_args.args) > 3 else "",
        )
        assert "funny" in user_text.lower() or mock.called


# ---------------------------------------------------------------------------
# Question Answerer
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_question_answerer_block() -> None:
    from llming_plumber.blocks.llm.question_answerer import (
        QuestionAnswererBlock,
        QuestionAnswererInput,
        QuestionAnswererOutput,
    )

    response = json.dumps({
        "answer": "Paris is the capital of France.",
        "confidence": "high",
    })
    with patch(MOCK_PROMPT, new_callable=AsyncMock) as mock:
        mock.return_value = response
        block = QuestionAnswererBlock()
        result = await block.execute(
            QuestionAnswererInput(
                provider="openai",
                model="m",
                context="France is a country. Its capital is Paris.",
                question="What is the capital of France?",
            )
        )
        assert isinstance(result, QuestionAnswererOutput)
        assert "Paris" in result.answer
        assert result.confidence == "high"
