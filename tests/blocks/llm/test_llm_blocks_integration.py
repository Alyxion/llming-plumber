"""Integration tests for LLM blocks — real API calls.

Run with: pytest -m integration tests/blocks/llm/

These tests verify the prompt behavior documented in prompts/*.txt:
- correct output structure and types
- edge cases (sarcasm, unanswerable, empty results, multi-label)
- style variants (summarizer styles, rewriter styles)
"""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

PROVIDER = "openai"
MODEL = "gpt-5-nano"


# ---------------------------------------------------------------------------
# Chat
# ---------------------------------------------------------------------------


async def test_chat_basic() -> None:
    from llming_plumber.blocks.llm.chat import ChatBlock, ChatInput, ChatOutput

    block = ChatBlock()
    result = await block.execute(
        ChatInput(
            provider=PROVIDER,
            model=MODEL,
            user_message="Reply with exactly: HELLO",
            max_tokens=256,
        )
    )
    assert isinstance(result, ChatOutput)
    assert "HELLO" in result.response.upper()


async def test_chat_custom_system_prompt() -> None:
    from llming_plumber.blocks.llm.chat import ChatBlock, ChatInput, ChatOutput

    block = ChatBlock()
    result = await block.execute(
        ChatInput(
            provider=PROVIDER,
            model=MODEL,
            system_prompt="You are a pirate. Always say 'Arrr'.",
            user_message="Greet me.",
            max_tokens=512,
        )
    )
    assert isinstance(result, ChatOutput)
    assert "arr" in result.response.lower()


async def test_chat_admits_uncertainty() -> None:
    """Prompt says: 'If you are unsure, say so rather than guessing.'"""
    from llming_plumber.blocks.llm.chat import ChatBlock, ChatInput, ChatOutput

    block = ChatBlock()
    result = await block.execute(
        ChatInput(
            provider=PROVIDER,
            model=MODEL,
            user_message=(
                "What is the name of the CEO of the fictional company "
                "Zybrex Industries? If you don't know, say so."
            ),
        )
    )
    assert isinstance(result, ChatOutput)
    assert len(result.response) > 0
    lower = result.response.lower()
    # Model should indicate it doesn't know
    assert any(
        phrase in lower
        for phrase in ["don't know", "do not know", "not aware",
                       "no information", "fictional", "cannot",
                       "unable", "not sure", "i'm not"]
    )


# ---------------------------------------------------------------------------
# Summarizer
# ---------------------------------------------------------------------------


async def test_summarizer_paragraph_brief() -> None:
    from llming_plumber.blocks.llm.summarizer import (
        SummarizerBlock,
        SummarizerInput,
        SummarizerOutput,
    )

    long_text = (
        "Python is a high-level programming language created by "
        "Guido van Rossum, first released in 1991. It emphasizes "
        "code readability through significant indentation. Python "
        "supports procedural, object-oriented, and functional "
        "programming paradigms and is widely used in web development, "
        "data science, artificial intelligence, and automation."
    )
    block = SummarizerBlock()
    result = await block.execute(
        SummarizerInput(
            provider=PROVIDER,
            model=MODEL,
            text=long_text,
            style="paragraph",
            max_length="brief",
        )
    )
    assert isinstance(result, SummarizerOutput)
    assert len(result.summary) > 10
    assert len(result.summary) < len(long_text) * 2


async def test_summarizer_bullet_points() -> None:
    """Prompt supports bullet_points style."""
    from llming_plumber.blocks.llm.summarizer import (
        SummarizerBlock,
        SummarizerInput,
        SummarizerOutput,
    )

    text = (
        "The company reported Q3 revenue of $4.2 billion, up 15% "
        "year-over-year. Net income was $890 million. The board "
        "approved a $500 million share buyback program. Headcount "
        "grew by 12% to 8,400 employees."
    )
    block = SummarizerBlock()
    result = await block.execute(
        SummarizerInput(
            provider=PROVIDER,
            model=MODEL,
            text=text,
            style="bullet_points",
            max_length="moderate",
        )
    )
    assert isinstance(result, SummarizerOutput)
    assert len(result.summary) > 10
    # Bullet points typically contain list markers
    assert any(c in result.summary for c in ["-", "•", "*", "–"])


async def test_summarizer_executive_summary() -> None:
    """Prompt supports executive_summary style."""
    from llming_plumber.blocks.llm.summarizer import (
        SummarizerBlock,
        SummarizerInput,
        SummarizerOutput,
    )

    text = (
        "After a thorough six-month evaluation of cloud providers, "
        "the engineering team recommends migrating from on-premises "
        "infrastructure to AWS. Key factors include a projected 30% "
        "cost reduction, improved uptime guarantees of 99.99%, and "
        "access to managed AI services. The migration is estimated "
        "to take 4 months with a team of 6 engineers. Risks include "
        "temporary service disruptions during cutover and a learning "
        "curve for operations staff."
    )
    block = SummarizerBlock()
    result = await block.execute(
        SummarizerInput(
            provider=PROVIDER,
            model=MODEL,
            text=text,
            style="executive_summary",
            max_length="detailed",
        )
    )
    assert isinstance(result, SummarizerOutput)
    assert len(result.summary) > 30


async def test_summarizer_short_text_passthrough() -> None:
    """Prompt says: 'If the input is already very short, return it with
    light cleanup instead of forcing a summary.'"""
    from llming_plumber.blocks.llm.summarizer import (
        SummarizerBlock,
        SummarizerInput,
        SummarizerOutput,
    )

    short_text = "The meeting is at noon."
    block = SummarizerBlock()
    result = await block.execute(
        SummarizerInput(
            provider=PROVIDER,
            model=MODEL,
            text=short_text,
            style="paragraph",
            max_length="brief",
        )
    )
    assert isinstance(result, SummarizerOutput)
    # Should be roughly the same length, not inflated
    assert len(result.summary) < len(short_text) * 3


# ---------------------------------------------------------------------------
# Translator
# ---------------------------------------------------------------------------


async def test_translator_basic() -> None:
    from llming_plumber.blocks.llm.translator import (
        TranslatorBlock,
        TranslatorInput,
        TranslatorOutput,
    )

    block = TranslatorBlock()
    result = await block.execute(
        TranslatorInput(
            provider=PROVIDER,
            model=MODEL,
            text="Good morning, how are you?",
            target_language="Spanish",
        )
    )
    assert isinstance(result, TranslatorOutput)
    assert len(result.translated_text) > 0
    assert result.detected_language.lower() in ("english", "en")


async def test_translator_preserves_tone() -> None:
    """Prompt says: 'preserving meaning, tone, and formatting'."""
    from llming_plumber.blocks.llm.translator import (
        TranslatorBlock,
        TranslatorInput,
        TranslatorOutput,
    )

    block = TranslatorBlock()
    result = await block.execute(
        TranslatorInput(
            provider=PROVIDER,
            model=MODEL,
            text="Hey! That's awesome, dude!",
            target_language="French",
        )
    )
    assert isinstance(result, TranslatorOutput)
    assert len(result.translated_text) > 0
    # Exclamation should survive — tone preserved
    assert "!" in result.translated_text


async def test_translator_auto_detect_non_english() -> None:
    """Prompt says: 'When the source language is auto-detect, identify it.'"""
    from llming_plumber.blocks.llm.translator import (
        TranslatorBlock,
        TranslatorInput,
        TranslatorOutput,
    )

    block = TranslatorBlock()
    result = await block.execute(
        TranslatorInput(
            provider=PROVIDER,
            model=MODEL,
            text="Guten Morgen, wie geht es Ihnen?",
            source_language="",
            target_language="English",
        )
    )
    assert isinstance(result, TranslatorOutput)
    assert result.detected_language.lower() in ("german", "de", "deutsch")
    assert len(result.translated_text) > 0


# ---------------------------------------------------------------------------
# Sentiment
# ---------------------------------------------------------------------------


async def test_sentiment_positive() -> None:
    from llming_plumber.blocks.llm.sentiment import (
        SentimentBlock,
        SentimentInput,
        SentimentOutput,
    )

    block = SentimentBlock()
    result = await block.execute(
        SentimentInput(
            provider=PROVIDER,
            model=MODEL,
            text="I absolutely love this product! Best purchase ever!",
        )
    )
    assert isinstance(result, SentimentOutput)
    assert result.sentiment == "positive"
    assert 0.0 <= result.confidence <= 1.0
    assert result.confidence > 0.7
    assert len(result.explanation) > 0


async def test_sentiment_negative() -> None:
    """Prompt must handle negative sentiment."""
    from llming_plumber.blocks.llm.sentiment import (
        SentimentBlock,
        SentimentInput,
        SentimentOutput,
    )

    block = SentimentBlock()
    result = await block.execute(
        SentimentInput(
            provider=PROVIDER,
            model=MODEL,
            text="This is terrible. Worst experience of my life. Never again.",
        )
    )
    assert isinstance(result, SentimentOutput)
    assert result.sentiment == "negative"
    assert 0.0 <= result.confidence <= 1.0
    assert result.confidence > 0.7


async def test_sentiment_sarcasm_mixed() -> None:
    """Prompt says: 'For ambiguous or sarcastic text, lean toward mixed
    with a lower confidence.'"""
    from llming_plumber.blocks.llm.sentiment import (
        SentimentBlock,
        SentimentInput,
        SentimentOutput,
    )

    block = SentimentBlock()
    result = await block.execute(
        SentimentInput(
            provider=PROVIDER,
            model=MODEL,
            text="Oh great, another meeting that could have been an email. Just what I needed today.",
        )
    )
    assert isinstance(result, SentimentOutput)
    assert result.sentiment in ("negative", "mixed")
    assert 0.0 <= result.confidence <= 1.0
    assert len(result.explanation) > 0


# ---------------------------------------------------------------------------
# Classifier
# ---------------------------------------------------------------------------


async def test_classifier_single_label() -> None:
    from llming_plumber.blocks.llm.classifier import (
        ClassifierBlock,
        ClassifierInput,
        ClassifierOutput,
    )

    block = ClassifierBlock()
    result = await block.execute(
        ClassifierInput(
            provider=PROVIDER,
            model=MODEL,
            text="The server crashed and all data was lost.",
            categories=["bug_report", "feature_request", "question"],
        )
    )
    assert isinstance(result, ClassifierOutput)
    assert "bug_report" in result.labels
    assert len(result.labels) == 1
    assert result.confidence_scores["bug_report"] > 0.5
    # Scores should sum to ~1.0
    total = sum(result.confidence_scores.values())
    assert 0.8 <= total <= 1.2


async def test_classifier_multi_label() -> None:
    """Prompt says: 'With multi_label enabled, return every applicable label.'"""
    from llming_plumber.blocks.llm.classifier import (
        ClassifierBlock,
        ClassifierInput,
        ClassifierOutput,
    )

    block = ClassifierBlock()
    result = await block.execute(
        ClassifierInput(
            provider=PROVIDER,
            model=MODEL,
            text=(
                "Please add dark mode support. Also the login page "
                "throws an error when I use special characters."
            ),
            categories=["bug_report", "feature_request", "question"],
            multi_label=True,
        )
    )
    assert isinstance(result, ClassifierOutput)
    assert len(result.labels) >= 2
    assert "bug_report" in result.labels
    assert "feature_request" in result.labels


async def test_classifier_poor_fit_low_confidence() -> None:
    """Prompt says: 'When none of the categories fits well, choose the closest
    one and reflect the uncertainty with a low score.'"""
    from llming_plumber.blocks.llm.classifier import (
        ClassifierBlock,
        ClassifierInput,
        ClassifierOutput,
    )

    block = ClassifierBlock()
    result = await block.execute(
        ClassifierInput(
            provider=PROVIDER,
            model=MODEL,
            text="The weather is nice today.",
            categories=["billing_issue", "technical_support", "account_deletion"],
        )
    )
    assert isinstance(result, ClassifierOutput)
    assert len(result.labels) >= 1
    # No category is a good fit — top score should be modest
    top_score = max(result.confidence_scores.values())
    assert top_score <= 0.95


# ---------------------------------------------------------------------------
# Entity Extractor
# ---------------------------------------------------------------------------


async def test_entity_extractor_basic() -> None:
    from llming_plumber.blocks.llm.entity_extractor import (
        EntityExtractorBlock,
        EntityExtractorInput,
        EntityExtractorOutput,
    )

    block = EntityExtractorBlock()
    result = await block.execute(
        EntityExtractorInput(
            provider=PROVIDER,
            model=MODEL,
            text="Marie Curie worked at the University of Paris in France.",
            entity_types=["person", "organization", "location"],
        )
    )
    assert isinstance(result, EntityExtractorOutput)
    assert len(result.entities) >= 2
    texts = [e.text for e in result.entities]
    types = [e.type for e in result.entities]
    assert any("Curie" in t for t in texts)
    assert "person" in types


async def test_entity_extractor_filters_types() -> None:
    """Prompt says: 'Only return entities that match the requested types.'"""
    from llming_plumber.blocks.llm.entity_extractor import (
        EntityExtractorBlock,
        EntityExtractorInput,
        EntityExtractorOutput,
    )

    block = EntityExtractorBlock()
    result = await block.execute(
        EntityExtractorInput(
            provider=PROVIDER,
            model=MODEL,
            text="Marie Curie worked at the University of Paris in France.",
            entity_types=["location"],
        )
    )
    assert isinstance(result, EntityExtractorOutput)
    # Should only have location entities, not person/org
    for entity in result.entities:
        assert entity.type == "location"


async def test_entity_extractor_empty_when_none() -> None:
    """Prompt says: 'Return an empty list when no matching entities exist.'"""
    from llming_plumber.blocks.llm.entity_extractor import (
        EntityExtractorBlock,
        EntityExtractorInput,
        EntityExtractorOutput,
    )

    block = EntityExtractorBlock()
    result = await block.execute(
        EntityExtractorInput(
            provider=PROVIDER,
            model=MODEL,
            text="The sky is blue and water is wet.",
            entity_types=["organization"],
        )
    )
    assert isinstance(result, EntityExtractorOutput)
    assert len(result.entities) == 0


# ---------------------------------------------------------------------------
# Data Extractor
# ---------------------------------------------------------------------------


async def test_data_extractor_basic() -> None:
    from llming_plumber.blocks.llm.data_extractor import (
        DataExtractorBlock,
        DataExtractorInput,
        DataExtractorOutput,
    )

    block = DataExtractorBlock()
    result = await block.execute(
        DataExtractorInput(
            provider=PROVIDER,
            model=MODEL,
            text="The Deluxe Widget costs $49.95 and weighs 1.2 kg.",
            schema_description="Extract product name, price as a number, and weight",
        )
    )
    assert isinstance(result, DataExtractorOutput)
    data = result.extracted_data
    # Should have extracted a numeric price (not a string with $)
    price_key = next(
        (k for k in data if "price" in k.lower()), None
    )
    assert price_key is not None
    assert isinstance(data[price_key], (int, float))


async def test_data_extractor_omits_unknown_fields() -> None:
    """Prompt says: 'Leave out fields whose values cannot be determined.'"""
    from llming_plumber.blocks.llm.data_extractor import (
        DataExtractorBlock,
        DataExtractorInput,
        DataExtractorOutput,
    )

    block = DataExtractorBlock()
    result = await block.execute(
        DataExtractorInput(
            provider=PROVIDER,
            model=MODEL,
            text="The product is called SuperGadget.",
            schema_description=(
                "Extract product name, price, manufacturer, and release date"
            ),
        )
    )
    assert isinstance(result, DataExtractorOutput)
    data = result.extracted_data
    # Name should be present
    name_key = next(
        (k for k in data if "name" in k.lower() or "product" in k.lower()),
        None,
    )
    assert name_key is not None
    # Price/manufacturer/date are not in the text — should be absent or null
    for key in data:
        if "price" in key.lower():
            assert data[key] is None or data[key] == ""


# ---------------------------------------------------------------------------
# Rewriter
# ---------------------------------------------------------------------------

_REWRITER_SOURCE = "The quick brown fox jumped over the lazy dog."


async def test_rewriter_formal() -> None:
    from llming_plumber.blocks.llm.rewriter import (
        RewriterBlock,
        RewriterInput,
        RewriterOutput,
    )

    block = RewriterBlock()
    result = await block.execute(
        RewriterInput(
            provider=PROVIDER,
            model=MODEL,
            text=_REWRITER_SOURCE,
            style="formal",
        )
    )
    assert isinstance(result, RewriterOutput)
    assert len(result.rewritten_text) > 0
    assert result.rewritten_text != _REWRITER_SOURCE


async def test_rewriter_casual() -> None:
    """Prompt defines casual: 'relaxed, conversational tone'."""
    from llming_plumber.blocks.llm.rewriter import (
        RewriterBlock,
        RewriterInput,
        RewriterOutput,
    )

    block = RewriterBlock()
    result = await block.execute(
        RewriterInput(
            provider=PROVIDER,
            model=MODEL,
            text=(
                "The committee has decided to postpone the quarterly "
                "review until further notice."
            ),
            style="casual",
        )
    )
    assert isinstance(result, RewriterOutput)
    assert len(result.rewritten_text) > 0


async def test_rewriter_simple() -> None:
    """Prompt defines simple: 'plain language, short sentences'."""
    from llming_plumber.blocks.llm.rewriter import (
        RewriterBlock,
        RewriterInput,
        RewriterOutput,
    )

    block = RewriterBlock()
    result = await block.execute(
        RewriterInput(
            provider=PROVIDER,
            model=MODEL,
            text=(
                "Photosynthesis is the biochemical process by which "
                "chloroplasts convert electromagnetic radiation into "
                "chemical energy via carbon fixation."
            ),
            style="simple",
        )
    )
    assert isinstance(result, RewriterOutput)
    assert len(result.rewritten_text) > 0


async def test_rewriter_with_instructions() -> None:
    """Prompt supports additional instructions appended to system prompt."""
    from llming_plumber.blocks.llm.rewriter import (
        RewriterBlock,
        RewriterInput,
        RewriterOutput,
    )

    block = RewriterBlock()
    result = await block.execute(
        RewriterInput(
            provider=PROVIDER,
            model=MODEL,
            text="The project deadline is next Friday.",
            style="creative",
            instructions="Make it rhyme",
        )
    )
    assert isinstance(result, RewriterOutput)
    assert len(result.rewritten_text) > 0


# ---------------------------------------------------------------------------
# Question Answerer
# ---------------------------------------------------------------------------


async def test_question_answerer_answerable() -> None:
    from llming_plumber.blocks.llm.question_answerer import (
        QuestionAnswererBlock,
        QuestionAnswererInput,
        QuestionAnswererOutput,
    )

    block = QuestionAnswererBlock()
    result = await block.execute(
        QuestionAnswererInput(
            provider=PROVIDER,
            model=MODEL,
            context="The capital of France is Paris. France is in Europe.",
            question="What is the capital of France?",
        )
    )
    assert isinstance(result, QuestionAnswererOutput)
    assert "Paris" in result.answer
    assert result.confidence == "high"


async def test_question_answerer_unanswerable() -> None:
    """Prompt says: 'When the context is insufficient, explain that in the
    answer and set confidence to low.'"""
    from llming_plumber.blocks.llm.question_answerer import (
        QuestionAnswererBlock,
        QuestionAnswererInput,
        QuestionAnswererOutput,
    )

    block = QuestionAnswererBlock()
    result = await block.execute(
        QuestionAnswererInput(
            provider=PROVIDER,
            model=MODEL,
            context="Bananas are yellow when ripe. They grow in tropical climates.",
            question="What is the GDP of Brazil?",
        )
    )
    assert isinstance(result, QuestionAnswererOutput)
    assert result.confidence == "low"


async def test_question_answerer_inference() -> None:
    """Prompt defines medium confidence: 'can be inferred'."""
    from llming_plumber.blocks.llm.question_answerer import (
        QuestionAnswererBlock,
        QuestionAnswererInput,
        QuestionAnswererOutput,
    )

    block = QuestionAnswererBlock()
    result = await block.execute(
        QuestionAnswererInput(
            provider=PROVIDER,
            model=MODEL,
            context=(
                "The store opens at 9 AM and closes at 5 PM on weekdays. "
                "It is closed on weekends."
            ),
            question="Can I visit the store on a Saturday at 10 AM?",
        )
    )
    assert isinstance(result, QuestionAnswererOutput)
    assert result.confidence in ("high", "medium")
    # Answer should indicate no
    lower = result.answer.lower()
    assert "no" in lower or "closed" in lower or "cannot" in lower
