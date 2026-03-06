"""Seed example pipelines into MongoDB.

Usage:
    python examples/seed_pipelines.py

Creates three real pipeline definitions:
1. News Digest — RSS → Summarizer
2. Weather Monitor — Weather API → Filter by temperature threshold
3. Sentiment Tracker — News API → Sentiment Analysis → Aggregate
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from llming_plumber.db import close_connections, get_database
from llming_plumber.models.mongo_helpers import model_to_doc
from llming_plumber.models.pipeline import (
    BlockDefinition,
    BlockPosition,
    PipeDefinition,
    PipelineDefinition,
)
from llming_plumber.models.schedule import Schedule


def _news_digest() -> PipelineDefinition:
    """RSS → LLM Summarizer pipeline."""
    return PipelineDefinition(
        name="News Digest",
        description=(
            "Fetches the latest headlines from a tech RSS feed "
            "and summarizes them into a brief digest."
        ),
        owner_id="admin",
        owner_type="user",
        tags=["news", "llm", "daily"],
        blocks=[
            BlockDefinition(
                uid="rss-fetch",
                block_type="rss_reader",
                label="Fetch RSS Feed",
                config={
                    "url": "https://feeds.arstechnica.com/arstechnica/technology-lab",
                    "max_items": 5,
                },
                position=BlockPosition(x=100, y=200),
            ),
            BlockDefinition(
                uid="summarize",
                block_type="llm_summarizer",
                label="Summarize Headlines",
                config={
                    "provider": "openai",
                    "model": "gpt-5-nano",
                    "style": "bullet_points",
                    "max_length": "brief",
                },
                position=BlockPosition(x=400, y=200),
            ),
        ],
        pipes=[
            PipeDefinition(
                uid="rss-to-summary",
                source_block_uid="rss-fetch",
                source_fitting_uid="output",
                target_block_uid="summarize",
                target_fitting_uid="input",
                field_mapping={"entries": "text"},
            ),
        ],
    )


def _weather_monitor() -> PipelineDefinition:
    """Weather API → Filter pipeline."""
    return PipelineDefinition(
        name="Weather Monitor",
        description=(
            "Checks the weather in Berlin and filters for "
            "temperature above a threshold."
        ),
        owner_id="admin",
        owner_type="user",
        tags=["weather", "monitoring"],
        blocks=[
            BlockDefinition(
                uid="weather-check",
                block_type="openweathermap",
                label="Get Berlin Weather",
                config={
                    "city": "Berlin,DE",
                    "units": "metric",
                },
                position=BlockPosition(x=100, y=200),
                notes="API key injected from credentials store",
            ),
            BlockDefinition(
                uid="temp-filter",
                block_type="filter",
                label="Filter Hot Days",
                config={
                    "field": "temp",
                    "operator": "gt",
                    "value": 30.0,
                },
                position=BlockPosition(x=400, y=200),
            ),
        ],
        pipes=[
            PipeDefinition(
                uid="weather-to-filter",
                source_block_uid="weather-check",
                source_fitting_uid="output",
                target_block_uid="temp-filter",
                target_fitting_uid="input",
            ),
        ],
    )


def _sentiment_tracker() -> PipelineDefinition:
    """News API → Sentiment → Aggregate pipeline."""
    return PipelineDefinition(
        name="Sentiment Tracker",
        description=(
            "Fetches recent news about a topic, analyzes "
            "sentiment of each article, and aggregates results."
        ),
        owner_id="admin",
        owner_type="team",
        tags=["news", "llm", "analysis"],
        blocks=[
            BlockDefinition(
                uid="news-fetch",
                block_type="news_api",
                label="Fetch News",
                config={
                    "query": "artificial intelligence",
                    "language": "en",
                    "page_size": 5,
                    "sort_by": "publishedAt",
                },
                position=BlockPosition(x=100, y=200),
            ),
            BlockDefinition(
                uid="sentiment",
                block_type="llm_sentiment",
                label="Analyze Sentiment",
                config={
                    "provider": "openai",
                    "model": "gpt-5-nano",
                },
                position=BlockPosition(x=400, y=200),
            ),
            BlockDefinition(
                uid="aggregate",
                block_type="aggregate",
                label="Aggregate Results",
                config={
                    "group_by": "sentiment",
                    "aggregations": {"count": "count"},
                },
                position=BlockPosition(x=700, y=200),
            ),
        ],
        pipes=[
            PipeDefinition(
                uid="news-to-sentiment",
                source_block_uid="news-fetch",
                source_fitting_uid="output",
                target_block_uid="sentiment",
                target_fitting_uid="input",
                field_mapping={"description": "text"},
            ),
            PipeDefinition(
                uid="sentiment-to-aggregate",
                source_block_uid="sentiment",
                source_fitting_uid="output",
                target_block_uid="aggregate",
                target_fitting_uid="input",
            ),
        ],
    )


PIPELINES = [_news_digest, _weather_monitor, _sentiment_tracker]


async def seed() -> None:
    """Insert example pipelines and schedules into MongoDB."""
    db = await get_database()

    now = datetime.now(UTC)

    for factory in PIPELINES:
        pipeline = factory()
        pipeline.created_at = now
        pipeline.updated_at = now

        doc = model_to_doc(pipeline)
        result = await db.pipelines.insert_one(doc)
        pipeline_id = str(result.inserted_id)
        print(f"  Created pipeline: {pipeline.name} ({pipeline_id})")

    # Add a daily schedule for the news digest
    news_pipeline = await db.pipelines.find_one({"name": "News Digest"})
    if news_pipeline:
        schedule = Schedule(
            pipeline_id=str(news_pipeline["_id"]),
            cron_expression="0 8 * * *",  # daily at 8 AM
            enabled=True,
            next_run_at=now,
            tags=["daily"],
            created_at=now,
        )
        doc = model_to_doc(schedule)
        await db.schedules.insert_one(doc)
        print("  Created schedule: News Digest daily at 8 AM")

    # Add hourly schedule for weather monitor
    weather_pipeline = await db.pipelines.find_one(
        {"name": "Weather Monitor"}
    )
    if weather_pipeline:
        schedule = Schedule(
            pipeline_id=str(weather_pipeline["_id"]),
            cron_expression="0 * * * *",  # every hour
            enabled=True,
            next_run_at=now,
            tags=["hourly", "monitoring"],
            created_at=now,
        )
        doc = model_to_doc(schedule)
        await db.schedules.insert_one(doc)
        print("  Created schedule: Weather Monitor hourly")

    print(f"\nSeeded {len(PIPELINES)} pipelines + 2 schedules.")
    await close_connections()


def main() -> None:
    print("Seeding example pipelines...")
    asyncio.run(seed())


if __name__ == "__main__":
    main()
