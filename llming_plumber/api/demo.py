"""Sample pipeline catalog — add/remove individual sample pipelines.

Every sample is tagged ``_sample:<key>`` so it can be individually
identified, added, and removed without affecting user pipelines.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException
from motor.motor_asyncio import AsyncIOMotorDatabase

from llming_plumber.api.deps import get_db
from llming_plumber.models.mongo_helpers import doc_to_model, model_to_doc
from llming_plumber.models.pipeline import (
    BlockDefinition,
    BlockPosition,
    PipeDefinition,
    PipelineDefinition,
)
from llming_plumber.models.schedule import Schedule, TimeWindow

router = APIRouter()

SAMPLE_TAG_PREFIX = "_sample:"


# ------------------------------------------------------------------
# Sample pipeline definitions
# ------------------------------------------------------------------


class SampleEntry:
    """Descriptor for a sample pipeline in the catalog."""

    def __init__(
        self,
        key: str,
        category: str,
        title: str,
        description: str,
        icon: str,
        pipeline_fn: Any,
        schedule_fn: Any = None,
    ) -> None:
        self.key = key
        self.category = category
        self.title = title
        self.description = description
        self.icon = icon
        self.pipeline_fn = pipeline_fn
        self.schedule_fn = schedule_fn

    @property
    def tag(self) -> str:
        return f"{SAMPLE_TAG_PREFIX}{self.key}"


def _p(key: str) -> str:
    """Build the pipeline name from a sample key."""
    return f"[Sample] {key.replace('_', ' ').title()}"


# --- Timer / Schedule samples ---


def _daily_weather_report() -> PipelineDefinition:
    return PipelineDefinition(
        name=_p("daily_weather_report"),
        description="Fetches Berlin weather every morning and formats a daily report.",
        tags=[f"{SAMPLE_TAG_PREFIX}daily_weather_report"],
        owner_id="sample", owner_type="user",
        blocks=[
            BlockDefinition(
                uid="timer", block_type="timer_trigger", label="Daily Timer",
                config={"label": "Morning report"},
                position=BlockPosition(x=80, y=200),
            ),
            BlockDefinition(
                uid="weather", block_type="weather", label="Get Weather",
                config={"city": "Berlin,DE", "units": "metric"},
                position=BlockPosition(x=350, y=200),
            ),
            BlockDefinition(
                uid="report", block_type="text_template", label="Format Report",
                config={"template": "Daily Report ({date})\n\nBerlin: {temp}°C, {description}\nFeels like {feels_like}°C, humidity {humidity}%\nWind: {wind_speed} m/s"},
                position=BlockPosition(x=650, y=200),
            ),
            BlockDefinition(
                uid="log", block_type="log", label="Log Report",
                config={"message": "{rendered}", "level": "info"},
                position=BlockPosition(x=950, y=200),
            ),
        ],
        pipes=[
            PipeDefinition(
                uid="t-w", source_block_uid="timer", source_fitting_uid="output",
                target_block_uid="weather", target_fitting_uid="input",
                field_mapping={"date": "date"},
            ),
            PipeDefinition(
                uid="w-r", source_block_uid="weather", source_fitting_uid="output",
                target_block_uid="report", target_fitting_uid="input",
                field_mapping={"temp": "temp", "description": "description",
                               "feels_like": "feels_like", "humidity": "humidity",
                               "wind_speed": "wind_speed"},
            ),
            PipeDefinition(
                uid="t-r-date", source_block_uid="timer", source_fitting_uid="output",
                target_block_uid="report", target_fitting_uid="input",
                field_mapping={"date": "date"},
            ),
            PipeDefinition(
                uid="r-l", source_block_uid="report", source_fitting_uid="output",
                target_block_uid="log", target_fitting_uid="input",
                field_mapping={"rendered": "rendered"},
            ),
        ],
    )


def _daily_weather_schedule() -> Schedule:
    return Schedule(
        cron_expression="0 7 * * *",
        enabled=False,
        tags=[f"{SAMPLE_TAG_PREFIX}daily_weather_report"],
    )


def _work_hours_monitor() -> PipelineDefinition:
    return PipelineDefinition(
        name=_p("work_hours_monitor"),
        description=(
            "Monitors weather every 30s during work hours (Mon-Fri 08:00-18:00), "
            "slows to every 2 min outside hours."
        ),
        tags=[f"{SAMPLE_TAG_PREFIX}work_hours_monitor"],
        owner_id="sample", owner_type="user",
        blocks=[
            BlockDefinition(
                uid="timer", block_type="timer_trigger", label="Interval Timer",
                config={"label": "Work hours check"},
                position=BlockPosition(x=80, y=200),
            ),
            BlockDefinition(
                uid="weather", block_type="weather", label="Check Weather",
                config={"city": "Berlin,DE", "units": "metric"},
                position=BlockPosition(x=400, y=200),
            ),
            BlockDefinition(
                uid="log", block_type="log", label="Log",
                config={"message": "[{time}] Berlin: {temp}°C — {description}", "level": "info"},
                position=BlockPosition(x=700, y=200),
            ),
        ],
        pipes=[
            PipeDefinition(
                uid="t-w", source_block_uid="timer", source_fitting_uid="output",
                target_block_uid="weather", target_fitting_uid="input",
            ),
            PipeDefinition(
                uid="w-l", source_block_uid="weather", source_fitting_uid="output",
                target_block_uid="log", target_fitting_uid="input",
                field_mapping={"temp": "temp", "description": "description"},
            ),
            PipeDefinition(
                uid="t-l", source_block_uid="timer", source_fitting_uid="output",
                target_block_uid="log", target_fitting_uid="input",
                field_mapping={"time": "time"},
            ),
        ],
    )


def _work_hours_schedule() -> Schedule:
    return Schedule(
        interval_seconds=30,
        enabled=True,
        time_windows=[TimeWindow(start="08:00", end="18:00", weekdays=[0, 1, 2, 3, 4])],
        interval_multiplier_off_hours=4.0,
        tags=[f"{SAMPLE_TAG_PREFIX}work_hours_monitor"],
    )


def _multi_slot_poller() -> PipelineDefinition:
    return PipelineDefinition(
        name=_p("multi_slot_poller"),
        description=(
            "Runs at fixed time slots: 08:00, 12:00, 17:00 on weekdays. "
            "Good for reports that run at specific times."
        ),
        tags=[f"{SAMPLE_TAG_PREFIX}multi_slot_poller"],
        owner_id="sample", owner_type="user",
        blocks=[
            BlockDefinition(
                uid="timer", block_type="timer_trigger", label="Slot Timer",
                config={"label": "Scheduled slot"},
                position=BlockPosition(x=80, y=200),
            ),
            BlockDefinition(
                uid="log", block_type="log", label="Slot Check-In",
                config={"message": "Slot triggered at {time} on {weekday} ({date})", "level": "info"},
                position=BlockPosition(x=400, y=200),
            ),
        ],
        pipes=[
            PipeDefinition(
                uid="t-l", source_block_uid="timer", source_fitting_uid="output",
                target_block_uid="log", target_fitting_uid="input",
                field_mapping={"time": "time", "weekday": "weekday", "date": "date"},
            ),
        ],
    )


def _multi_slot_schedule() -> Schedule:
    return Schedule(
        cron_expression="0 8,12,17 * * 1-5",
        enabled=True,
        tags=[f"{SAMPLE_TAG_PREFIX}multi_slot_poller"],
    )


# --- Data processing samples ---


def _news_digest() -> PipelineDefinition:
    return PipelineDefinition(
        name=_p("news_digest"),
        description="Fetches tech news via RSS and summarizes with an LLM.",
        tags=[f"{SAMPLE_TAG_PREFIX}news_digest"],
        owner_id="sample", owner_type="user",
        blocks=[
            BlockDefinition(
                uid="rss", block_type="rss_reader", label="Tech RSS",
                config={"feed_url": "https://feeds.arstechnica.com/arstechnica/technology-lab", "max_items": 5},
                position=BlockPosition(x=80, y=200),
            ),
            BlockDefinition(
                uid="summarize", block_type="llm_summarizer", label="Summarize",
                config={"provider": "openai", "model": "gpt-5-nano",
                        "style": "bullet_points", "max_length": "brief"},
                position=BlockPosition(x=450, y=200),
            ),
        ],
        pipes=[
            PipeDefinition(
                uid="r-s", source_block_uid="rss", source_fitting_uid="output",
                target_block_uid="summarize", target_fitting_uid="input",
            ),
        ],
    )


def _etl_pipeline() -> PipelineDefinition:
    return PipelineDefinition(
        name=_p("etl_pipeline"),
        description="Classic ETL: static data -> filter -> sort -> aggregate.",
        tags=[f"{SAMPLE_TAG_PREFIX}etl_pipeline"],
        owner_id="sample", owner_type="user",
        blocks=[
            BlockDefinition(
                uid="data", block_type="static_data", label="Employee Data",
                config={"content": '[\n  {"name":"Alice","score":85,"dept":"engineering"},\n  {"name":"Bob","score":62,"dept":"marketing"},\n  {"name":"Carol","score":93,"dept":"engineering"},\n  {"name":"Dave","score":41,"dept":"marketing"},\n  {"name":"Eve","score":78,"dept":"engineering"},\n  {"name":"Frank","score":55,"dept":"sales"},\n  {"name":"Grace","score":91,"dept":"sales"}\n]', "mime_type": "application/json"},
                position=BlockPosition(x=80, y=200),
            ),
            BlockDefinition(
                uid="parse", block_type="json_transformer", label="Parse JSON",
                config={"expression": "data"},
                position=BlockPosition(x=350, y=200),
            ),
            BlockDefinition(
                uid="filter", block_type="filter", label="Score >= 60",
                config={"field": "score", "operator": "gte", "value": "60"},
                position=BlockPosition(x=600, y=200),
            ),
            BlockDefinition(
                uid="sort", block_type="sort", label="Sort by Score",
                config={"field": "score", "order": "desc"},
                position=BlockPosition(x=850, y=200),
            ),
        ],
        pipes=[
            PipeDefinition(
                uid="d-p", source_block_uid="data", source_fitting_uid="output",
                target_block_uid="parse", target_fitting_uid="input",
                field_mapping={"content": "data"},
            ),
            PipeDefinition(
                uid="p-f", source_block_uid="parse", source_fitting_uid="output",
                target_block_uid="filter", target_fitting_uid="input",
            ),
            PipeDefinition(
                uid="f-s", source_block_uid="filter", source_fitting_uid="output",
                target_block_uid="sort", target_fitting_uid="input",
            ),
        ],
    )


def _tagesschau_digest() -> PipelineDefinition:
    return PipelineDefinition(
        name=_p("tagesschau_digest"),
        description="Fetches latest Tagesschau headlines (no API key needed).",
        tags=[f"{SAMPLE_TAG_PREFIX}tagesschau_digest"],
        owner_id="sample", owner_type="user",
        blocks=[
            BlockDefinition(
                uid="tagesschau", block_type="tagesschau", label="Tagesschau",
                config={"max_items": 5},
                position=BlockPosition(x=80, y=200),
            ),
            BlockDefinition(
                uid="log", block_type="log", label="Log Headlines",
                config={"message": "Fetched {articles} headlines", "level": "info"},
                position=BlockPosition(x=400, y=200),
            ),
        ],
        pipes=[
            PipeDefinition(
                uid="t-l", source_block_uid="tagesschau", source_fitting_uid="output",
                target_block_uid="log", target_fitting_uid="input",
            ),
        ],
    )


def _heartbeat() -> PipelineDefinition:
    return PipelineDefinition(
        name=_p("heartbeat"),
        description="Simple heartbeat: timer fires every 10s, logs a timestamped ping.",
        tags=[f"{SAMPLE_TAG_PREFIX}heartbeat"],
        owner_id="sample", owner_type="user",
        blocks=[
            BlockDefinition(
                uid="timer", block_type="timer_trigger", label="Heartbeat",
                config={"label": "heartbeat"},
                position=BlockPosition(x=80, y=200),
            ),
            BlockDefinition(
                uid="log", block_type="log", label="Ping",
                config={"message": "ping at {time} ({weekday})", "level": "info"},
                position=BlockPosition(x=400, y=200),
            ),
        ],
        pipes=[
            PipeDefinition(
                uid="t-l", source_block_uid="timer", source_fitting_uid="output",
                target_block_uid="log", target_fitting_uid="input",
                field_mapping={"time": "time", "weekday": "weekday"},
            ),
        ],
    )


def _heartbeat_schedule() -> Schedule:
    return Schedule(
        interval_seconds=10,
        enabled=True,
        tags=[f"{SAMPLE_TAG_PREFIX}heartbeat"],
    )


def _slow_pipeline() -> PipelineDefinition:
    return PipelineDefinition(
        name=_p("slow_pipeline"),
        description="Demonstrates the wait (sleep) block: timer -> sleep 3s -> log.",
        tags=[f"{SAMPLE_TAG_PREFIX}slow_pipeline"],
        owner_id="sample", owner_type="user",
        blocks=[
            BlockDefinition(
                uid="timer", block_type="timer_trigger", label="Start",
                config={"label": "slow demo"},
                position=BlockPosition(x=80, y=200),
            ),
            BlockDefinition(
                uid="sleep", block_type="wait", label="Sleep 3s",
                config={"seconds": 3},
                position=BlockPosition(x=400, y=200),
            ),
            BlockDefinition(
                uid="log", block_type="log", label="Done",
                config={"message": "Woke up after {waited_seconds}s", "level": "info"},
                position=BlockPosition(x=720, y=200),
            ),
        ],
        pipes=[
            PipeDefinition(
                uid="t-s", source_block_uid="timer", source_fitting_uid="output",
                target_block_uid="sleep", target_fitting_uid="input",
            ),
            PipeDefinition(
                uid="s-l", source_block_uid="sleep", source_fitting_uid="output",
                target_block_uid="log", target_fitting_uid="input",
                field_mapping={"waited_seconds": "waited_seconds"},
            ),
        ],
    )


def _periodic_guard_demo() -> PipelineDefinition:
    return PipelineDefinition(
        name=_p("periodic_guard_demo"),
        description=(
            "Demonstrates the periodic guard: pauses the pipeline when the "
            "current second is NOT divisible by 5, resumes when it is. "
            "Watch the console log to see pause/resume cycles in real time."
        ),
        tags=[f"{SAMPLE_TAG_PREFIX}periodic_guard_demo"],
        owner_id="sample", owner_type="user",
        blocks=[
            BlockDefinition(
                uid="guard", block_type="periodic_guard",
                label="Clock Guard (every 5s)",
                config={
                    "check_block_type": "system_clock",
                    "check_config": "{}",
                    "condition": "second % 5 == 0",
                    "interval_seconds": "5",
                    "pause_message": "Second is {second} — not divisible by 5, pausing.",
                    "max_pause_seconds": "120",
                },
                position=BlockPosition(x=50, y=200),
            ),
            BlockDefinition(
                uid="split", block_type="split",
                label="Generate 20 items",
                config={"expression": "[i for i in range(20)]"},
                position=BlockPosition(x=350, y=200),
            ),
            BlockDefinition(
                uid="wait", block_type="wait",
                label="Wait 2s each",
                config={"seconds": 2},
                position=BlockPosition(x=650, y=200),
            ),
            BlockDefinition(
                uid="log", block_type="log",
                label="Log item",
                config={
                    "message": "Processed item #{item} at {time}",
                    "level": "info",
                },
                position=BlockPosition(x=950, y=200),
            ),
            BlockDefinition(
                uid="collect", block_type="collect",
                label="Collect results",
                config={},
                position=BlockPosition(x=1250, y=200),
            ),
        ],
        pipes=[
            PipeDefinition(
                uid="g-s", source_block_uid="guard", source_fitting_uid="output",
                target_block_uid="split", target_fitting_uid="input",
            ),
            PipeDefinition(
                uid="s-w", source_block_uid="split", source_fitting_uid="output",
                target_block_uid="wait", target_fitting_uid="input",
            ),
            PipeDefinition(
                uid="w-l", source_block_uid="wait", source_fitting_uid="output",
                target_block_uid="log", target_fitting_uid="input",
            ),
            PipeDefinition(
                uid="l-c", source_block_uid="log", source_fitting_uid="output",
                target_block_uid="collect", target_fitting_uid="input",
            ),
        ],
    )


def _http_json_pipeline() -> PipelineDefinition:
    return PipelineDefinition(
        name=_p("http_json_pipeline"),
        description="Fetches a public JSON API and logs the response.",
        tags=[f"{SAMPLE_TAG_PREFIX}http_json_pipeline"],
        owner_id="sample", owner_type="user",
        blocks=[
            BlockDefinition(
                uid="http", block_type="http_request", label="Fetch API",
                config={"url": "https://jsonplaceholder.typicode.com/posts/1", "method": "GET"},
                position=BlockPosition(x=80, y=200),
            ),
            BlockDefinition(
                uid="log", block_type="log", label="Log Response",
                config={"message": "Status: {status_code}", "level": "info"},
                position=BlockPosition(x=400, y=200),
            ),
        ],
        pipes=[
            PipeDefinition(
                uid="h-l", source_block_uid="http", source_fitting_uid="output",
                target_block_uid="log", target_fitting_uid="input",
                field_mapping={"status_code": "status_code", "body": "body"},
            ),
        ],
    )


def _cached_http_pipeline() -> PipelineDefinition:
    return PipelineDefinition(
        name=_p("cached_http_pipeline"),
        description="Reads from cache first; on miss, fetches HTTP and stores result.",
        tags=[f"{SAMPLE_TAG_PREFIX}cached_http_pipeline"],
        owner_id="sample", owner_type="user",
        blocks=[
            BlockDefinition(
                uid="trigger", block_type="manual_trigger", label="Start",
                config={"label": "cached fetch"},
                position=BlockPosition(x=80, y=200),
            ),
            BlockDefinition(
                uid="read_cache", block_type="read_cache", label="Check Cache",
                config={"cache_name": "api_response", "scope": "pipeline"},
                position=BlockPosition(x=350, y=200),
            ),
            BlockDefinition(
                uid="http", block_type="http_request", label="Fetch API",
                config={"url": "https://jsonplaceholder.typicode.com/posts/1", "method": "GET"},
                position=BlockPosition(x=620, y=300),
            ),
            BlockDefinition(
                uid="store_cache", block_type="store_cache", label="Store in Cache",
                config={"cache_name": "api_response", "scope": "pipeline", "ttl_seconds": 60, "data": ""},
                position=BlockPosition(x=890, y=300),
            ),
            BlockDefinition(
                uid="log", block_type="log", label="Log Result",
                config={"message": "Cache hit: {cache_hit}", "level": "info"},
                position=BlockPosition(x=890, y=200),
            ),
        ],
        pipes=[
            PipeDefinition(
                uid="t-rc", source_block_uid="trigger", source_fitting_uid="output",
                target_block_uid="read_cache", target_fitting_uid="input",
            ),
            PipeDefinition(
                uid="rc-log", source_block_uid="read_cache", source_fitting_uid="hit",
                target_block_uid="log", target_fitting_uid="input",
            ),
            PipeDefinition(
                uid="rc-http", source_block_uid="read_cache", source_fitting_uid="miss",
                target_block_uid="http", target_fitting_uid="input",
            ),
            PipeDefinition(
                uid="http-sc", source_block_uid="http", source_fitting_uid="output",
                target_block_uid="store_cache", target_fitting_uid="input",
                field_mapping={"data": "body"},
            ),
        ],
    )


def _variable_counter_pipeline() -> PipelineDefinition:
    return PipelineDefinition(
        name=_p("variable_counter_pipeline"),
        description="Demonstrates variable operations: set, increment, and string concat.",
        tags=[f"{SAMPLE_TAG_PREFIX}variable_counter_pipeline"],
        owner_id="sample", owner_type="user",
        blocks=[
            BlockDefinition(
                uid="trigger", block_type="manual_trigger", label="Start",
                config={"label": "variable demo"},
                position=BlockPosition(x=80, y=200),
            ),
            BlockDefinition(
                uid="vars", block_type="set_variables", label="Compute Variables",
                config={
                    "script": (
                        'pl_run_count += 1\n'
                        'job_status = "processed"\n'
                        'label = "run_" + str(pl_run_count)\n'
                        'total = pl_run_count * 10'
                    ),
                },
                position=BlockPosition(x=400, y=200),
            ),
            BlockDefinition(
                uid="log", block_type="log", label="Log Variables",
                config={"message": "Run #{pl_run_count}, label={label}, total={total}", "level": "info"},
                position=BlockPosition(x=720, y=200),
            ),
        ],
        pipes=[
            PipeDefinition(
                uid="t-v", source_block_uid="trigger", source_fitting_uid="output",
                target_block_uid="vars", target_fitting_uid="input",
            ),
            PipeDefinition(
                uid="v-l", source_block_uid="vars", source_fitting_uid="output",
                target_block_uid="log", target_fitting_uid="input",
            ),
        ],
    )


# ------------------------------------------------------------------
# Catalog
# ------------------------------------------------------------------


CATALOG: list[SampleEntry] = [
    # Timer / scheduled
    SampleEntry(
        "heartbeat", "Scheduled",
        "Heartbeat (10s)",
        "Fires every 10 seconds, logs a timestamped ping. Great for testing schedules.",
        "favorite",
        _heartbeat, _heartbeat_schedule,
    ),
    SampleEntry(
        "daily_weather_report", "Scheduled",
        "Daily Weather Report",
        "Weather report with timestamp. Schedule disabled by default — edit the cron or run manually.",
        "schedule",
        _daily_weather_report, _daily_weather_schedule,
    ),
    SampleEntry(
        "work_hours_monitor", "Scheduled",
        "Work Hours Monitor",
        "Polls weather every 30s during work hours (Mon-Fri 8-18), 4x slower at night.",
        "work",
        _work_hours_monitor, _work_hours_schedule,
    ),
    SampleEntry(
        "multi_slot_poller", "Scheduled",
        "Multi-Slot Poller",
        "Runs at 08:00, 12:00, and 17:00 on weekdays. Good for fixed-time reports.",
        "alarm",
        _multi_slot_poller, _multi_slot_schedule,
    ),
    # Data
    SampleEntry(
        "slow_pipeline", "Data",
        "Sleep Demo",
        "Timer -> wait 3s (sleep block) -> log. Demonstrates the wait block.",
        "hourglass_empty",
        _slow_pipeline,
    ),
    SampleEntry(
        "etl_pipeline", "Data",
        "ETL Pipeline",
        "Static data -> parse JSON -> filter -> sort. Classic ETL pattern.",
        "filter_alt",
        _etl_pipeline,
    ),
    SampleEntry(
        "http_json_pipeline", "Data",
        "HTTP JSON Fetch",
        "Fetches a public JSON API and logs the response status.",
        "http",
        _http_json_pipeline,
    ),
    # News
    SampleEntry(
        "news_digest", "News",
        "News Digest (LLM)",
        "RSS feed -> LLM summarizer. Requires an OpenAI API key.",
        "newspaper",
        _news_digest,
    ),
    SampleEntry(
        "tagesschau_digest", "News",
        "Tagesschau Headlines",
        "Fetches latest Tagesschau articles. No API key needed.",
        "public",
        _tagesschau_digest,
    ),
    # Variables & Cache
    SampleEntry(
        "variable_counter_pipeline", "Data",
        "Variable Counter",
        "Increments a pipeline-scoped counter each run, computes labels and totals.",
        "calculate",
        _variable_counter_pipeline,
    ),
    SampleEntry(
        "cached_http_pipeline", "Data",
        "Cached HTTP Fetch",
        "Reads from cache first; on miss, fetches HTTP and stores for 60s.",
        "cached",
        _cached_http_pipeline,
    ),
    # Guard
    SampleEntry(
        "periodic_guard_demo", "Flow Control",
        "Periodic Guard Demo",
        "Pauses when second is not divisible by 5, resumes when it is. "
        "Watch the console for pause/resume cycles.",
        "shield",
        _periodic_guard_demo,
    ),
]

_CATALOG_BY_KEY = {s.key: s for s in CATALOG}


# ------------------------------------------------------------------
# Endpoints
# ------------------------------------------------------------------


@router.get("/catalog")
async def list_catalog(
    db: AsyncIOMotorDatabase = Depends(get_db),  # type: ignore[type-arg]
) -> list[dict[str, Any]]:
    """Return the full sample catalog with installed status."""
    installed_tags: set[str] = set()
    async for doc in db["pipelines"].find(
        {"tags": {"$regex": f"^{SAMPLE_TAG_PREFIX}"}},
        {"tags": 1, "_id": 1},
    ):
        for tag in doc.get("tags", []):
            if tag.startswith(SAMPLE_TAG_PREFIX):
                installed_tags.add(tag)

    result: list[dict[str, Any]] = []
    for s in CATALOG:
        result.append({
            "key": s.key,
            "category": s.category,
            "title": s.title,
            "description": s.description,
            "icon": s.icon,
            "has_schedule": s.schedule_fn is not None,
            "installed": s.tag in installed_tags,
        })
    return result


@router.post("/add/{key}", status_code=201)
async def add_sample(
    key: str,
    db: AsyncIOMotorDatabase = Depends(get_db),  # type: ignore[type-arg]
) -> dict[str, Any]:
    """Add a single sample pipeline by key."""
    entry = _CATALOG_BY_KEY.get(key)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"Unknown sample: {key}")

    existing = await db["pipelines"].find_one({"tags": entry.tag})
    if existing:
        return {"status": "already_installed", "pipeline_id": str(existing["_id"])}

    now = datetime.now(UTC)
    pipeline: PipelineDefinition = entry.pipeline_fn()
    pipeline.created_at = now
    pipeline.updated_at = now

    doc = model_to_doc(pipeline)
    doc.pop("_id", None)
    result = await db["pipelines"].insert_one(doc)
    pipeline_id = str(result.inserted_id)

    if entry.schedule_fn:
        schedule: Schedule = entry.schedule_fn()
        schedule.pipeline_id = pipeline_id
        schedule.next_run_at = now
        schedule.created_at = now
        sdoc = model_to_doc(schedule)
        sdoc.pop("_id", None)
        await db["schedules"].insert_one(sdoc)

    return {"status": "created", "pipeline_id": pipeline_id}


@router.delete("/remove/{key}")
async def remove_sample(
    key: str,
    db: AsyncIOMotorDatabase = Depends(get_db),  # type: ignore[type-arg]
) -> dict[str, Any]:
    """Remove a single sample pipeline and its schedule/runs."""
    entry = _CATALOG_BY_KEY.get(key)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"Unknown sample: {key}")

    tag = entry.tag

    # Find pipeline IDs with this tag
    pipeline_ids: list[str] = []
    async for doc in db["pipelines"].find({"tags": tag}, {"_id": 1}):
        pipeline_ids.append(str(doc["_id"]))

    if not pipeline_ids:
        return {"status": "not_installed"}

    await db["schedules"].delete_many({"tags": tag})
    await db["pipelines"].delete_many({"tags": tag})
    await db["runs"].delete_many({"pipeline_id": {"$in": pipeline_ids}})

    return {"status": "removed", "pipelines_removed": len(pipeline_ids)}
