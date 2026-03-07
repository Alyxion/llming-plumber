# Plumber — Architecture Concept

## Overview

Plumber is a pipeline automation engine built on FastAPI, MongoDB, and Redis.
It can run as a standalone server or be mounted into an existing FastAPI
application as a sub-application.

**MongoDB** is the source of truth — pipeline definitions, run state, logs,
credentials. **Redis + ARQ** handles run dispatch and coordination — instant
push-based delivery, exactly-once claiming, retries, and cron scheduling.

---

## Deployment Modes

```
┌─────────────────────────────────────────────────────────┐
│  Mode 1: All-in-One                                     │
│  plumber serve --mode=all                               │
│                                                         │
│  ┌─────────────┐  ┌─────────────┐                       │
│  │   UI (API)   │  │ ARQ Lemming │                       │
│  │  /api/...    │  │  (asyncio)  │                       │
│  └──────┬───────┘  └──────┬──────┘                       │
│         │                 │                              │
│         ▼                 ▼                              │
│  ┌──────────┐      ┌──────────┐                          │
│  │ MongoDB  │      │  Redis   │                          │
│  └──────────┘      └──────────┘                          │
└─────────────────────────────────────────────────────────┘

┌──────────────────┐       ┌──────────────────┐
│  Mode 2: UI Only │       │  Mode 3: Lemming │
│  plumber serve   │       │  plumber serve   │
│   --mode=ui      │       │   --mode=worker  │
│                  │       │                  │
│  ┌────────────┐  │       │  ┌────────────┐  │
│  │  UI (API)  │  │       │  │ ARQ Lemming│  │
│  │  /api/...  │  │       │  │  (asyncio) │  │
│  └─────┬──────┘  │       │  └─────┬──────┘  │
│        │         │       │        │         │
└────────┼─────────┘       └────────┼─────────┘
         │                          │
    ┌────▼────┐  ┌──────┐    ┌──────▼───┐
    │ MongoDB │  │Redis │    │  Redis   │
    └─────────┘  └──┬───┘    └──────────┘
                    │           │
    UI enqueues ────┘           └──── Lemmings dequeue
    runs into Redis                   runs from Redis
```

The UI server enqueues runs into Redis (via ARQ). Lemmings pick them up
instantly. MongoDB is read/written by both sides for persistent state,
but Redis is the dispatch mechanism — no polling, no wasted queries.

---

## How ARQ Fits In

[ARQ](https://github.com/python-arq/arq) is an async-native Redis job
queue for Python. It replaces our previous MongoDB polling design with
push-based dispatch.

### What ARQ handles

| Concern | ARQ feature |
|---|---|
| Run dispatch | `await pool.enqueue_job("execute_pipeline", run_id=...)` |
| Exactly-once delivery | Redis atomic `BRPOPLPUSH` — only one lemming gets each run |
| Concurrency control | `max_jobs` setting per lemming |
| Retries | Built-in retry with configurable count and backoff |
| Deferred execution | `_defer_until` and `_defer_by` parameters |
| Cron scheduling | `cron_jobs` in `LemmingSettings` |
| Timeouts | `_timeout` per run, `health_check_interval` for crash detection |
| Run results | Stored in Redis with configurable TTL |

### What MongoDB still handles

| Concern | Why MongoDB, not Redis |
|---|---|
| Pipeline definitions | Complex documents with blocks, pipes, versions — need querying and indexing |
| Run history & state | Permanent record, queryable by status/date/pipeline/lemming |
| Execution logs | Append-only, indexed by run_id + timestamp, retained indefinitely |
| Credentials | Encrypted at rest, queried by pipeline at execution time |
| Schedules | Persisted schedule definitions (ARQ cron triggers them, MongoDB stores them) |

### Data Flow

```
  User clicks "Run"          ARQ enqueues              Lemming picks up
  or cron fires              into Redis                from Redis
       │                         │                         │
       ▼                         ▼                         ▼
  ┌─────────┐   enqueue    ┌──────────┐   BRPOP     ┌──────────┐
  │ UI/Cron │ ───────────► │  Redis   │ ──────────► │ Lemming  │
  └────┬────┘              └──────────┘              └────┬─────┘
       │                                                  │
       │  write run doc                                   │  update status,
       │  status=pending                                  │  write logs
       ▼                                                  ▼
  ┌──────────┐                                      ┌──────────┐
  │ MongoDB  │ ◄────────────────────────────────────│ MongoDB  │
  └──────────┘                                      └──────────┘
```

1. **Trigger** — UI API call, webhook, or ARQ cron fires.
2. **Create run doc** — Write a run document to MongoDB with `status: "queued"`.
3. **Enqueue** — `await arq_pool.enqueue_job("execute_pipeline", run_id=str(run_doc._id))`.
4. **Lemming picks up** — ARQ delivers to exactly one lemming (instant, no polling).
5. **Execute** — Lemming reads pipeline definition from MongoDB, runs blocks sequentially, updates `status: "running"`, writes logs to `run_logs`.
6. **Complete** — Lemming sets `status: "completed"` or `"failed"` in MongoDB.

---

## Embedding into an Existing FastAPI App

Plumber exposes a `create_app()` factory and a mountable `APIRouter`.
An existing application can integrate it in two ways:

```python
# Option A — mount as sub-application
from llming_plumber import create_app as create_plumber_app

app = FastAPI()
app.mount("/plumber", create_plumber_app(mode="all"))
```

```python
# Option B — include just the API router (more control)
from llming_plumber.api import router as plumber_router
from llming_plumber.worker import create_lemming

app = FastAPI()
app.include_router(plumber_router, prefix="/plumber/api")

# Start an in-process ARQ lemming alongside the API
@app.on_event("startup")
async def start_lemming():
    app.state.plumber_lemming = create_lemming()
    # Lemming runs as a background asyncio task
```

The lemming can also run as a standalone process with no HTTP server at all:

```bash
arq llming_plumber.worker.LemmingSettings
```

---

## MongoDB Collections

MongoDB is the persistent source of truth. Redis is ephemeral dispatch.

| Collection | Purpose |
|---|---|
| `plumber.pipelines` | Pipeline definitions (name, blocks, pipes, config, version) |
| `plumber.runs` | Run instances — one document per execution of a pipeline |
| `plumber.run_logs` | Append-only log entries for each run step |
| `plumber.schedules` | Cron/interval definitions tied to pipelines |
| `plumber.credentials` | Encrypted credential store for connected services |

Note: the `plumber.locks` collection from the previous design is **gone** —
ARQ handles distributed locking via Redis.

### Key Indexes

```javascript
// runs — query by status, pipeline, date
db.plumber.runs.createIndex({ status: 1, created_at: -1 })
db.plumber.runs.createIndex({ pipeline_id: 1, created_at: -1 })
db.plumber.runs.createIndex({ lemming_id: 1, status: 1 })

// run_logs — query logs for a specific run, ordered
db.plumber.run_logs.createIndex({ run_id: 1, ts: 1 })

// schedules — find enabled schedules
db.plumber.schedules.createIndex({ enabled: 1 })
```

---

## Run Lifecycle

```
                  schedule/
  ┌──────────┐    API trigger   ┌──────────┐   ARQ dispatches  ┌───────────┐
  │          │    + enqueue     │          │   to a lemming     │           │
  │  (none)  │ ──────────────► │  queued  │ ─────────────────► │  running  │
  │          │                  │          │                    │           │
  └──────────┘                  └──────────┘                    └─────┬─────┘
                                     │                               │
                                     │ cancelled                     ├──► completed
                                     ▼                               │
                                ┌──────────┐                         ├──► failed
                                │ cancelled│                         │
                                └──────────┘                         └──► retrying
                                                                          │
                                                            re-enqueue into Redis,
                                                            back to queued
```

### Run Document

```javascript
{
  _id: ObjectId,
  pipeline_id: ObjectId,
  pipeline_version: 3,
  status: "queued" | "running" | "completed" | "failed" | "retrying" | "cancelled",

  // Scheduling
  created_at: ISODate,
  started_at: ISODate | null,
  finished_at: ISODate | null,

  // Lemming assignment (set when ARQ lemming picks up the run)
  lemming_id: "prod-lemming-02:48291:a3f1c9e2" | null,

  // ARQ reference
  arq_job_id: "arq:job:abc123" | null,

  // Execution
  current_block: "block-id" | null,
  block_states: { "block-id": { status, duration_ms, error? } },
  input: { ... },               // trigger payload

  // Debug mode — enables Redis debug trace (see Debug Trace section)
  debug: false,

  // Retry
  attempt: 1,
  max_attempts: 3,

  // Audit
  error: "..." | null,
  tags: ["nightly", "import"],
}
```

---

## ARQ Lemming Configuration

```python
import socket, os
from uuid import uuid4
from arq import cron
from arq.connections import RedisSettings

from llming_plumber.config import settings
from llming_plumber.worker.executor import execute_pipeline
from llming_plumber.worker.scheduler import check_schedules


LEMMING_ID = f"{socket.gethostname()}:{os.getpid()}:{uuid4().hex[:8]}"


class LemmingSettings:
    """ARQ lemming configuration — run with `arq llming_plumber.worker.LemmingSettings`."""

    redis_settings = RedisSettings.from_dsn(settings.redis_url)

    # The task functions ARQ can dispatch to
    functions = [execute_pipeline]

    # Built-in cron: check MongoDB schedules every minute, enqueue due pipelines
    cron_jobs = [cron(check_schedules, minute=None)]  # every minute

    # Concurrency: how many runs this lemming handles simultaneously
    max_jobs = settings.lemming_concurrency  # default: 4

    # Health check: if a lemming doesn't report in this interval, ARQ marks it dead
    health_check_interval = 30

    # Run timeout: kill runs that take longer than this
    job_timeout = 3600  # 1 hour default

    # Pass lemming_id to all tasks via ctx
    on_startup = startup
    on_shutdown = shutdown


async def startup(ctx):
    """Called once when the lemming starts."""
    ctx["lemming_id"] = LEMMING_ID
    ctx["db"] = await connect_to_mongodb()
    # Register lemming heartbeat in MongoDB
    await ctx["db"].plumber.lemmings.update_one(
        {"lemming_id": LEMMING_ID},
        {"$set": {"started_at": datetime.utcnow(), "status": "online"}},
        upsert=True,
    )


async def shutdown(ctx):
    """Called when the lemming shuts down."""
    await ctx["db"].plumber.lemmings.update_one(
        {"lemming_id": LEMMING_ID},
        {"$set": {"status": "offline", "stopped_at": datetime.utcnow()}},
    )
```

### Task Function

```python
async def execute_pipeline(ctx: dict, *, run_id: str) -> dict:
    """Execute a pipeline run. Called by ARQ when a run is dequeued."""
    db = ctx["db"]
    lemming_id = ctx["lemming_id"]

    # Mark running in MongoDB
    run = await db.plumber.runs.find_one_and_update(
        {"_id": ObjectId(run_id), "status": "queued"},
        {"$set": {
            "status": "running",
            "lemming_id": lemming_id,
            "started_at": datetime.utcnow(),
        }},
        return_document=ReturnDocument.AFTER,
    )
    if not run:
        return {"skipped": True}  # already cancelled or picked up

    pipeline = await db.plumber.pipelines.find_one({"_id": run["pipeline_id"]})

    try:
        result = await run_blocks(pipeline, run, db, lemming_id)
        await db.plumber.runs.update_one(
            {"_id": ObjectId(run_id)},
            {"$set": {
                "status": "completed",
                "output": result,
                "finished_at": datetime.utcnow(),
            }},
        )
        return result

    except Exception as e:
        await db.plumber.runs.update_one(
            {"_id": ObjectId(run_id)},
            {"$set": {
                "status": "failed",
                "error": str(e),
                "finished_at": datetime.utcnow(),
            }},
        )
        raise  # ARQ handles retry if configured
```

### Enqueueing from the UI

```python
from arq.connections import create_pool

# In the FastAPI startup
@app.on_event("startup")
async def init_arq():
    app.state.arq_pool = await create_pool(RedisSettings.from_dsn(settings.redis_url))

# In the API route
@router.post("/pipelines/{pipeline_id}/run")
async def run_pipeline(pipeline_id: str, request: Request):
    run_doc = {
        "pipeline_id": ObjectId(pipeline_id),
        "status": "queued",
        "created_at": datetime.utcnow(),
        "attempt": 0,
        "input": await request.json(),
    }
    result = await db.plumber.runs.insert_one(run_doc)
    run_id = str(result.inserted_id)

    # Enqueue into Redis — a lemming picks this up instantly
    await request.app.state.arq_pool.enqueue_job(
        "execute_pipeline", run_id=run_id,
    )
    return {"run_id": run_id, "status": "queued"}
```

---

## Cron / Scheduled Pipelines

Schedules are stored in MongoDB. ARQ's built-in cron runs a check every
minute that scans for due schedules and enqueues them:

```python
async def check_schedules(ctx: dict):
    """ARQ cron — runs every minute. Finds due schedules, enqueues runs."""
    db = ctx["db"]
    now = datetime.utcnow()
    pool = ctx["redis"]  # ARQ provides this

    async for schedule in db.plumber.schedules.find({
        "enabled": True,
        "next_run_at": {"$lte": now},
    }):
        # Create run doc
        run_doc = {
            "pipeline_id": schedule["pipeline_id"],
            "status": "queued",
            "created_at": now,
            "attempt": 0,
            "tags": schedule.get("tags", []),
        }
        result = await db.plumber.runs.insert_one(run_doc)

        # Enqueue
        await pool.enqueue_job("execute_pipeline", run_id=str(result.inserted_id))

        # Advance next_run_at
        next_run = compute_next_run(schedule["cron_expression"], now)
        await db.plumber.schedules.update_one(
            {"_id": schedule["_id"]},
            {"$set": {"next_run_at": next_run, "last_run_at": now}},
        )
```

---

## Lemming Identity & Logging

Every lemming process generates a unique identity at startup:

```python
lemming_id = f"{hostname}:{pid}:{uuid4().hex[:8]}"
# e.g. "prod-lemming-02:48291:a3f1c9e2"
```

This ID is:
- Stored in `lemming_id` on every claimed run document.
- Included in every log entry the lemming emits.
- Written to every `plumber.run_logs` document.
- Registered in `plumber.lemmings` collection with heartbeat timestamps.

### Structured Log Format

All log output uses structured JSON logging:

```json
{
  "ts": "2026-03-06T14:22:03.112Z",
  "level": "info",
  "lemming_id": "prod-lemming-02:48291:a3f1c9e2",
  "run_id": "660a1f...",
  "pipeline": "daily-news-import",
  "block": "fetch-rss",
  "msg": "Block completed",
  "duration_ms": 342
}
```

### Run Log Collection

In addition to stdout/stderr, every block execution writes to `plumber.run_logs`.
Logging is capped by `_RunLogger` — at most `MAX_RUN_LOG_ENTRIES` (default 50)
entries per run. Error-level entries are always written regardless of the cap.

```javascript
{
  _id: ObjectId,
  run_id: ObjectId,
  lemming_id: "prod-lemming-02:48291:a3f1c9e2",
  block_id: "fetch-rss",
  block_type: "rss_reader",
  ts: ISODate,
  level: "info" | "warning" | "error",
  msg: "Fetched 42 parcels from https://...",
  duration_ms: 342,
  output_summary: null,  // only set when PLUMBER_LOG_BLOCK_OUTPUT=1
}
```

This makes it trivial to answer:
- "Which lemming ran run X?" → `db.plumber.runs.findOne({_id: X}).lemming_id`
- "What did run X do?" → `db.plumber.run_logs.find({run_id: X}).sort({ts: 1})`
- "What is lemming Y doing?" → `db.plumber.runs.find({lemming_id: Y, status: "running"})`
- "Is lemming Y alive?" → `db.plumber.lemmings.findOne({lemming_id: Y})`

---

## Data Protection Defaults

Block output data may contain private or sensitive information. To prevent
accidental leakage into MongoDB, the executor applies these defaults:

- **`LOG_BLOCK_OUTPUT`** (env: `PLUMBER_LOG_BLOCK_OUTPUT`, default `0`) —
  block output is **not** written to `block_states` or `run_logs` unless
  explicitly opted in. Only status, timing, and errors are persisted.
- **`_RunLogger`** caps log writes to `MAX_RUN_LOG_ENTRIES` per run
  (default 50). Error entries are always written.
- **Error messages** are truncated to `MAX_ERROR_MESSAGE_LENGTH` (default
  2000 chars) before writing to MongoDB.

To opt in to output logging for debugging:

```bash
PLUMBER_LOG_BLOCK_OUTPUT=1
```

For production inspection of intermediate data, use the debug trace
system (Redis, short-lived TTL) instead of persisting to MongoDB.

---

## Run Console

Every pipeline run has a virtual console that blocks can write to via
`ctx.log("message")`. The console is backed by a Redis list with automatic
TTL cleanup.

```
Redis key: plumber:console:{run_id}
```

### Writing

Blocks call `await ctx.log("Processing item 42")` during execution.
The `BlockContext.log()` method delegates to `RunConsole.write()`, which
appends a JSON entry to the Redis list using a pipeline of
`RPUSH + LTRIM + EXPIRE` for atomicity.

Each entry contains: `ts`, `block_id`, `level`, `msg`.

### Reading

```python
from llming_plumber.worker.console import read_console

entries = await read_console(redis, run_id, offset=0, limit=200)
# → [{"ts": "...", "block_id": "log-1", "level": "info", "msg": "Hello"}, ...]
```

### Limits

| Setting | Default | Env var |
|---|---|---|
| TTL | 1 hour | `PLUMBER_CONSOLE_TTL_SECONDS` |
| Max entries | 5000 | `PLUMBER_CONSOLE_MAX_ENTRIES` |

The console is designed for real-time inspection and short-term review.
Entries auto-expire after the TTL.

---

## Debug Trace

When a run has `debug: true`, every block execution writes a lightweight
summary to Redis so users can inspect intermediate data flow after the fact.
All debug keys auto-expire via `DEBUG_TTL_SECONDS` (default 1 hour).

### Redis Key Layout

```
plumber:debug:{run_id}:order              → JSON list of block UIDs (exec order)
plumber:debug:{run_id}:{block_uid}        → JSON block summary (timing, status, parcel count)
plumber:debug:{run_id}:{block_uid}:g      → JSON list of item glimpses (short labels)
plumber:debug:{run_id}:{block_uid}:p:{i}  → JSON parcel detail (full fields, truncated)
```

### What Gets Stored

- **Execution order** — the topological order blocks ran in.
- **Block summaries** — type, duration, parcel count, status, error.
- **Glimpses** — short human-readable labels extracted from parcel fields
  (looks for `name`, `filename`, `title`, `url`, `id`, etc.). Up to
  `DEBUG_MAX_GLIMPSES` (200) per block.
- **Parcel detail** — full field data for the first `DEBUG_MAX_PARCELS`
  (20) parcels, with large values truncated to `DEBUG_MAX_PARCEL_BYTES`
  (100 KB) per parcel.

### Reading Debug Data

```python
from llming_plumber.worker.debug_trace import (
    get_debug_trace,
    get_debug_parcel,
    search_debug_parcels,
)

# Full trace overview
trace = await get_debug_trace(redis, run_id)
# → {"run_id": "...", "order": [...], "blocks": {...}}

# Single parcel detail
parcel = await get_debug_parcel(redis, run_id, "block-uid", index=0)

# Search glimpses by label
results = await search_debug_parcels(redis, run_id, "block-uid", label_contains="report")
```

### Limits

| Setting | Default | Env var |
|---|---|---|
| TTL | 1 hour | `PLUMBER_DEBUG_TTL_SECONDS` |
| Max glimpses per block | 200 | `PLUMBER_DEBUG_MAX_GLIMPSES` |
| Max parcels with detail | 20 | `PLUMBER_DEBUG_MAX_PARCELS` |
| Max parcel JSON size | 100 KB | `PLUMBER_DEBUG_MAX_PARCEL_BYTES` |

---

## Wall-Clock Timeout

The executor enforces a hard wall-clock limit on every pipeline run
via `MAX_RUN_WALL_SECONDS` (default 3600 = 1 hour, env:
`PLUMBER_MAX_RUN_WALL_SECONDS`).

The timeout is checked:
1. **Before each block** starts executing.
2. **Between fan-out batches** during iteration.

When the limit is exceeded, a `ResourceLimitError` is raised and the run
fails with a clear error message. This prevents infinite loops and
runaway pipelines from consuming resources indefinitely.

---

## Real-Time UI Updates (via Redis Pub/Sub)

When a lemming updates a run's status, it also publishes to a Redis channel.
The UI server subscribes and pushes updates to the browser via WebSocket:

```python
# Lemming side — after status change
await redis.publish("plumber:run_updates", json.dumps({
    "run_id": run_id,
    "status": "completed",
    "lemming_id": lemming_id,
}))

# UI side — WebSocket endpoint
@router.websocket("/ws/runs")
async def run_updates_ws(websocket: WebSocket):
    await websocket.accept()
    pubsub = redis.pubsub()
    await pubsub.subscribe("plumber:run_updates")
    async for message in pubsub.listen():
        if message["type"] == "message":
            await websocket.send_text(message["data"])
```

No polling needed — the UI updates the moment a run changes state.

---

## API Surface (UI Server)

The UI server exposes a REST API. All endpoints are grouped under a
single `APIRouter` so they can be mounted at any prefix.

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/pipelines` | List pipelines |
| `POST` | `/api/pipelines` | Create pipeline |
| `GET` | `/api/pipelines/{id}` | Get pipeline detail |
| `PUT` | `/api/pipelines/{id}` | Update pipeline |
| `DELETE` | `/api/pipelines/{id}` | Delete pipeline |
| `POST` | `/api/pipelines/{id}/run` | Trigger a run (writes to MongoDB + enqueues via ARQ) |
| `GET` | `/api/runs` | List runs (filterable by status, pipeline, date) |
| `GET` | `/api/runs/{id}` | Get run detail + block states |
| `GET` | `/api/runs/{id}/logs` | Get run execution logs |
| `GET` | `/api/runs/{id}/console` | Read run console entries (Redis) |
| `GET` | `/api/runs/{id}/debug` | Get debug trace overview (Redis, requires `debug: true`) |
| `GET` | `/api/runs/{id}/debug/{block_uid}/{index}` | Get single parcel detail from debug trace |
| `POST` | `/api/runs/{id}/cancel` | Cancel a pending/running run |
| `POST` | `/api/runs/{id}/retry` | Retry a failed run |
| `GET` | `/api/schedules` | List schedules |
| `POST` | `/api/schedules` | Create schedule |
| `PUT` | `/api/schedules/{id}` | Update schedule |
| `DELETE` | `/api/schedules/{id}` | Delete schedule |
| `GET` | `/api/blocks` | List all registered block types from the catalog |
| `GET` | `/api/lemmings` | List active lemmings (from `plumber.lemmings` collection) |
| `GET` | `/api/health` | Health check (MongoDB + Redis connectivity) |
| `WS` | `/ws/runs` | Real-time run status updates via WebSocket |

---

## Frontend

The visual pipeline editor is a Vue 3 SPA built with Quasar and @vue-flow/core.
See [`frontend/README.md`](../../frontend/README.md) for full documentation.

### Development

```bash
# Backend (port 8100)
.venv/bin/python -m uvicorn llming_plumber.main:app --port 8100 --reload

# Frontend dev server (port 9000, proxies /api → 8100)
cd frontend && npm run dev
```

### Production

```bash
cd frontend && npm run build   # → frontend/dist/
# FastAPI auto-serves dist/ as SPA — only port 8100 needed
```

### Plugin System

The frontend supports extensibility via plugins that can provide custom
node renderers, sidebar panels, themes, toolbar actions, and block config
editors. See [`frontend/README.md`](../../frontend/README.md) for the
plugin API reference.

---

## Configuration

All configuration is via environment variables (loaded from `.env`):

```bash
# MongoDB
PLUMBER_MONGO_URI=mongodb://localhost:27017
PLUMBER_MONGO_DB=plumber

# Redis
PLUMBER_REDIS_URL=redis://localhost:6379/0

# Mode
PLUMBER_MODE=all                # all | ui | worker

# Lemming
PLUMBER_LEMMING_CONCURRENCY=4   # concurrent run slots per lemming process
PLUMBER_RUN_TIMEOUT=3600        # max seconds per run
PLUMBER_HEALTH_CHECK_INTERVAL=30

# API
PLUMBER_API_PREFIX=/api         # prefix for all API routes
PLUMBER_HOST=0.0.0.0
PLUMBER_PORT=8100

# Credentials encryption
PLUMBER_SECRET_KEY=...          # for encrypting stored credentials

# External services
OPENWEATHER_API_KEY=...
OPENWEATHER_API_BASE=https://api.openweathermap.org
```

---

## Project Layout

```
llming_plumber/                  # the installable package
├── __init__.py              # create_app() factory
├── config.py                # Settings (pydantic-settings, reads .env)
├── models/
│   ├── pipeline.py          # Pipeline, Block, Pipe
│   ├── run.py               # Run, BlockState
│   ├── schedule.py          # Schedule
│   └── log.py               # RunLog
├── api/
│   ├── __init__.py          # router aggregation
│   ├── pipelines.py
│   ├── runs.py
│   ├── schedules.py
│   ├── lemmings.py
│   └── ws.py                # WebSocket endpoint for real-time updates
├── worker/
│   ├── __init__.py          # LemmingSettings for ARQ
│   ├── executor.py          # execute_pipeline task + block runner
│   ├── console.py           # RunConsole — Redis-backed per-run console
│   ├── debug_trace.py       # DebugTracer — Redis-backed intermediate data inspector
│   └── scheduler.py         # check_schedules cron task
├── blocks/                  # Building-block implementations
│   ├── base.py              # BaseBlock, BlockInput, BlockOutput, BlockContext
│   ├── limits.py            # Resource limits (file size, fan-out, timeouts, …)
│   ├── registry.py          # BlockRegistry — auto-discovers all block types
│   ├── core/                # Generic building blocks
│   │   ├── http_request.py
│   │   ├── split.py         # SplitBlock — fan-out over list items
│   │   ├── collect.py       # CollectBlock — fan-in, gather items into list
│   │   ├── range_block.py   # RangeBlock — generate numbered sequences
│   │   ├── wait.py          # WaitBlock — async sleep with cap
│   │   ├── log.py           # LogBlock — write to run console
│   │   ├── text_template.py # TextTemplateBlock — safe expression interpolation
│   │   ├── safe_eval.py     # AST-based restricted expression evaluator
│   │   └── ...              # filter, sort, merge, aggregate, csv, json, etc.
│   ├── documents/           # Document generation & parsing
│   │   ├── excel_builder.py # ExcelBuilderBlock — multi-sheet Excel workbooks
│   │   ├── pdf_builder.py   # PdfBuilderBlock — PDF generation
│   │   └── ...              # word, powerpoint, parquet, yaml, readers/writers
│   ├── weather/             # Weather data blocks
│   │   ├── openweathermap.py
│   │   └── dwd.py
│   ├── news/                # News & feeds
│   │   ├── rss_reader.py
│   │   ├── tagesschau.py
│   │   └── news_api.py
│   └── government/          # German public data (bund.dev)
│       ├── autobahn.py
│       └── pegel_online.py
├── mcp/                     # MCP tool generation (blocks & pipelines → MCP tools)
│   ├── __init__.py
│   ├── adapter.py           # blocks_to_mcp_tools(), pipeline_to_mcp_tool()
│   └── server.py            # MCP server entrypoint
├── db.py                    # MongoDB + Redis connection helpers
└── cli.py                   # `llming-plumber serve --mode=...`
                             # `llming-plumber mcp serve --blocks ...`

frontend/                       # Vue 3 + Quasar + @vue-flow/core SPA
├── src/
│   ├── layouts/             # MainLayout.vue (header, sidebar, content)
│   ├── pages/               # PipelineList, PipelineEditor, RunList
│   ├── components/          # BlockNode, BlockPalette, BlockConfigPanel
│   ├── stores/              # Pinia stores (pipeline, theme)
│   ├── themes/              # Theme system (Lodge, Daylight, Midnight, Forest)
│   ├── plugins/             # Plugin system for extensibility
│   ├── composables/         # useApi (Axios)
│   └── types/               # TypeScript types mirroring Python models
├── index.html
├── vite.config.ts           # Vite + proxy to FastAPI at port 8100
└── package.json
```

---

## API Response Caching

External APIs like OpenWeatherMap and NewsAPI have rate limits and/or
per-request costs. During development, running the same test 50 times
in a row should not burn through the entire daily quota.

### Design

Blocks that call rate-limited external APIs use a **transparent response
cache** backed by Redis. The cache key is derived from the block type +
a hash of the input parameters (e.g. city, query, date range). Identical
requests return the cached response instead of hitting the external API.

```
  Block.execute(input)
       │
       ▼
  ┌──────────────┐    cache hit     ┌────────────┐
  │ Cache lookup │ ───────────────► │ Return     │
  │ (Redis)      │                  │ cached     │
  └──────┬───────┘                  └────────────┘
         │ cache miss
         ▼
  ┌──────────────┐
  │ Call external│
  │ API          │
  └──────┬───────┘
         │
         ▼
  ┌──────────────┐
  │ Store in     │
  │ cache + TTL  │
  └──────────────┘
```

### Cache Modes

Controlled via the `PLUMBER_API_CACHE` environment variable:

| Value | Behavior | Use case |
|---|---|---|
| `on` (default) | Cache enabled, responses stored with a per-block TTL (capped at `PLUMBER_API_CACHE_MAX_TTL`) | Local development, manual testing |
| `off` | Cache fully disabled, every call hits the real API | CI full integration tests, production |
| `aggressive` | Cache enabled with longer TTLs (24h), ignores max TTL cap | Rapid local iteration, offline-friendly |

### Max TTL

Even with caching on, responses are **never cached forever**. A global
max TTL caps how long any cached response can live, regardless of the
per-block setting:

```bash
PLUMBER_API_CACHE_MAX_TTL=1800   # 30 minutes (default)
```

The effective TTL for any cached response is:
```
effective_ttl = min(block.cache_ttl, PLUMBER_API_CACHE_MAX_TTL)
```

In `aggressive` mode the max TTL cap is ignored and per-block TTLs are
multiplied by 10 (e.g. weather 10min → 100min, news 15min → 150min),
but still never exceed 24 hours.

### Per-Block TTL

Each block declares its own default cache TTL:

```python
class WeatherBlock(BaseBlock[WeatherInput, WeatherOutput]):
    block_type = "weather"
    cache_ttl = 600          # 10 minutes — weather doesn't change that fast
    ...

class NewsApiBlock(BaseBlock[NewsInput, NewsOutput]):
    block_type = "news_api"
    cache_ttl = 900          # 15 minutes — headlines don't refresh every second
    ...

class HttpRequestBlock(BaseBlock[HttpInput, HttpOutput]):
    block_type = "http_request"
    cache_ttl = 0            # no caching — generic HTTP is too unpredictable
    ...
```

### Cache Key

```python
cache_key = f"plumber:cache:{block_type}:{hashlib.sha256(input.model_dump_json().encode()).hexdigest()}"
```

Same block type + same input = same cache key. Changing any input
parameter (different city, different query) produces a different key.

### Test Configuration

```bash
# .env for local development — cache on by default
PLUMBER_API_CACHE=on

# pytest CI config — cache off, test real behavior
# (set in CI environment or conftest.py)
PLUMBER_API_CACHE=off
```

In `conftest.py`, tests that need real API responses set `PLUMBER_API_CACHE=off`.
Tests that just validate block logic mock the HTTP layer entirely and
don't touch the cache at all.

---

## Summary

| Concern | Solution |
|---|---|
| Persistent storage | MongoDB — pipelines, runs, logs, credentials, schedules |
| Run dispatch | ARQ + Redis — push-based, instant delivery, no polling |
| Exactly-once execution | ARQ's atomic Redis operations — only one lemming gets each run |
| Retries | ARQ built-in retry with configurable count and backoff |
| Cron scheduling | ARQ `cron_jobs` triggers `check_schedules` every minute |
| Embeddable | `create_app()` factory + mountable `APIRouter` |
| Deployment flexibility | Three modes: `all`, `ui`, `worker` — controlled by one env var |
| Horizontal scaling | N lemming processes/servers, all consuming from the same Redis queue |
| Crash recovery | ARQ detects dead lemmings via `health_check_interval`, re-queues runs |
| Real-time UI | Redis pub/sub → WebSocket, no polling |
| Run console | Redis list per run — blocks write via `ctx.log()`, auto-expire after 1h |
| Debug trace | Redis-backed intermediate data snapshots — glimpses, parcel detail, auto-expire |
| Data protection | Block output not persisted by default — opt in via `PLUMBER_LOG_BLOCK_OUTPUT` |
| Resource limits | Centralised `limits.py` — file size, fan-out, timeouts, all env-configurable |
| Wall-clock timeout | `MAX_RUN_WALL_SECONDS` checked between blocks and fan-out batches |
| Audit trail | `lemming_id` on every run + append-only `run_logs` collection |
| Structured logging | JSON logs with `lemming_id`, `run_id`, `block_id` on every line |
| API rate limit protection | Redis response cache with per-block TTL, disabled in CI |
