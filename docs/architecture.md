# Plumber — Architecture Concept

## Overview

Plumber is a workflow automation engine built on FastAPI, MongoDB, and Redis.
It can run as a standalone server or be mounted into an existing FastAPI
application as a sub-application.

**MongoDB** is the source of truth — workflow definitions, job state, logs,
credentials. **Redis + ARQ** handles job dispatch and coordination — instant
push-based delivery, exactly-once claiming, retries, and cron scheduling.

---

## Deployment Modes

```
┌─────────────────────────────────────────────────────────┐
│  Mode 1: All-in-One                                     │
│  plumber serve --mode=all                               │
│                                                         │
│  ┌─────────────┐  ┌─────────────┐                       │
│  │   UI (API)   │  │ ARQ Worker  │                       │
│  │  /api/...    │  │  (asyncio)  │                       │
│  └──────┬───────┘  └──────┬──────┘                       │
│         │                 │                              │
│         ▼                 ▼                              │
│  ┌──────────┐      ┌──────────┐                          │
│  │ MongoDB  │      │  Redis   │                          │
│  └──────────┘      └──────────┘                          │
└─────────────────────────────────────────────────────────┘

┌──────────────────┐       ┌──────────────────┐
│  Mode 2: UI Only │       │  Mode 3: Worker  │
│  plumber serve   │       │  plumber serve   │
│   --mode=ui      │       │   --mode=worker  │
│                  │       │                  │
│  ┌────────────┐  │       │  ┌────────────┐  │
│  │  UI (API)  │  │       │  │ ARQ Worker │  │
│  │  /api/...  │  │       │  │  (asyncio) │  │
│  └─────┬──────┘  │       │  └─────┬──────┘  │
│        │         │       │        │         │
└────────┼─────────┘       └────────┼─────────┘
         │                          │
    ┌────▼────┐  ┌──────┐    ┌──────▼───┐
    │ MongoDB │  │Redis │    │  Redis   │
    └─────────┘  └──┬───┘    └──────────┘
                    │           │
    UI enqueues ────┘           └──── Workers dequeue
    jobs into Redis                   jobs from Redis
```

The UI server enqueues jobs into Redis (via ARQ). Workers pick them up
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
| Job dispatch | `await pool.enqueue_job("execute_workflow", job_id=...)` |
| Exactly-once delivery | Redis atomic `BRPOPLPUSH` — only one worker gets each job |
| Concurrency control | `max_jobs` setting per worker |
| Retries | Built-in retry with configurable count and backoff |
| Deferred execution | `_defer_until` and `_defer_by` parameters |
| Cron scheduling | `cron_jobs` in `WorkerSettings` |
| Timeouts | `_timeout` per job, `health_check_interval` for crash detection |
| Job results | Stored in Redis with configurable TTL |

### What MongoDB still handles

| Concern | Why MongoDB, not Redis |
|---|---|
| Workflow definitions | Complex documents with nodes, edges, versions — need querying and indexing |
| Job history & state | Permanent record, queryable by status/date/workflow/worker |
| Execution logs | Append-only, indexed by job_id + timestamp, retained indefinitely |
| Credentials | Encrypted at rest, queried by workflow at execution time |
| Schedules | Persisted schedule definitions (ARQ cron triggers them, MongoDB stores them) |

### Data Flow

```
  User clicks "Run"          ARQ enqueues              Worker picks up
  or cron fires              into Redis                from Redis
       │                         │                         │
       ▼                         ▼                         ▼
  ┌─────────┐   enqueue    ┌──────────┐   BRPOP     ┌──────────┐
  │ UI/Cron │ ───────────► │  Redis   │ ──────────► │  Worker  │
  └────┬────┘              └──────────┘              └────┬─────┘
       │                                                  │
       │  write job doc                                   │  update status,
       │  status=pending                                  │  write logs
       ▼                                                  ▼
  ┌──────────┐                                      ┌──────────┐
  │ MongoDB  │ ◄────────────────────────────────────│ MongoDB  │
  └──────────┘                                      └──────────┘
```

1. **Trigger** — UI API call, webhook, or ARQ cron job fires.
2. **Create job doc** — Write a job document to MongoDB with `status: "queued"`.
3. **Enqueue** — `await arq_pool.enqueue_job("execute_workflow", job_id=str(job_doc._id))`.
4. **Worker picks up** — ARQ delivers to exactly one worker (instant, no polling).
5. **Execute** — Worker reads workflow definition from MongoDB, runs nodes sequentially, updates `status: "running"`, writes logs to `job_logs`.
6. **Complete** — Worker sets `status: "completed"` or `"failed"` in MongoDB.

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
from llming_plumber.worker import create_worker

app = FastAPI()
app.include_router(plumber_router, prefix="/plumber/api")

# Start an in-process ARQ worker alongside the API
@app.on_event("startup")
async def start_worker():
    app.state.plumber_worker = create_worker()
    # Worker runs as a background asyncio task
```

The worker can also run as a standalone process with no HTTP server at all:

```bash
arq llming_plumber.worker.WorkerSettings
```

---

## MongoDB Collections

MongoDB is the persistent source of truth. Redis is ephemeral dispatch.

| Collection | Purpose |
|---|---|
| `plumber.workflows` | Workflow definitions (name, nodes, edges, config, version) |
| `plumber.jobs` | Job instances — one document per execution of a workflow |
| `plumber.job_logs` | Append-only log entries for each job step |
| `plumber.schedules` | Cron/interval definitions tied to workflows |
| `plumber.credentials` | Encrypted credential store for connected services |

Note: the `plumber.locks` collection from the previous design is **gone** —
ARQ handles distributed locking via Redis.

### Key Indexes

```javascript
// jobs — query by status, workflow, date
db.plumber.jobs.createIndex({ status: 1, created_at: -1 })
db.plumber.jobs.createIndex({ workflow_id: 1, created_at: -1 })
db.plumber.jobs.createIndex({ worker_id: 1, status: 1 })

// job_logs — query logs for a specific job, ordered
db.plumber.job_logs.createIndex({ job_id: 1, ts: 1 })

// schedules — find enabled schedules
db.plumber.schedules.createIndex({ enabled: 1 })
```

---

## Job Lifecycle

```
                  schedule/
  ┌──────────┐    API trigger   ┌──────────┐   ARQ dispatches  ┌───────────┐
  │          │    + enqueue     │          │   to a worker      │           │
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

### Job Document

```javascript
{
  _id: ObjectId,
  workflow_id: ObjectId,
  workflow_version: 3,
  status: "queued" | "running" | "completed" | "failed" | "retrying" | "cancelled",

  // Scheduling
  created_at: ISODate,
  started_at: ISODate | null,
  finished_at: ISODate | null,

  // Worker assignment (set when ARQ worker picks up the job)
  worker_id: "prod-worker-02:48291:a3f1c9e2" | null,

  // ARQ reference
  arq_job_id: "arq:job:abc123" | null,

  // Execution
  current_node: "node-id" | null,
  node_states: { "node-id": { status, output, error, duration_ms } },
  input: { ... },               // trigger payload
  output: { ... } | null,       // final result

  // Retry
  attempt: 1,
  max_attempts: 3,

  // Audit
  error: "..." | null,
  tags: ["nightly", "import"],
}
```

---

## ARQ Worker Configuration

```python
import socket, os
from uuid import uuid4
from arq import cron
from arq.connections import RedisSettings

from llming_plumber.config import settings
from llming_plumber.worker.executor import execute_workflow
from llming_plumber.worker.scheduler import check_schedules


WORKER_ID = f"{socket.gethostname()}:{os.getpid()}:{uuid4().hex[:8]}"


class WorkerSettings:
    """ARQ worker configuration — run with `arq llming_plumber.worker.WorkerSettings`."""

    redis_settings = RedisSettings.from_dsn(settings.redis_url)

    # The task functions ARQ can dispatch to
    functions = [execute_workflow]

    # Built-in cron: check MongoDB schedules every minute, enqueue due workflows
    cron_jobs = [cron(check_schedules, minute=None)]  # every minute

    # Concurrency: how many jobs this worker runs simultaneously
    max_jobs = settings.worker_concurrency  # default: 4

    # Health check: if a worker doesn't report in this interval, ARQ marks it dead
    health_check_interval = 30

    # Job timeout: kill jobs that run longer than this
    job_timeout = 3600  # 1 hour default

    # Pass worker_id to all tasks via ctx
    on_startup = startup
    on_shutdown = shutdown


async def startup(ctx):
    """Called once when the worker starts."""
    ctx["worker_id"] = WORKER_ID
    ctx["db"] = await connect_to_mongodb()
    # Register worker heartbeat in MongoDB
    await ctx["db"].plumber.workers.update_one(
        {"worker_id": WORKER_ID},
        {"$set": {"started_at": datetime.utcnow(), "status": "online"}},
        upsert=True,
    )


async def shutdown(ctx):
    """Called when the worker shuts down."""
    await ctx["db"].plumber.workers.update_one(
        {"worker_id": WORKER_ID},
        {"$set": {"status": "offline", "stopped_at": datetime.utcnow()}},
    )
```

### Task Function

```python
async def execute_workflow(ctx: dict, *, job_id: str) -> dict:
    """Execute a workflow job. Called by ARQ when a job is dequeued."""
    db = ctx["db"]
    worker_id = ctx["worker_id"]

    # Mark running in MongoDB
    job = await db.plumber.jobs.find_one_and_update(
        {"_id": ObjectId(job_id), "status": "queued"},
        {"$set": {
            "status": "running",
            "worker_id": worker_id,
            "started_at": datetime.utcnow(),
        }},
        return_document=ReturnDocument.AFTER,
    )
    if not job:
        return {"skipped": True}  # already cancelled or picked up

    workflow = await db.plumber.workflows.find_one({"_id": job["workflow_id"]})

    try:
        result = await run_nodes(workflow, job, db, worker_id)
        await db.plumber.jobs.update_one(
            {"_id": ObjectId(job_id)},
            {"$set": {
                "status": "completed",
                "output": result,
                "finished_at": datetime.utcnow(),
            }},
        )
        return result

    except Exception as e:
        await db.plumber.jobs.update_one(
            {"_id": ObjectId(job_id)},
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
@router.post("/workflows/{workflow_id}/run")
async def run_workflow(workflow_id: str, request: Request):
    job_doc = {
        "workflow_id": ObjectId(workflow_id),
        "status": "queued",
        "created_at": datetime.utcnow(),
        "attempt": 0,
        "input": await request.json(),
    }
    result = await db.plumber.jobs.insert_one(job_doc)
    job_id = str(result.inserted_id)

    # Enqueue into Redis — a worker picks this up instantly
    await request.app.state.arq_pool.enqueue_job(
        "execute_workflow", job_id=job_id,
    )
    return {"job_id": job_id, "status": "queued"}
```

---

## Cron / Scheduled Workflows

Schedules are stored in MongoDB. ARQ's built-in cron runs a check every
minute that scans for due schedules and enqueues them:

```python
async def check_schedules(ctx: dict):
    """ARQ cron job — runs every minute. Finds due schedules, enqueues jobs."""
    db = ctx["db"]
    now = datetime.utcnow()
    pool = ctx["redis"]  # ARQ provides this

    async for schedule in db.plumber.schedules.find({
        "enabled": True,
        "next_run_at": {"$lte": now},
    }):
        # Create job doc
        job_doc = {
            "workflow_id": schedule["workflow_id"],
            "status": "queued",
            "created_at": now,
            "attempt": 0,
            "tags": schedule.get("tags", []),
        }
        result = await db.plumber.jobs.insert_one(job_doc)

        # Enqueue
        await pool.enqueue_job("execute_workflow", job_id=str(result.inserted_id))

        # Advance next_run_at
        next_run = compute_next_run(schedule["cron_expression"], now)
        await db.plumber.schedules.update_one(
            {"_id": schedule["_id"]},
            {"$set": {"next_run_at": next_run, "last_run_at": now}},
        )
```

---

## Worker Identity & Logging

Every worker process generates a unique identity at startup:

```python
worker_id = f"{hostname}:{pid}:{uuid4().hex[:8]}"
# e.g. "prod-worker-02:48291:a3f1c9e2"
```

This ID is:
- Stored in `worker_id` on every claimed job document.
- Included in every log entry the worker emits.
- Written to every `plumber.job_logs` document.
- Registered in `plumber.workers` collection with heartbeat timestamps.

### Structured Log Format

All log output uses structured JSON logging:

```json
{
  "ts": "2026-03-06T14:22:03.112Z",
  "level": "info",
  "worker_id": "prod-worker-02:48291:a3f1c9e2",
  "job_id": "660a1f...",
  "workflow": "daily-news-import",
  "node": "fetch-rss",
  "msg": "Node completed",
  "duration_ms": 342
}
```

### Job Log Collection

In addition to stdout/stderr, every node execution writes to `plumber.job_logs`:

```javascript
{
  _id: ObjectId,
  job_id: ObjectId,
  worker_id: "prod-worker-02:48291:a3f1c9e2",
  node_id: "fetch-rss",
  node_type: "rss_reader",
  ts: ISODate,
  level: "info" | "warning" | "error",
  msg: "Fetched 42 items from https://...",
  duration_ms: 342,
  output_summary: { item_count: 42 },
}
```

This makes it trivial to answer:
- "Which worker ran job X?" → `db.plumber.jobs.findOne({_id: X}).worker_id`
- "What did job X do?" → `db.plumber.job_logs.find({job_id: X}).sort({ts: 1})`
- "What is worker Y doing?" → `db.plumber.jobs.find({worker_id: Y, status: "running"})`
- "Is worker Y alive?" → `db.plumber.workers.findOne({worker_id: Y})`

---

## Real-Time UI Updates (via Redis Pub/Sub)

When a worker updates a job's status, it also publishes to a Redis channel.
The UI server subscribes and pushes updates to the browser via WebSocket:

```python
# Worker side — after status change
await redis.publish("plumber:job_updates", json.dumps({
    "job_id": job_id,
    "status": "completed",
    "worker_id": worker_id,
}))

# UI side — WebSocket endpoint
@router.websocket("/ws/jobs")
async def job_updates_ws(websocket: WebSocket):
    await websocket.accept()
    pubsub = redis.pubsub()
    await pubsub.subscribe("plumber:job_updates")
    async for message in pubsub.listen():
        if message["type"] == "message":
            await websocket.send_text(message["data"])
```

No polling needed — the UI updates the moment a job changes state.

---

## API Surface (UI Server)

The UI server exposes a REST API. All endpoints are grouped under a
single `APIRouter` so they can be mounted at any prefix.

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/workflows` | List workflows |
| `POST` | `/api/workflows` | Create workflow |
| `GET` | `/api/workflows/{id}` | Get workflow detail |
| `PUT` | `/api/workflows/{id}` | Update workflow |
| `DELETE` | `/api/workflows/{id}` | Delete workflow |
| `POST` | `/api/workflows/{id}/run` | Trigger a job (writes to MongoDB + enqueues via ARQ) |
| `GET` | `/api/jobs` | List jobs (filterable by status, workflow, date) |
| `GET` | `/api/jobs/{id}` | Get job detail + node states |
| `GET` | `/api/jobs/{id}/logs` | Get job execution logs |
| `POST` | `/api/jobs/{id}/cancel` | Cancel a pending/running job |
| `POST` | `/api/jobs/{id}/retry` | Retry a failed job |
| `GET` | `/api/schedules` | List schedules |
| `POST` | `/api/schedules` | Create schedule |
| `PUT` | `/api/schedules/{id}` | Update schedule |
| `DELETE` | `/api/schedules/{id}` | Delete schedule |
| `GET` | `/api/workers` | List active workers (from `plumber.workers` collection) |
| `GET` | `/api/health` | Health check (MongoDB + Redis connectivity) |
| `WS` | `/ws/jobs` | Real-time job status updates via WebSocket |

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

# Worker
PLUMBER_WORKER_CONCURRENCY=4    # concurrent job slots per worker process
PLUMBER_JOB_TIMEOUT=3600        # max seconds per job
PLUMBER_HEALTH_CHECK_INTERVAL=30

# API
PLUMBER_API_PREFIX=/api         # prefix for all API routes
PLUMBER_HOST=0.0.0.0
PLUMBER_PORT=8000

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
│   ├── workflow.py          # Workflow, Node, Edge
│   ├── job.py               # Job, NodeState
│   ├── schedule.py          # Schedule
│   └── log.py               # JobLog
├── api/
│   ├── __init__.py          # router aggregation
│   ├── workflows.py
│   ├── jobs.py
│   ├── schedules.py
│   ├── workers.py
│   └── ws.py                # WebSocket endpoint for real-time updates
├── worker/
│   ├── __init__.py          # WorkerSettings for ARQ
│   ├── executor.py          # execute_workflow task + node runner
│   └── scheduler.py         # check_schedules cron task
├── nodes/                   # Building-block implementations
│   ├── base.py              # BaseNode interface
│   ├── http_request.py
│   ├── rss_reader.py
│   ├── send_email.py
│   ├── weather.py
│   └── ...
├── db.py                    # MongoDB + Redis connection helpers
└── cli.py                   # `llming-plumber serve --mode=...`
```

---

## Summary

| Concern | Solution |
|---|---|
| Persistent storage | MongoDB — workflows, jobs, logs, credentials, schedules |
| Job dispatch | ARQ + Redis — push-based, instant delivery, no polling |
| Exactly-once execution | ARQ's atomic Redis operations — only one worker gets each job |
| Retries | ARQ built-in retry with configurable count and backoff |
| Cron scheduling | ARQ `cron_jobs` triggers `check_schedules` every minute |
| Embeddable | `create_app()` factory + mountable `APIRouter` |
| Deployment flexibility | Three modes: `all`, `ui`, `worker` — controlled by one env var |
| Horizontal scaling | N worker processes/servers, all consuming from the same Redis queue |
| Crash recovery | ARQ detects dead workers via `health_check_interval`, re-queues jobs |
| Real-time UI | Redis pub/sub → WebSocket, no polling |
| Audit trail | `worker_id` on every job + append-only `job_logs` collection |
| Structured logging | JSON logs with `worker_id`, `job_id`, `node_id` on every line |
