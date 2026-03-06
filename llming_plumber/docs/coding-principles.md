# Coding Principles

## Type Safety & Testing

- **>99% mypy coverage** — strict mode, no `Any` escape hatches, no `type: ignore` without a comment explaining why.
- **>99% test coverage** — every block, every model, every API route. Use `pytest-cov` with `--cov-fail-under=99`.
- Unit tests must run fast. Mock external services, never hit real APIs in CI.

### Two Test Layers (Always Required)

Every feature that touches external services **must** have both:

1. **Unit tests** — mocked, fast, run in CI. Verify logic, error handling, data mapping.
2. **Integration tests** — marked `@pytest.mark.integration`, call real APIs with keys from `.env`. Verify actual end-to-end behavior.

```bash
# Unit tests only (CI-safe, no API keys needed)
pytest -m "not integration" --cov=llming_plumber --cov-fail-under=99

# Integration tests (requires .env with valid API keys)
pytest -m integration -v
```

Integration tests are **not optional** — they catch real-world issues that mocks cannot: auth failures, API changes, response format drift, rate limits. When adding a new LLM provider or external-API block, always add integration tests that exercise the full request/response cycle.

## Async Everywhere

All I/O is async. No exceptions.

- **Block handlers are `async def`** — every `execute()` method is a coroutine. There is no sync block interface.
- **All external I/O uses async clients** — `httpx.AsyncClient` for HTTP, `motor` for MongoDB, `redis.asyncio` for Redis, `aiofiles` for disk. Never use `requests`, `pymongo`, or synchronous `redis-py`.
- **No `sync_to_async` wrappers** — if a third-party library is sync-only, wrap it in `asyncio.to_thread()` explicitly. Don't hide blocking calls behind adapters.
- **Lemmings run in parallel processes**, each with its own async event loop. ARQ manages the loop — blocks run as coroutines within that loop. This means a single lemming can handle multiple concurrent runs (controlled by `max_jobs`), and I/O-bound blocks (HTTP calls, DB queries) naturally yield while waiting.

```python
# Correct — async all the way
async def execute(self, input: WeatherInput, ctx: BlockContext | None) -> WeatherOutput:
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"{self.api_base}/weather", params={"q": input.city})
        data = resp.json()
    return WeatherOutput(temp=data["main"]["temp"], condition=data["weather"][0]["main"])

# Wrong — blocks sync HTTP in an async handler
def execute(self, input: WeatherInput, ctx: BlockContext | None) -> WeatherOutput:
    resp = requests.get(...)  # blocks the entire event loop
```

## Pydantic Everywhere

All data structures are Pydantic models. No raw dicts crossing function boundaries.

- `PipelineDefinition`, `BlockDefinition`, `PipeDefinition` — pipeline structure
- `Run`, `RunLog`, `BlockState` — execution state
- `Schedule`, `Credential` — configuration
- `BlockInput`, `BlockOutput` — fitting data flowing between blocks

## Block (Plugin) Architecture

Every block is **self-contained** in a single module under `llming_plumber/blocks/`. A block consists of:

```python
from llming_plumber.blocks.base import BaseBlock, BlockInput, BlockOutput

class RssReaderInput(BlockInput):
    """Pydantic model defining what this block accepts."""
    feed_url: str
    max_items: int = 50

class RssReaderOutput(BlockOutput):
    """Pydantic model defining what this block produces."""
    items: list[FeedItem]

class RssReaderBlock(BaseBlock[RssReaderInput, RssReaderOutput]):
    block_type = "rss_reader"
    input_model = RssReaderInput
    output_model = RssReaderOutput

    async def execute(self, input: RssReaderInput, ctx: BlockContext) -> RssReaderOutput:
        ...
```

### Rules

- **One file per block, organized by category** — `llming_plumber/blocks/weather/openweathermap.py`, `llming_plumber/blocks/news/rss_reader.py`, etc.
- **No cross-block imports** — blocks never import from each other.
- **No global state** — all dependencies come through `BlockContext`.
- **Input/Output models are the contract** — the executor validates data against these models when piping between blocks.

### Standalone Usability

Every block must work **independently** of the Plumber engine. A developer should be able to `pip install llming-plumber` and use a single block without running MongoDB, Redis, or any server:

```python
# Using the weather block as a standalone library — no server, no lemming, no DB
from llming_plumber.blocks.weather import WeatherBlock, WeatherInput

block = WeatherBlock()
result = await block.execute(
    WeatherInput(city="Berlin,DE", api_key="..."),
    ctx=None,  # no BlockContext needed for standalone use
)
print(f"{result.temp}°C, {result.condition}")
# → "8°C, light rain"
```

This makes every block reusable as a plain Python library — embed the weather block in a website widget, use the RSS reader in a CLI script, or call the email sender from a Jupyter notebook.

The block must **never assume it's running inside a pipeline**. `BlockContext` is optional and only provides pipeline-level features (logging, credential lookup) when present. This means:

- No imports from `llming_plumber.worker`, `llming_plumber.db`, or `llming_plumber.api` inside block code.
- Configuration (API keys, URLs) comes via the input model, not from environment variables or global config. When running inside Plumber, the executor resolves credentials and injects them into the input.
- Blocks only depend on `llming_plumber.blocks.base` and their own third-party libraries (e.g. `feedparser` for RSS, `httpx` for HTTP).

## Fittings & Piping

Data flows as **Parcels** — each parcel carries structured `fields` (JSON)
and optional `attachments` (binary with MIME metadata). See
[Data Piping](data-piping.md) for the full specification.

The key points:

- **Every entity has a UID** — blocks, fittings, pipes, attachments. No implicit identity.
- **Parcels, not values** — blocks receive and produce `list[Parcel]`. A parcel has `fields` (dict) and `attachments` (list of base64-encoded binaries with MIME types).
- **Fittings declare MIME types** — an OCR block accepts `application/pdf` and `image/*`, a text block produces no binary. Incompatible types are rejected at save time.
- **Fittings declare field schemas** — JSON Schema on each fitting enables validation and editor auto-complete.
- **Pipes support field mapping and attachment filtering** — rename fields, select subsets, filter by MIME type per pipe.
- **Large binaries offload to GridFS/S3** — attachments over 1 MB are stored by reference, resolved transparently via `ctx.resolve_attachment()`.

## Testing Blocks

Every block has a matching test file: `tests/blocks/news/test_rss_reader.py`.

Blocks are trivially testable in isolation because they are pure `async` functions with Pydantic in/out:

```python
# Unit test — mocked HTTP, runs in CI
async def test_rss_reader():
    block = RssReaderBlock()
    result = await block.execute(
        RssReaderInput(feed_url="https://example.com/feed.xml"),
        ctx=mock_block_context(),
    )
    assert isinstance(result, RssReaderOutput)
    assert len(result.items) > 0
```

All block tests can run in parallel (`pytest -n auto`) because blocks are isolated and share no state. This is a direct consequence of the standalone design — if a block works without the engine, it works without other blocks too.

## Testing LLM Providers

Every LLM provider has two test files:

- `tests/llm/test_providers.py` — **unit tests** (mocked env, no API calls). Verify registration, model catalogues, client creation, error handling.
- `tests/llm/test_providers_integration.py` — **integration tests** (real API calls). Every provider must have tests for all four call modes: `invoke`, `ainvoke`, `stream`, `astream`.

```python
# Integration test — calls real API, marked so CI skips it
@pytest.mark.integration
class TestAnthropicIntegration:
    def test_invoke(self) -> None:
        client = _create_client("anthropic")
        result = client.invoke(_messages())
        assert isinstance(result, LlmAIMessage)
        assert len(result.content) > 0
```

When adding a new provider, **always** add both unit and integration tests. The integration tests are the ground truth — if the integration test fails, the provider is broken regardless of what unit tests say.

## Backward Compatibility

Every change must be **safe to deploy without breaking existing pipelines**.

- **Versioned pipeline definitions** — pipelines store the version of their schema. Older versions are migrated forward on load, never silently reinterpreted.
- **Additive-only block changes** — new input fields must have defaults. Removing or renaming a field requires a migration and a version bump.
- **Fitting contracts are stable** — once a block's output model is published, its existing fields keep their name and type. New fields can be added, existing ones never change.
- **Deprecate, don't delete** — mark blocks as deprecated in the catalog. They keep running in existing pipelines but are hidden from the "add block" UI. Remove only after a full migration cycle.
- **Schema migrations are explicit** — when a model changes, write a migration function that transforms old documents. Never rely on Pydantic defaults silently papering over missing fields in production data.

## Extensibility & Block Discovery

It must be trivially easy to add new blocks — from internal code or from external packages.

- **Auto-discovery** — Plumber scans `llming_plumber/blocks/` recursively for all `BaseBlock` subclasses at startup. Drop a file in the right category folder, it's available.
- **Entry point plugins** — external packages can register blocks via Python entry points (`[project.entry-points."llming_plumber.blocks"]`). This allows third-party libraries to ship Plumber-compatible blocks without forking the repo:

```toml
# In an external package's pyproject.toml
[project.entry-points."llming_plumber.blocks"]
my_custom_block = "my_package.blocks:MyCustomBlock"
```

- **Catalog** — all discovered blocks (built-in + entry points) are collected in a `Catalog` at startup. The catalog exposes each block's `block_type`, input/output schemas, and metadata. Both the API and the UI read from this catalog.
- **`GET /api/blocks`** — returns all registered blocks with their input/output JSON schemas, descriptions, and categories. This is the single source of truth for what's available — the no-code editor and API consumers use the same endpoint.

## MCP Tool Generation

Blocks and pipelines can be exposed as [MCP (Model Context Protocol)](https://modelcontextprotocol.io/) tools with minimal effort. Because every block already has typed Pydantic input/output models, the mapping is nearly automatic.

### How It Works

Every `BaseBlock` subclass carries everything MCP needs:

- **`block_type`** → MCP tool name (e.g. `"plumber.weather"`)
- **`BlockInput.model_json_schema()`** → MCP `inputSchema`
- **`BlockOutput.model_json_schema()`** → response schema
- **`execute(input, ctx)`** → MCP tool handler
- **i18n labels** → MCP tool description (localized)

### Exposing Individual Blocks

```python
# Generate MCP tool definitions from the catalog
from llming_plumber.mcp import blocks_to_mcp_tools

tools = blocks_to_mcp_tools(
    block_types=["weather", "rss_reader", "tagesschau"],
    lang="en-us",
)
# → list of MCP Tool objects, ready to register with an MCP server
```

### Exposing Entire Pipelines

A pipeline becomes a single MCP tool. The pipeline's trigger block defines the input schema, and the final block's output defines the response:

```python
from llming_plumber.mcp import pipeline_to_mcp_tool

tool = pipeline_to_mcp_tool(
    pipeline_id="daily-news-digest",
    name="get_daily_news_digest",
    description="Fetch, summarize, and return today's news digest",
)
# → one MCP Tool that runs the full pipeline when called
```

### Running as an MCP Server

```bash
# Expose selected blocks as MCP tools
llming-plumber mcp serve --blocks weather,rss_reader,tagesschau

# Expose a pipeline as a single MCP tool
llming-plumber mcp serve --pipeline daily-news-digest
```

### Design Rules

- **Blocks don't know about MCP** — the MCP layer reads Pydantic schemas and wraps `execute()`. Blocks stay standalone.
- **No MCP imports inside `llming_plumber/blocks/`** — the MCP adapter lives in `llming_plumber/mcp/` and depends on blocks, not the other way around.
- **Pydantic is the bridge** — JSON Schema from Pydantic models is the single source of truth for both the REST API and MCP tool definitions. No manual schema duplication.

## No-Code / Visual Editor Parity

The visual pipeline editor is a **first-class citizen**, not an afterthought. Every pipeline operation possible via the API must be equally possible in the no-code UI, and vice versa.

### Visual Metadata

Pipeline definitions store all information needed to reconstruct the visual layout. This metadata is **not optional** — it is part of the pipeline model and persisted in MongoDB:

```python
class BlockPosition(BaseModel):
    x: float
    y: float

class BlockDefinition(BaseModel):
    id: str
    block_type: str                   # e.g. "rss_reader"
    label: str                        # human-readable name on the canvas
    config: dict[str, Any]            # block-specific configuration
    position: BlockPosition           # grid position in the visual editor
    notes: str = ""                   # optional user annotation

class PipelineDefinition(BaseModel):
    name: str
    description: str = ""
    blocks: list[BlockDefinition]
    pipes: list[PipeDefinition]
    canvas_zoom: float = 1.0          # viewport zoom level
    canvas_offset: BlockPosition = BlockPosition(x=0, y=0)  # viewport pan
    version: int
```

### API ↔ Visual Equivalence

- **Creating a pipeline via API** requires `position` on every block. The API does not auto-layout — the caller is responsible for providing coordinates (this keeps API-created pipelines usable in the editor without surprises).
- **Creating a pipeline in the editor** produces the exact same `PipelineDefinition` document. There is no separate "visual" schema.
- **Importing/exporting** pipelines preserves all visual metadata. A pipeline exported from one instance and imported into another looks identical on the canvas.

## Internationalization (i18n)

Same approach as [llming-lodge](https://github.com/user/llming-lodge): JSON-based
translations, fallback chains, zero external dependencies.

### How It Works

- Translation files are **flat JSON dictionaries** with dot-notation keys.
- A `t(key, lang, **kwargs)` function resolves translations with parameter substitution.
- Fallback chains ensure regional variants degrade gracefully (e.g. `de-swg` → `de-de` → `en-us`).
- Translation files are loaded once and cached via `@lru_cache`.

### Core Module

```
llming_plumber/
├── i18n/
│   ├── __init__.py              # t(), get_translations(), FALLBACK_CHAINS
│   └── translations/
│       ├── en-us.json           # base language (always complete)
│       ├── de-de.json
│       └── ...
```

```python
from llming_plumber.i18n import t

# Translate a block label
label = t("blocks.weather.label", "de-de")        # → "Wetter"
desc  = t("blocks.weather.description", "de-de")  # → "Aktuelle Wetterdaten abrufen"
```

### Plugin / Block Translations

Every block **ships its own translations** alongside its code. This keeps
blocks self-contained — a new plugin doesn't require touching the central
translation files.

```
llming_plumber/blocks/
├── weather/
│   ├── openweathermap.py
│   ├── openweathermap.i18n.json   # translations for this block
│   ├── dwd.py
│   └── dwd.i18n.json
├── news/
│   ├── rss_reader.py
│   ├── rss_reader.i18n.json
│   └── ...
└── ...
```

A block's `i18n.json` file follows the same flat-key format, namespaced
by block type:

```json
{
  "en-us": {
    "blocks.weather.label": "Weather",
    "blocks.weather.description": "Fetch current weather data and forecasts",
    "blocks.weather.input.city": "City",
    "blocks.weather.input.api_key": "API Key",
    "blocks.weather.output.temp": "Temperature",
    "blocks.weather.output.condition": "Condition"
  },
  "de-de": {
    "blocks.weather.label": "Wetter",
    "blocks.weather.description": "Aktuelle Wetterdaten und Vorhersagen abrufen",
    "blocks.weather.input.city": "Stadt",
    "blocks.weather.input.api_key": "API-Schlüssel",
    "blocks.weather.output.temp": "Temperatur",
    "blocks.weather.output.condition": "Wetterlage"
  }
}
```

At startup, the catalog **auto-discovers and merges** all
`*.i18n.json` files from `llming_plumber/blocks/` into the global
translation dictionary. External plugins (entry points) ship their own
`i18n.json` the same way.

### What Gets Translated

| Element | Key pattern | Example |
|---|---|---|
| Block label | `blocks.{type}.label` | `blocks.weather.label` |
| Block description | `blocks.{type}.description` | `blocks.weather.description` |
| Input field labels | `blocks.{type}.input.{field}` | `blocks.weather.input.city` |
| Output field labels | `blocks.{type}.output.{field}` | `blocks.weather.output.temp` |
| Category labels | `categories.{path}` | `categories.government.weather` |
| UI elements | `ui.{section}.{key}` | `ui.editor.save`, `ui.runs.status` |
| Error messages | `errors.{code}` | `errors.block_timeout` |

### Fallback Chains

```python
FALLBACK_CHAINS = {
    "de-de":  ["de-de", "en-us"],
    "de-swg": ["de-swg", "de-de", "en-us"],
    "fr-fr":  ["fr-fr", "en-us"],
    "es-es":  ["es-es", "en-us"],
    "zh-cn":  ["zh-cn", "en-us"],
    "en-us":  ["en-us"],
}
```

If a key is missing in `de-de`, it falls back to `en-us`. If a plugin
only ships `en-us` translations, it still works — German users see
English labels until someone contributes a `de-de` entry.

### Rules

- `en-us` is the **base language** and must always be complete.
- New blocks must include at least `en-us` translations.
- Translation keys use **dot notation**, namespaced by block type.
- No external i18n libraries — keep it simple, just JSON + `t()`.
- The `GET /api/blocks` endpoint returns translated labels based on the
  `Accept-Language` header or `?lang=` query parameter.

## Code Style

- Use `ruff` for formatting and linting (replaces black + isort + flake8).
- Keep functions short. If a function needs a docstring to explain what it does, consider renaming it or splitting it.
- No print statements — use `structlog` for all logging.

## Running

```bash
source .venv/bin/activate

# All-in-one
llming-plumber serve --mode=all

# UI only
llming-plumber serve --mode=ui

# Lemming only (via ARQ)
arq llming_plumber.worker.LemmingSettings

# Tests
pytest --cov=llming_plumber --cov-fail-under=99 -n auto

# Type checking
mypy llming_plumber/ --strict

# Lint
ruff check llming_plumber/ tests/
```
