# Coding Principles

## Type Safety & Testing

- **>99% mypy coverage** — strict mode, no `Any` escape hatches, no `type: ignore` without a comment explaining why.
- **>99% test coverage** — every node, every model, every API route. Use `pytest-cov` with `--cov-fail-under=99`.
- Tests must run fast. Mock external services, never hit real APIs in CI.

## Pydantic Everywhere

All data structures are Pydantic models. No raw dicts crossing function boundaries.

- `WorkflowDefinition`, `NodeDefinition`, `EdgeDefinition` — workflow structure
- `Job`, `JobLog`, `NodeState` — execution state
- `Schedule`, `Credential` — configuration
- `NodeInput`, `NodeOutput` — socket data flowing between nodes

## Node (Plugin) Architecture

Every node is **self-contained** in a single module under `llming_plumber/nodes/`. A node consists of:

```python
from llming_plumber.nodes.base import BaseNode, NodeInput, NodeOutput

class RssReaderInput(NodeInput):
    """Pydantic model defining what this node accepts."""
    feed_url: str
    max_items: int = 50

class RssReaderOutput(NodeOutput):
    """Pydantic model defining what this node produces."""
    items: list[FeedItem]

class RssReaderNode(BaseNode[RssReaderInput, RssReaderOutput]):
    node_type = "rss_reader"
    input_model = RssReaderInput
    output_model = RssReaderOutput

    async def execute(self, input: RssReaderInput, ctx: NodeContext) -> RssReaderOutput:
        ...
```

### Rules

- **One file per node** — `llming_plumber/nodes/rss_reader.py`, `llming_plumber/nodes/weather.py`, etc.
- **No cross-node imports** — nodes never import from each other.
- **No global state** — all dependencies come through `NodeContext`.
- **Input/Output models are the contract** — the executor validates data against these models when piping between nodes.

### Standalone Usability

Every node must work **independently** of the Plumber engine. A developer should be able to `pip install llming-plumber` and use a single node without running MongoDB, Redis, or any server:

```python
# Using the weather node as a standalone library — no server, no worker, no DB
from llming_plumber.nodes.weather import WeatherNode, WeatherInput

node = WeatherNode()
result = await node.execute(
    WeatherInput(city="Berlin,DE", api_key="..."),
    ctx=None,  # no NodeContext needed for standalone use
)
print(f"{result.temp}°C, {result.condition}")
# → "8°C, light rain"
```

This makes every node reusable as a plain Python library — embed the weather node in a website widget, use the RSS reader in a CLI script, or call the email sender from a Jupyter notebook.

The node must **never assume it's running inside a workflow**. `NodeContext` is optional and only provides workflow-level features (logging, credential lookup) when present. This means:

- No imports from `llming_plumber.worker`, `llming_plumber.db`, or `llming_plumber.api` inside node code.
- Configuration (API keys, URLs) comes via the input model, not from environment variables or global config. When running inside Plumber, the executor resolves credentials and injects them into the input.
- Nodes only depend on `llming_plumber.nodes.base` and their own third-party libraries (e.g. `feedparser` for RSS, `httpx` for HTTP).

## Sockets & Piping

Nodes connect through **typed sockets**. An edge in a workflow definition maps an output field of one node to an input field of the next:

```python
class EdgeDefinition(BaseModel):
    source_node: str          # node id
    source_output: str        # field name on the source node's output model
    target_node: str          # node id
    target_input: str         # field name on the target node's input model
```

The executor validates **type compatibility at workflow save time** (not just at runtime). If `RssReaderOutput.items` is `list[FeedItem]` and the target expects `str`, that's a validation error before the workflow ever runs.

Socket types are derived from the Pydantic model field annotations. The workflow editor can use this metadata to show compatible connections and prevent invalid wiring in the UI.

## Testing Nodes

Every node has a matching test file: `tests/nodes/test_rss_reader.py`.

Nodes are trivially testable in isolation because they are pure `async` functions with Pydantic in/out:

```python
async def test_rss_reader():
    node = RssReaderNode()
    result = await node.execute(
        RssReaderInput(feed_url="https://example.com/feed.xml"),
        ctx=mock_node_context(),
    )
    assert isinstance(result, RssReaderOutput)
    assert len(result.items) > 0
```

All node tests can run in parallel (`pytest -n auto`) because nodes are isolated and share no state. This is a direct consequence of the standalone design — if a node works without the engine, it works without other nodes too.

## Backward Compatibility

Every change must be **safe to deploy without breaking existing workflows**.

- **Versioned workflow definitions** — workflows store the version of their schema. Older versions are migrated forward on load, never silently reinterpreted.
- **Additive-only node changes** — new input fields must have defaults. Removing or renaming a field requires a migration and a version bump.
- **Socket contracts are stable** — once a node's output model is published, its existing fields keep their name and type. New fields can be added, existing ones never change.
- **Deprecate, don't delete** — mark nodes as deprecated in the registry. They keep running in existing workflows but are hidden from the "add node" UI. Remove only after a full migration cycle.
- **Schema migrations are explicit** — when a model changes, write a migration function that transforms old documents. Never rely on Pydantic defaults silently papering over missing fields in production data.

## Extensibility & Node Discovery

It must be trivially easy to add new nodes — from internal code or from external packages.

- **Auto-discovery** — Plumber scans `llming_plumber/nodes/` for all `BaseNode` subclasses at startup. Drop a file in, it's available.
- **Entry point plugins** — external packages can register nodes via Python entry points (`[project.entry-points."llming_plumber.nodes"]`). This allows third-party libraries to ship Plumber-compatible nodes without forking the repo:

```toml
# In an external package's pyproject.toml
[project.entry-points."llming_plumber.nodes"]
my_custom_node = "my_package.nodes:MyCustomNode"
```

- **Node registry** — all discovered nodes (built-in + entry points) are collected in a `NodeRegistry` at startup. The registry exposes each node's `node_type`, input/output schemas, and metadata. Both the API and the UI read from this registry.
- **`GET /api/nodes`** — returns all registered nodes with their input/output JSON schemas, descriptions, and categories. This is the single source of truth for what's available — the no-code editor and API consumers use the same endpoint.

## No-Code / Visual Editor Parity

The visual workflow editor is a **first-class citizen**, not an afterthought. Every workflow operation possible via the API must be equally possible in the no-code UI, and vice versa.

### Visual Metadata

Workflow definitions store all information needed to reconstruct the visual layout. This metadata is **not optional** — it is part of the workflow model and persisted in MongoDB:

```python
class NodePosition(BaseModel):
    x: float
    y: float

class NodeDefinition(BaseModel):
    id: str
    node_type: str                    # e.g. "rss_reader"
    label: str                        # human-readable name on the canvas
    config: dict[str, Any]            # node-specific configuration
    position: NodePosition            # grid position in the visual editor
    notes: str = ""                   # optional user annotation

class WorkflowDefinition(BaseModel):
    name: str
    description: str = ""
    nodes: list[NodeDefinition]
    edges: list[EdgeDefinition]
    canvas_zoom: float = 1.0          # viewport zoom level
    canvas_offset: NodePosition = NodePosition(x=0, y=0)  # viewport pan
    version: int
```

### API ↔ Visual Equivalence

- **Creating a workflow via API** requires `position` on every node. The API does not auto-layout — the caller is responsible for providing coordinates (this keeps API-created workflows usable in the editor without surprises).
- **Creating a workflow in the editor** produces the exact same `WorkflowDefinition` document. There is no separate "visual" schema.
- **Importing/exporting** workflows preserves all visual metadata. A workflow exported from one instance and imported into another looks identical on the canvas.

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

# Worker only (via ARQ)
arq llming_plumber.worker.WorkerSettings

# Tests
pytest --cov=llming_plumber --cov-fail-under=99 -n auto

# Type checking
mypy llming_plumber/ --strict

# Lint
ruff check llming_plumber/ tests/
```
