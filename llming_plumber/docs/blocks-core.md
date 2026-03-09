# Core Blocks

> Triggers, flow control, data transformation, and utility blocks that form the foundation of every pipeline.

---

## Triggers

### manual_trigger

Manually start a pipeline, optionally passing test data for development.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **test_data** | dict | `{}` | Optional key-value data injected as the block's output |

**Output:** `data` (dict)

Use this as the first block in any pipeline you want to trigger on demand from the UI or API. The `test_data` field is useful during development to simulate upstream input.

---

### timer_trigger

Trigger a pipeline on a recurring schedule using either a fixed interval or a cron expression.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **trigger_type** | select | `interval` | `interval` (every N seconds) or `cron` (cron expression) |
| **interval_seconds** | int | `3600` | Seconds between runs (only for `interval` type) |
| **cron_expression** | str | — | Standard 5-field cron expression, e.g. `0 8 * * 1-5` (only for `cron` type) |

**Output:** `triggered_at` (ISO timestamp)

When a pipeline with a `timer_trigger` is saved, the system automatically creates a schedule (tagged `_auto_timer`). The scheduler includes anti-stacking protection — if a run is already queued or running for the pipeline, the next scheduled trigger is skipped.

---

## Flow Control

### split

Fan-out block — takes a list and emits each item individually to downstream blocks.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **items** | list | `[]` | List of items to fan out |

**Output:** `items` (list) — each downstream block runs once per item.

**Execution:** Sets `fan_out_field = "items"`. The executor splits the output list into individual parcels and runs all downstream blocks once per item. Bounded by `MAX_FAN_OUT_ITEMS` (default 10,000).

---

### collect

Fan-in block — gathers all upstream fan-out results back into a single list.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **items** | list | `[]` | Automatically populated by the executor |

**Output:** `items` (list of all gathered results)

Place after a fan-out chain to consolidate results. For example:
```
[Split] → [Process Each] → [Collect] → [Excel Builder]
```

---

### range

Generate a numbered sequence for iteration, similar to Python's `range()`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **start** | int | `0` | Start value (inclusive) |
| **end** | int | `10` | End value (exclusive) |
| **step** | int | `1` | Step between values |

**Output:** `items` (list of `{index: N}` dicts)

**Execution:** Fans out via `fan_out_field = "items"` — downstream blocks run once per index.

Example pipeline:
```
[Range(0..5)] → [Log("Processing #{index}")] → [Collect] → [Excel Builder]
```

---

### merge

Combine multiple item lists into one.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **list1** | list | `[]` | First list |
| **list2** | list | `[]` | Second list |
| **list3** | list | `[]` | Third list (optional) |

**Output:** `items` (combined list), `total_count` (int)

Useful for joining results from parallel branches before further processing.

---

### filter

Filter a list of items by a field condition.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **items** | list | `[]` | Items to filter |
| **field** | str | — | Field name to check |
| **operator** | select | `eq` | One of: `eq`, `ne`, `gt`, `lt`, `gte`, `lte`, `contains`, `startswith`, `endswith`, `regex`, `exists`, `not_exists` |
| **value** | str | — | Value to compare against |
| **case_sensitive** | bool | `true` | Whether string comparisons are case-sensitive |

**Output:** `items` (filtered list), `filtered_count` (int)

---

### sort

Sort items by a field.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **items** | list | `[]` | Items to sort |
| **field** | str | — | Field name to sort by |
| **descending** | bool | `false` | Sort in descending order |

**Output:** `items` (sorted list)

---

### deduplicator

Remove duplicate items based on a field value.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **items** | list | `[]` | Items to deduplicate |
| **field** | str | — | Field to check for uniqueness |
| **keep_first** | bool | `true` | Keep the first occurrence (or last if false) |

**Output:** `items` (deduplicated list), `removed_count` (int)

---

### wait

Pause execution for a specified duration. Useful for rate limiting or staged execution.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **duration_seconds** | float | `1.0` | How long to wait (capped at `MAX_WAIT_SECONDS`, default 300) |

**Output:** `waited` (bool), `waited_seconds` (float)

---

### log

Write a message to the run console. Supports `{expression}` interpolation with piped variables.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **message** | str | — | Message to log (supports `{field}` interpolation) |

**Output:** `logged` (bool)

The message appears in the run console and as a live progress indicator on the block node in the UI.

---

## Data Transformation

### text_template

Render text templates with `{expression}` placeholders using the safe expression evaluator.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **template** | str (textarea) | — | Template text with `{expression}` placeholders |
| **context** | dict | `{}` | Additional variables for the template |
| **strict** | bool | `false` | Raise error on missing variables (otherwise leaves placeholder) |

**Output:** `output` (str)

Expressions support whitelisted functions: `str`, `int`, `float`, `bool`, `len`, `abs`, `min`, `max`, `round`, `chr`, `join`, `pluck`. Use `chr(10)` for newlines inside expressions.

---

### json_transformer

Rename, select, or remove fields from a dict.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **data** | str/dict | — | Input data (JSON string or dict) |
| **operation** | select | `rename` | `rename`, `select`, or `remove` |
| **fields** | dict | `{}` | For rename: `{old: new}`. For select: `{field: true}` |
| **remove_fields** | list | `[]` | Fields to remove (only for `remove` operation) |

**Output:** `result` (dict), `result_json` (str)

---

### jsonpath

Extract values from data using JSONPath expressions.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **data** | str/dict | — | Input data (JSON string or dict) |
| **path** | str | — | JSONPath expression, e.g. `$.items[*].name` |

**Output:** `values` (list), `count` (int)

---

### regex_extractor

Extract patterns from text using regular expressions.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **text** | str | — | Input text |
| **pattern** | str | — | Regex pattern (with optional capture groups) |
| **flags** | str | — | Regex flags, e.g. `i` for case-insensitive |

**Output:** `matches` (list), `groups` (dict)

---

### aggregate

Compute statistics across a list of items.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **items** | list | `[]` | Items to aggregate |
| **field** | str | — | Numeric field to aggregate |
| **operation** | select | `sum` | One of: `sum`, `avg`, `min`, `max`, `count` |

**Output:** `result` (float)

---

### column_mapper

Rename and remap columns/fields across a list of items.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **items** | list | `[]` | Items to remap |
| **mapping** | dict | `{}` | `{new_name: old_name}` mapping |
| **drop_unmapped** | bool | `false` | Drop fields not in the mapping |

**Output:** `items` (remapped list)

---

### csv_parser

Parse CSV text into a list of row dicts.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **content** | str | — | Raw CSV text |
| **has_header** | bool | `true` | First row contains column names |
| **delimiter** | str | `,` | Field delimiter |
| **quote_char** | str | `"` | Quote character |

**Output:** `rows` (list of dicts)

---

### xml_parser

Extract elements from XML by tag name.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **content** | str | — | XML text |
| **tag** | str | — | Tag name to extract |
| **attribute** | str | — | Optional attribute to extract from each element |

**Output:** `elements` (list), `count` (int)

---

### html_extractor

Extract content from HTML using CSS selectors.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **html** | str | — | HTML content |
| **selector** | str | — | CSS selector, e.g. `div.price`, `h1`, `a[href]` |
| **extract_all** | bool | `true` | Extract all matches (or just the first) |
| **attribute** | str | — | Extract a specific attribute instead of text content |

**Output:** `results` (list), `count` (int)

---

### split_text

Split text into smaller chunks for processing (e.g., before sending to an LLM).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **text** | str | — | Text to split |
| **chunk_size** | int | `1000` | Maximum size per chunk |
| **overlap** | int | `0` | Overlap between chunks |
| **split_by** | select | `chars` | Split by `chars`, `lines`, or `paragraphs` |

**Output:** `chunks` (list), `chunk_count` (int)

---

### datetime_formatter

Parse and reformat datetime strings.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **timestamp** | str | — | Input datetime string |
| **input_format** | str | — | Parse format (Python strftime), e.g. `%Y-%m-%dT%H:%M:%S` |
| **output_format** | str | — | Output format, e.g. `%d.%m.%Y` |

**Output:** `formatted` (str)

---

### hash_generator

Compute a cryptographic hash of text content.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **content** | str | — | Text to hash |
| **algorithm** | select | `sha256` | `sha256`, `sha1`, or `md5` |

**Output:** `hash` (str)

---

### base64_codec

Encode or decode Base64 content.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **content** | str | — | Content to encode/decode |
| **mode** | select | `encode` | `encode` or `decode` |

**Output:** `output` (str)

---

### static_data

Embed static data directly in a pipeline — useful for constants, lookup tables, or test fixtures.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **data_format** | select | `text` | `text`, `json`, or `base64` |
| **content** | str | — | The static content |

**Output:** `data` (str, dict, or bytes depending on format)

---

### http_request

Make HTTP requests to external APIs.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **url** | str | — | Request URL |
| **method** | select | `GET` | HTTP method: `GET`, `POST`, `PUT`, `DELETE`, `PATCH` |
| **headers** | dict | `{}` | Request headers |
| **body** | str | — | Request body (for POST/PUT/PATCH) |
| **timeout** | int | `30` | Timeout in seconds |

**Output:** `status_code` (int), `body` (str), `headers` (dict), `elapsed_ms` (int)

---

## Pipeline-Scoped Cache

### store_cache

Store a value in Redis cache, scoped to the current pipeline.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **key** | str | — | Cache key |
| **value** | str | — | Value to store |
| **ttl_seconds** | int | `3600` | Time-to-live in seconds |

**Output:** `key` (str), `stored` (bool)

---

### read_cache

Read a value from the pipeline-scoped Redis cache.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **key** | str | — | Cache key |

**Output:** `key` (str), `value` (str), `exists` (bool)

---

### delete_cache

Delete a value from the pipeline-scoped Redis cache.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **key** | str | — | Cache key to delete |

**Output:** `key` (str), `deleted` (bool)

---

## Variables

### set_variables

Execute a variable operations script to set values at different scopes (global, pipeline, job).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **script** | str (textarea) | — | Variable operations, one per line. Prefix with scope: `gl.name = value` (global), `pl.name = value` (pipeline), `job.name = value` (job) |

**Output:** `gl_vars` (dict), `pl_vars` (dict), `job_vars` (dict)

Variables set here are available in downstream blocks via `{expression}` interpolation. Global variables persist across runs, pipeline variables persist within a pipeline, and job variables are scoped to the current run.

---

### variable_store

Low-level variable read/write operations.

See [Data Piping](data-piping.md) for the full variable scoping model.

---

## Markdown Rendering

### markdown_renderer

Render Markdown text to HTML.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **markdown** | str (textarea) | — | Markdown source text |
| **extensions** | list | `[]` | Markdown extensions to enable |
| **sanitize** | bool | `true` | Sanitize output HTML |

**Output:** `html` (str), `word_count` (int)
