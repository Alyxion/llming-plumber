# Data Piping & Fitting System

How data flows between blocks in a pipeline.

---

## Glossary

| Plumber Term | Industry Equivalent | Description |
|---|---|---|
| **Pipeline** | Workflow | A connected graph of blocks that processes data |
| **Block** | Node | A processing unit that receives, transforms, and outputs data |
| **Fitting** | Socket/Port | A typed connection point on a block (input or output) |
| **Pipe** | Edge/Wire | A connection carrying data from one fitting to another |
| **Parcel** | Item/Message | A unit of data flowing through the pipeline |
| **Lemming** | Worker | A process that picks up and executes pipeline runs |
| **Run** | Job/Execution | A single execution of a pipeline |
| **Catalog** | Registry | The index of all available block types |

---

## Design Principles

1. **Everything is JSON** — all data between blocks serializes to JSON. Binary content is base64-encoded and wrapped with metadata.
2. **MIME types are first-class** — every piece of content declares what it is. Blocks declare what they accept and produce.
3. **Every entity has a UID** — blocks, fittings, pipes, attachments. No implicit identity.
4. **Parcels, not single values** — data flows as a stream of parcels. Each parcel is a self-contained unit with structured fields and optional file attachments.
5. **Composability** — a web scraper that returns a page with 3 embedded PDFs and 12 images should pipe cleanly into an OCR block, a text extractor, or an archiver without any adapter glue.

---

## The Parcel

The fundamental unit of data flowing between blocks is a **Parcel**. Every
block receives a list of parcels and produces a list of parcels.

```python
class Attachment(BaseModel):
    """A binary payload attached to a parcel."""
    uid: str                          # unique ID for this attachment
    filename: str                     # e.g. "report.pdf"
    mime_type: str                    # e.g. "application/pdf"
    size_bytes: int                   # original size before encoding
    data_b64: str                     # base64-encoded content
    metadata: dict[str, Any] = {}     # extra info (page count, dimensions, encoding, ...)

class Parcel(BaseModel):
    """A single unit of data flowing through a pipeline."""
    uid: str                          # unique ID for this parcel
    fields: dict[str, Any]            # structured key-value data (the "JSON part")
    attachments: list[Attachment] = [] # binary payloads (PDFs, images, HTML, ...)
```

### JSON Serialization

A parcel on the wire:

```json
{
  "uid": "parcel_8f3a...",
  "fields": {
    "title": "Quarterly Report",
    "source_url": "https://example.com/report",
    "language": "de",
    "extracted_text": "..."
  },
  "attachments": [
    {
      "uid": "att_c91b...",
      "filename": "report.pdf",
      "mime_type": "application/pdf",
      "size_bytes": 284510,
      "data_b64": "JVBERi0xLjQKJ...",
      "metadata": {"page_count": 12}
    },
    {
      "uid": "att_d02f...",
      "filename": "chart.png",
      "mime_type": "image/png",
      "size_bytes": 45200,
      "data_b64": "iVBORw0KGgo...",
      "metadata": {"width": 800, "height": 600}
    }
  ]
}
```

### Why This Shape

- A **web scraper** returns parcels with `fields` (title, URL, text) and
  `attachments` (the HTML page, linked PDFs, embedded images).
- An **email trigger** returns parcels with `fields` (subject, sender, date)
  and `attachments` (the actual email attachments as files).
- A **weather block** returns parcels with only `fields` (temp, condition) and
  no attachments.
- An **RSS reader** returns multiple parcels, each with `fields` (title, link,
  summary) and no attachments.
- A **PDF generator** returns parcels with `fields` (metadata) and one
  attachment (the generated PDF).

Every block speaks the same language.

---

## Fittings

A fitting is a **typed connection point** on a block. Every fitting has a
unique ID, a direction (in/out), and a content declaration.

```python
class FittingKind(str, Enum):
    INPUT = "input"
    OUTPUT = "output"

class FittingDescriptor(BaseModel):
    uid: str                          # unique fitting ID (stable across versions)
    name: str                         # human-readable name (e.g. "articles")
    kind: FittingKind                 # input or output
    description: str = ""

    # What this fitting carries
    field_schema: dict[str, Any]      # JSON Schema for the `fields` dict
    accepted_mime_types: list[str] = ["*/*"]  # MIME types this fitting handles
    min_parcels: int = 0              # minimum parcels expected (0 = optional)
    max_parcels: int | None = None    # None = unlimited
```

### MIME Type Matching

Fittings declare which attachment types they can process:

```python
# An OCR block accepts images and PDFs
class OcrBlock(BaseBlock):
    input_fittings = [
        FittingDescriptor(
            uid="ocr_in_docs",
            name="documents",
            kind=FittingKind.INPUT,
            field_schema={},
            accepted_mime_types=[
                "application/pdf",
                "image/png",
                "image/jpeg",
                "image/tiff",
            ],
        ),
    ]
    output_fittings = [
        FittingDescriptor(
            uid="ocr_out_text",
            name="extracted",
            kind=FittingKind.OUTPUT,
            field_schema={
                "type": "object",
                "properties": {
                    "text": {"type": "string"},
                    "confidence": {"type": "number"},
                    "language": {"type": "string"},
                },
            },
            accepted_mime_types=[],  # text output, no binary
        ),
    ]
```

```python
# A web scraper produces HTML + any linked files
class WebScraperBlock(BaseBlock):
    output_fittings = [
        FittingDescriptor(
            uid="scraper_out_pages",
            name="pages",
            kind=FittingKind.OUTPUT,
            field_schema={
                "type": "object",
                "properties": {
                    "url": {"type": "string"},
                    "title": {"type": "string"},
                    "text_content": {"type": "string"},
                },
            },
            accepted_mime_types=[
                "text/html",
                "application/pdf",
                "image/*",
            ],
        ),
    ]
```

MIME matching uses standard patterns:
- `"application/pdf"` — exact match
- `"image/*"` — any image type
- `"*/*"` — accepts anything (default)

### Field Schema

The `field_schema` is a **JSON Schema** describing the `fields` dict
that parcels carry through this fitting. This enables:
- Validation at save time and runtime
- The visual editor to show available fields when wiring
- Auto-complete in transform expressions

---

## Pipes

A pipe connects one output fitting to one input fitting.

```python
class PipeDefinition(BaseModel):
    uid: str                          # unique pipe ID
    source_block_uid: str             # block instance UID in the pipeline
    source_fitting_uid: str           # output fitting UID on that block
    target_block_uid: str             # block instance UID in the pipeline
    target_fitting_uid: str           # input fitting UID on that block

    # Optional field-level mapping (when you don't want to pass everything)
    field_mapping: dict[str, str] | None = None   # source_field → target_field
    attachment_filter: list[str] | None = None     # MIME filter (e.g. ["application/pdf"])
    transform: str | None = None                   # lightweight expression
```

### Field Mapping

By default, all `fields` from the source parcel are passed through. When
you need to rename or select specific fields:

```python
PipeDefinition(
    uid="pipe_001",
    source_block_uid="weather_block",
    source_fitting_uid="weather_out",
    target_block_uid="email_block",
    target_fitting_uid="email_in",
    field_mapping={
        "temp": "temperature",        # rename
        "condition": "weather_desc",   # rename
        # city is not mapped → not passed
    },
)
```

### Attachment Filtering

When a source produces mixed attachments (HTML + PDFs + images), a
downstream block may only care about some of them:

```python
PipeDefinition(
    uid="pipe_002",
    source_block_uid="scraper_block",
    source_fitting_uid="scraper_out_pages",
    target_block_uid="ocr_block",
    target_fitting_uid="ocr_in_docs",
    attachment_filter=["application/pdf", "image/*"],
    # only PDFs and images reach the OCR block
)
```

Attachments not matching the filter are **dropped from that pipe** (they
still exist in the source block's output and can be wired elsewhere via
another pipe).

---

## Blocks

Every block instance in a pipeline has a UID and a type.

```python
class BlockInstance(BaseModel):
    uid: str                          # unique instance ID in this pipeline
    block_type: str                   # registered type (e.g. "rss_reader")
    label: str                        # user-facing name on the canvas
    config: dict[str, Any]            # static configuration (API keys, URLs, ...)
    position: BlockPosition           # visual editor position
    notes: str = ""                   # user annotation
```

UIDs are generated once at creation and **never change**. Pipes
reference block UIDs, so renaming a block or moving it on the canvas
doesn't break wiring.

---

## Type Compatibility

When wiring a pipe, the system checks two dimensions:

### 1. Field Compatibility

The source fitting's `field_schema` must be assignable to the target
fitting's `field_schema`. Rules:

- If the target schema is `{}` (empty) → accepts any fields, no validation.
- If the target defines required properties → the source must produce them
  (or the pipe must include a `field_mapping` that provides them).
- Extra fields from the source are passed through by default (unless
  `field_mapping` is set, which acts as an allowlist).

### 2. MIME Compatibility

At least one of the source fitting's `accepted_mime_types` must overlap
with the target fitting's `accepted_mime_types`.

```
source: ["text/html", "application/pdf", "image/*"]
target: ["application/pdf", "image/png", "image/jpeg"]
overlap: ["application/pdf", "image/*" ∩ "image/png", "image/*" ∩ "image/jpeg"]
→ compatible ✓
```

```
source: ["text/plain"]
target: ["application/pdf"]
→ incompatible ✗ (need a converter block in between)
```

The visual editor uses this to:
- **Color-code** fittings by their primary content type
- **Grey out** incompatible fittings while dragging a pipe
- **Show warnings** for partial MIME overlap ("only PDFs will pass through")

---

## Automatic Coercions

For `fields` values, the executor applies safe coercions:

| Source | Target | Coercion |
|---|---|---|
| `int` | `float` | Widen |
| `int`, `float`, `bool` | `string` | `str(value)` |
| `datetime` | `string` | ISO 8601 |
| single value | `array` | Wrap in `[value]` |
| `object` | `string` | JSON serialize |

No coercion for attachments — MIME types must match or be filtered.
If you need to convert a PDF to text, use an explicit converter block.

---

## Piping Patterns

### 1. Direct (1:1)

```
[RSS Reader] ──articles──► [LLM Summarizer]
```

Parcels flow through. Each parcel's fields and attachments are passed as-is
(or filtered/mapped by the pipe).

### 2. Fan-Out (1:N)

```
                    ┌──pages──► [OCR]           (only PDFs/images)
[Web Scraper] ─────┤
                    └──pages──► [Text Archive]  (everything)
```

Same output fitting wired to multiple targets. Each target gets a copy.
Different pipes can have different `attachment_filter` settings.

### 3. Fan-In (N:1)

```
[Weather] ──data──┐
                   ├──► [Merge] ──combined──► [Dashboard]
[News] ──articles─┘
```

Merge block collects parcels from multiple sources into one stream.

### 4. Iteration (list → per-item branch execution)

```
[Excel Reader] ──records──► [Split] ──item──► [HTTP Request] ──page──► [HTML Extractor]
                                                                              │
                                                                              ▼
                                                                         [Collect]
```

The Split block takes a list of records (e.g. URLs from a spreadsheet) and
**re-runs the downstream branch once per item**. Each item flows through the
same subgraph independently — an HTTP request, parser, LLM call, whatever the
user has wired up. The Collect block gathers results back into a list.

This is the core iteration primitive. It means:
- Every block stays simple (single input → single output)
- Users compose complex batch workflows from simple blocks
- The same HTTP Request block works for one URL or a thousand
- Error handling, retries, and concurrency are executor concerns

**Anti-pattern: batch blocks.** Do NOT create blocks like "HttpBatchBlock"
or "HtmlBatchExtractor" that internally loop over lists. Iteration belongs
in the executor/graph, not inside blocks. Blocks process one item at a time.
The executor handles fan-out, concurrency, error isolation, and collection.

#### How Fan-Out Works in the Executor

Fan-out is driven by `BaseBlock.fan_out_field` — a class variable naming
the output field whose list items become individual parcels:

```python
class SplitBlock(BaseBlock[SplitInput, SplitOutput]):
    fan_out_field: ClassVar[str | None] = "items"
```

When the executor sees a block with `fan_out_field`, it:
1. Executes the block normally.
2. Takes the list from `output.items` and creates one `Parcel` per item.
3. Downstream blocks run once per parcel (in batches with concurrency).

Fan-in is the inverse — `BaseBlock.fan_in = True` tells the executor to
gather all upstream parcels into a single `items` list before executing:

```python
class CollectBlock(BaseBlock[CollectInput, CollectOutput]):
    fan_in: ClassVar[bool] = True
```

#### Range Block for Numeric Iteration

The Range block generates `[{index: 0}, {index: 1}, ...]` for numeric
loops. Combined with fan-out, it creates loop-like iteration:

```
[Range(0..10)] ──items──► [Log("Item #{index}")] ──► [Collect] ──items──► [Excel Builder]
```

The Range block computes `len(range(...))` before allocating the list
to prevent OOM attacks — `range(0, 999999999)` is rejected at the size
check, not after allocating ~8 GB.

#### Accumulation Patterns

Results from fan-out iterations can be collected and condensed into
documents:

```
[Range] → [fan-out] → [Process] → [Collect] → [Excel Builder]
                                             → [PDF Builder]
```

The Collect block outputs `{items: [...]}`. Document builders like
Excel Builder accept either structured `sheets` definitions or a simple
`rows` list — the `rows` convenience field auto-creates a single sheet
from collected items.

#### Fan-Out Safety Limits

| Limit | Default | Env var |
|---|---|---|
| Max items | 10,000 | `PLUMBER_MAX_FAN_OUT_ITEMS` |
| Batch size | 200 | `PLUMBER_FAN_OUT_BATCH_SIZE` |
| Default concurrency | 10 | `PLUMBER_DEFAULT_FAN_OUT_CONCURRENCY` |

Wall-clock timeout (`MAX_RUN_WALL_SECONDS`) is checked between batches.

### 5. Enrichment (add attachments)

```
[DB Query] ──records──► [PDF Generator] ──records──► [Email Sender]
```

PDF Generator receives parcels with fields only, generates a PDF for each,
and adds it as an attachment. The downstream Email Sender receives parcels
with both fields (metadata) and attachments (the PDF).

### 6. Splitting (one parcel → many)

```
[Web Scraper] ──page──► [Attachment Splitter] ──files──► [OCR]
```

A page parcel with 5 attachments becomes 5 parcels, each with one
attachment. This lets you process each file individually.

### 7. Conditional Routing (by MIME type or field value)

```
                              ┌──pdfs──► [PDF Parser]
[Attachment Splitter] ──files─┤
                              ├──images──► [OCR]
                              └──other──► [Archive]
```

A Router block inspects each parcel's attachment MIME type (or field values)
and sends it down the matching branch.

---

## Real-World Example: Scrape → Extract → Summarize → Email

```
[Web Scraper]
  config: { url: "https://example.com/reports" }
  output fitting: "pages"
    field_schema: { url, title, text_content }
    mime_types: ["text/html", "application/pdf", "image/*"]
     │
     │  pipe: attachment_filter=["application/pdf"]
     ▼
[Attachment Splitter]
  input fitting: "documents" — accepts ["application/pdf"]
  output fitting: "files" — one parcel per PDF
    field_schema: { url, title, source_page }
    mime_types: ["application/pdf"]
     │
     ▼
[PDF Text Extractor]
  input fitting: "documents" — accepts ["application/pdf"]
  output fitting: "extracted"
    field_schema: { text, page_count, title }
    mime_types: []  (text only, no binary output)
     │
     ▼
[LLM Summarizer]
  input fitting: "text" — accepts any fields with a "text" key
  output fitting: "summaries"
    field_schema: { summary, key_points, title }
    mime_types: []
     │
     ▼
[Email Sender]
  input fitting: "content"
    field_schema: { summary, title }
    mime_types: []
  config: { to: "team@example.com", subject_template: "Report: {title}" }
```

---

## Safe Expression Evaluator

Blocks like **Text Template** and **Log** support `{expression}` placeholders
in their text fields. These are evaluated via a restricted Python evaluator
(`llming_plumber/blocks/core/safe_eval.py`) that prevents code injection.

### Allowed Constructs

- **Arithmetic:** `+`, `-`, `*`, `/`, `//`, `%`, `**`
- **Comparisons:** `==`, `!=`, `<`, `<=`, `>`, `>=`
- **Boolean logic:** `and`, `or`, `not`, ternary (`x if cond else y`)
- **Subscript:** `data["key"]`, `items[0]`
- **Literals:** strings, ints, floats, bools, `None`, lists, tuples, dicts
- **Whitelisted functions:** `str`, `int`, `float`, `bool`, `len`, `abs`,
  `min`, `max`, `round`
- **Variables:** passed in from piped fields or block config

### Blocked Constructs

- No imports, no attribute access, no `eval`/`exec`
- No method calls (`"abc".upper()` is rejected)
- No assignment, no loops, no function definitions

### Resource Guards

| Guard | Limit |
|---|---|
| Expression length | 1,000 chars |
| String result | 10,000 chars |
| Numeric result | ±10^15 |
| Power exponent | 100 |
| String/list repeat | 1,000 |
| Template length | 50,000 chars |
| Expressions per template | 100 |
| NaN / Infinity | Rejected |

### Template Syntax

```
Hello {name}, your score is {score * 100}%!
Use {{ and }} for literal braces.
```

Blocks that accept templates use Pydantic's `extra="allow"` on their input
model, so dynamically piped fields from upstream blocks become available
as template variables alongside explicit configuration values.

### Usage in Code

```python
from llming_plumber.blocks.core.safe_eval import safe_eval, render_template

safe_eval("index + 1", {"index": 3})          # → 4
render_template("Row {index}: {name}", {"index": 0, "name": "Alice"})  # → "Row 0: Alice"
```

---

## Runtime Execution

The executor processes blocks in topological order:

1. **Collect inputs** — for each input fitting, gather parcels from all
   wired pipes. Apply field mappings, attachment filters, and transforms.
2. **Validate** — check parcels against the fitting's `field_schema` and
   `accepted_mime_types`. Log warnings for parcels that don't match.
3. **Execute** — call `block.execute(parcels, ctx)` with the collected parcels.
4. **Store output** — serialize output parcels to the run's `block_states`.
   Attachments are stored as base64 in JSON (or offloaded to GridFS/S3
   for large files, referenced by UID).

```python
async def execute(self, parcels: list[Parcel], ctx: BlockContext) -> list[Parcel]:
    """Every block implements this. Receives parcels, returns parcels."""
    ...
```

### Large Binary Handling

For attachments exceeding a configurable threshold (default: 1 MB),
the executor stores the binary in MongoDB GridFS or S3 and replaces
`data_b64` with a reference:

```json
{
  "uid": "att_c91b...",
  "filename": "large_scan.tiff",
  "mime_type": "image/tiff",
  "size_bytes": 52428800,
  "data_b64": null,
  "storage_ref": "gridfs://plumber/att_c91b...",
  "metadata": {"width": 4800, "height": 6400}
}
```

Downstream blocks transparently resolve the reference when accessing the
data. The `BlockContext` provides a helper:

```python
data: bytes = await ctx.resolve_attachment(attachment)
```

---

## Schema Exposure for the Visual Editor

`GET /api/blocks` returns full fitting metadata:

```json
{
  "block_type": "web_scraper",
  "label": "Web Scraper",
  "categories": ["web"],
  "input_fittings": [
    {
      "uid": "scraper_in_urls",
      "name": "urls",
      "field_schema": {
        "type": "object",
        "properties": {
          "url": {"type": "string", "format": "uri"}
        },
        "required": ["url"]
      },
      "accepted_mime_types": [],
      "min_parcels": 1
    }
  ],
  "output_fittings": [
    {
      "uid": "scraper_out_pages",
      "name": "pages",
      "field_schema": {
        "type": "object",
        "properties": {
          "url": {"type": "string"},
          "title": {"type": "string"},
          "text_content": {"type": "string"}
        }
      },
      "accepted_mime_types": ["text/html", "application/pdf", "image/*"],
      "max_parcels": null
    }
  ]
}
```

The editor uses this to:
- Show typed connector dots (color-coded: green=text, blue=data, orange=binary, etc.)
- Allow dragging connections only between MIME-compatible fittings
- Preview which fields are available at each connection point
- Show attachment type icons on fittings that carry binary data
- Warn when only partial MIME overlap exists
