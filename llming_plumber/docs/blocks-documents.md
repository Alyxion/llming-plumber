# Document Blocks

> Read, write, build, and extract structured content from Excel, PDF, Word, PowerPoint, Parquet, YAML, and Markdown files.

All document blocks handle binary content as Base64-encoded strings, making them safe for JSON transport through pipes.

---

## Excel

### excel_reader

Read an Excel file (`.xlsx` / `.xls`) into row dicts.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **content** | str (base64) | — | Base64-encoded Excel file |
| **sheet_names** | list | `[]` | Sheets to read (empty = all) |
| **header_row** | int | `1` | Row number containing column headers |

**Output:** `rows` (list of dicts), `row_count` (int), `sheet_names` (list)

---

### excel_writer

Write a list of row dicts to an Excel file.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **rows** | list | `[]` | Row dicts to write |
| **sheet_name** | str | `Sheet1` | Name of the sheet |
| **include_header** | bool | `true` | Include column headers |
| **columns** | list | `[]` | Column order (empty = auto-detect from keys) |

**Output:** `content` (str, base64), `file_size` (int)

---

### excel_builder

Build complex Excel workbooks with multiple sheets, styling, column widths, and freeze panes.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **sheets** | list[SheetDef] | `[]` | Sheet definitions (structured) |
| **sheets_json** | str | — | Alternative: JSON string of sheet definitions |

Each `SheetDef` contains:
- `name` — sheet name
- `rows` — list of row dicts
- `columns` — column definitions with widths and formats
- `freeze_row` / `freeze_col` — freeze pane position
- `styles` — cell and header styling

**Output:** `content` (str, base64), `file_size` (int)

---

### excel_extractor

Extract structured `SheetDef` models from an Excel file — the inverse of `excel_builder`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **content** | str (base64) | — | Base64-encoded Excel file |
| **sheet_names** | list | `[]` | Sheets to extract (empty = all) |
| **header_row** | int | `1` | Header row number |

**Output:** `sheets` (list[SheetDef]), `sheets_json` (str)

Useful for round-tripping: read an Excel file, modify the `SheetDef` structures, then rebuild with `excel_builder`.

---

## PDF

### pdf_reader

Extract text from a PDF file.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **content** | str (base64) | — | Base64-encoded PDF |
| **pages** | list | `[]` | Page numbers to extract (empty = all) |

**Output:** `text` (str), `page_count` (int), `title` (str)

---

### pdf_builder

Build PDF documents from structured page definitions. Supports text mode (paragraphs, headers), geometric mode (positioned elements), or mixed.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **pages** | list[PageDef] | `[]` | Page definitions |
| **pages_json** | str | — | Alternative: JSON string of page definitions |
| **format** | select | `A4` | Page format: `A4` or `Letter` |
| **mode** | select | `text` | Rendering mode: `text`, `geometric`, or `mixed` |

**Output:** `content` (str, base64), `page_count` (int), `file_size` (int)

---

### pdf_extractor

Extract structured `PageDef` models from a PDF — the inverse of `pdf_builder`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **content** | str (base64) | — | Base64-encoded PDF |
| **mode** | select | `text` | Extraction mode: `text`, `geometric`, or `mixed` |
| **pages** | list | `[]` | Page numbers to extract (empty = all) |

**Output:** `pages_def` (list[PageDef]), `pages_json` (str), `mode` (str)

---

### pdf_renderer

Render PDF pages to images (PNG or JPG).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **content** | str (base64) | — | Base64-encoded PDF |
| **pages** | list | `[]` | Pages to render (empty = all) |
| **format** | select | `png` | Output format: `png` or `jpg` |
| **dpi** | int | `150` | Resolution in dots per inch |

**Output:** `images` (list of base64 strings), `image_count` (int)

---

## Word

### word_reader

Read text from a Word (`.docx`) document.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **content** | str (base64) | — | Base64-encoded Word file |

**Output:** `text` (str), `paragraph_count` (int), `has_tables` (bool)

---

### word_writer

Write a simple Word document from paragraphs.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **paragraphs** | list | `[]` | Paragraph strings |
| **title** | str | — | Document title |
| **author** | str | — | Document author |

**Output:** `content` (str, base64), `file_size` (int)

---

### word_builder

Build complex Word documents with sections, tables, images, and formatting.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **sections** | list[SectionDef] | `[]` | Section definitions |
| **sections_json** | str | — | Alternative: JSON string of section definitions |

Each `SectionDef` can contain headings, paragraphs, tables, images, and style overrides.

**Output:** `content` (str, base64), `file_size` (int)

---

### word_extractor

Extract structured `SectionDef` models from a Word document.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **content** | str (base64) | — | Base64-encoded Word file |
| **extract_formatting** | bool | `true` | Include formatting details |

**Output:** `sections` (list[SectionDef]), `sections_json` (str)

---

## PowerPoint

### powerpoint_reader

Read text from a PowerPoint (`.pptx`) presentation.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **content** | str (base64) | — | Base64-encoded PowerPoint file |
| **extract_images** | bool | `false` | Also extract embedded images |

**Output:** `slides_text` (list of strings), `slide_count` (int)

---

### powerpoint_writer

Write a simple PowerPoint presentation from slide texts.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **slides** | list | `[]` | Slide content (text strings) |
| **title** | str | — | Presentation title |
| **author** | str | — | Author name |

**Output:** `content` (str, base64), `slide_count` (int), `file_size` (int)

---

### powerpoint_builder

Build complex PowerPoint presentations with text boxes, shapes, tables, and images.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **slides** | list[SlideDef] | `[]` | Slide definitions |
| **slides_json** | str | — | Alternative: JSON string of slide definitions |

**Output:** `content` (str, base64), `slide_count` (int), `file_size` (int)

---

### powerpoint_extractor

Extract structured `SlideDef` models from a PowerPoint file.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **content** | str (base64) | — | Base64-encoded PowerPoint file |
| **extract_shapes** | bool | `true` | Extract shape details |
| **extract_images** | bool | `false` | Extract embedded images |

**Output:** `slides` (list[SlideDef]), `slides_json` (str)

---

## Data Formats

### parquet_reader

Read Apache Parquet files into a list of records.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **content** | str (base64) | — | Base64-encoded Parquet file |
| **columns** | list | `[]` | Columns to read (empty = all) |

**Output:** `records` (list of dicts), `record_count` (int)

---

### parquet_writer

Write records to an Apache Parquet file.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **records** | list | `[]` | Records to write |
| **columns** | list | `[]` | Column order (empty = auto-detect) |

**Output:** `content` (str, base64), `record_count` (int), `file_size` (int)

---

### yaml_parser

Parse YAML text into structured data.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **content** | str (textarea) | — | YAML text |

**Output:** `data` (dict), `is_valid` (bool)

---

### yaml_writer

Write structured data to YAML text.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **data** | dict | `{}` | Data to serialize |
| **indent** | int | `2` | Indentation level |

**Output:** `content` (str), `file_size` (int)

---

### markdown_renderer

Render Markdown text to HTML.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **markdown** | str (textarea) | — | Markdown source text |
| **extensions** | list | `[]` | Markdown extensions to enable |
| **sanitize** | bool | `true` | Sanitize output HTML |

**Output:** `html` (str), `word_count` (int)

---

## Builder / Extractor Pattern

Document blocks follow a consistent pattern:

| Block | Purpose |
|-------|---------|
| `*_reader` | Simple text extraction from a document |
| `*_writer` | Simple document creation from basic inputs |
| `*_builder` | Complex document creation from structured definitions |
| `*_extractor` | Extract structured definitions from existing documents |

The `builder` and `extractor` blocks are inverses — you can extract a structure from an existing document, modify it, and rebuild. This enables template-based document generation where the template is an actual document file.
