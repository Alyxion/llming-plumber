# File Browser

> Browse, search, and preview files produced by pipeline blocks — across all runs and storage backends.

---

## Overview

The file browser provides a visual interface for exploring files created by pipeline blocks that write to storage backends (e.g., Azure Blob Storage). It supports:

- **Version history** — select from past runs to see what files were produced at each point in time
- **Folder navigation** — browse the hierarchical file structure with breadcrumb navigation
- **File preview** — view raw content for text files (HTML, JSON, TXT, CSV, XML) up to 512 KB
- **HTML rendering** — switch between raw source and rendered preview for HTML files
- **Search** — filter files by name in the tree, or search within an open file's content with match highlighting
- **Any storage backend** — works with all file-producing blocks (sinks, resource blocks)

---

## How It Works

### Which Blocks Support Browsing?

The **Browse Files** button appears in the config popout for **resource blocks** (e.g., `azure_blob_resource`). Click on the sink/resource block in the editor to open its config, then click **Browse Files**.

The file browser shows all files in the storage container, including files written by all action blocks piped into this resource.

### Version Selection

When you open the file browser, it queries all past runs for the pipeline and shows versions where the block produced files. Each version shows:

- Run date and time
- Number of files written
- Total data size

Select any version to browse its files.

### Folder Structure

Files are organized in the structure created by the block. For example, the web crawler creates:

```
{domain}/
  {date}/
    html/
      page-slug.html
      another-page.html
    text/
      page-slug.txt
      another-page.txt
    content.json
```

Navigate folders by clicking them. Use the breadcrumb bar or ".." to go back up.

### File Preview

Click any file to preview it. The browser supports:

| File Type | Preview |
|-----------|---------|
| `.html`, `.htm` | Raw source + rendered preview (toggle between tabs) |
| `.json` | Syntax-highlighted raw content |
| `.txt`, `.csv`, `.xml` | Raw text content |
| Binary files | Size and content type info (no inline preview) |
| Files > 512 KB | Size info (too large for inline preview) |

### Hyperlink Intelligence

When browsing crawled pages, the file browser automatically loads the `content.json` manifest and builds a map of all cached URLs. This enables:

**HTML files:** A collapsible **Links** panel appears between the tab bar and the content area, listing all hyperlinks found in the page. Links whose target URL exists in the cache get a "cached" badge and a folder icon to open the cached version directly within the file browser. All links also have an external-link icon to open the original URL in a new tab.

**Text files:** A **source page bar** shows the original URL of the page, with shortcuts to open the cached HTML version or the live page in the browser.

### Search

The search bar works in two modes:

1. **File name search** — filters the file tree to show only matching files and folders
2. **Content search** — when a file is open for preview, the search term is highlighted within the file content, with a match count displayed

---

## API Endpoints

### List Block Versions

```
GET /api/pipelines/{pipeline_id}/blocks/{block_uid}/versions?limit=20
```

Returns past runs where the block produced files, sorted by date descending.

**Response:**
```json
[
  {
    "run_id": "...",
    "status": "completed",
    "created_at": "2026-03-09T14:30:00Z",
    "finished_at": "2026-03-09T14:35:00Z",
    "container": "plumber-crawls",
    "base_path": "www_example_com/2026-03-09",
    "files_written": 120,
    "total_bytes": 4500000
  }
]
```

### List Block Files

```
GET /api/runs/{run_id}/blocks/{block_uid}/files?prefix=
```

Returns folders and files at the given prefix within the block's storage path.

**Response:**
```json
{
  "container": "plumber-crawls",
  "base_path": "www_example_com/2026-03-09",
  "prefix": "html",
  "folders": ["subdir"],
  "files": [
    {
      "name": "index.html",
      "path": "www_example_com/2026-03-09/html/index.html",
      "size": 12345,
      "content_type": "text/html",
      "last_modified": "2026-03-09T14:31:00Z"
    }
  ],
  "total_files": 1,
  "total_folders": 1
}
```

### Get File Content

```
GET /api/runs/{run_id}/blocks/{block_uid}/files/content?path=...
```

Returns the raw file content for preview. For text files under 512 KB, returns the content with the appropriate Content-Type. For binary or large files, returns a JSON metadata stub:

```json
{
  "preview": false,
  "reason": "too_large",
  "size": 1048576,
  "content_type": "application/pdf",
  "path": "..."
}
```

---

## Data Storage

### Sink Metadata

When a block writes to a sink, the executor always stores the following metadata in `block_states` (regardless of the `LOG_BLOCK_OUTPUT` setting):

| Field | Description |
|-------|-------------|
| `sink_container` | Storage container name |
| `sink_base_path` | Base path prefix for all files |
| `sink_files_written` | Total files written |
| `sink_total_bytes` | Total bytes written |
| `sink_retention_days` | Auto-delete period |

For resource blocks, the resolved config (minus the connection string) is stored in `block_states.{uid}.resource_config`.

This metadata enables the file browser to locate files without requiring `LOG_BLOCK_OUTPUT` to be enabled.

---

## Adding File Browser Support to New Blocks

Any block that writes files can support the file browser by:

1. **Using the `Sink` interface** — call `ctx.sink.write(path, content)` during execution
2. **Connecting to a resource block** — the executor handles sink creation and metadata storage automatically

No additional code is needed in the block itself. The file browser uses the sink metadata stored by the executor.

For new storage backends (e.g., S3, GCS), implement:
1. A new `Sink` subclass in the resource block
2. A new resource block with `block_kind = "resource"` and `create_sink()`
3. The file browser API will need a backend-specific blob listing method (currently Azure-only)
