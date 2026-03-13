# Azure Blocks

> Read, write, list, and manage blobs in Azure Blob Storage — plus a resource block for streaming writes from other blocks.

All Azure blocks require a `connection_string` (stored as a secret in the block config).

---

## Action Blocks

### azure_blob_read

Download a blob from Azure Blob Storage.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **connection_string** | str (secret) | — | Azure Storage connection string |
| **container** | str | — | Container name |
| **blob_name** | str | — | Full blob path, e.g. `data/report.csv` |

**Output:** `content` (str, base64), `size_bytes` (int), `content_type` (str), `last_modified` (str)

---

### azure_blob_write

Upload data to Azure Blob Storage.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **connection_string** | str (secret) | — | Azure Storage connection string |
| **container** | str | — | Container name |
| **blob_name** | str | — | Destination blob path |
| **content** | str | — | Content to upload (text or Base64 binary) |
| **content_type** | str | — | MIME type (auto-detected if empty) |
| **metadata** | dict | `{}` | Custom blob metadata |

**Output:** `blob_name` (str), `container` (str), `size_bytes` (int), `url` (str)

---

### azure_blob_list

List blobs in a container, optionally filtered by prefix.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **connection_string** | str (secret) | — | Azure Storage connection string |
| **container** | str | — | Container name |
| **prefix** | str | — | Only return blobs starting with this prefix |
| **max_results** | int | `100` | Maximum blobs to return |

**Output:** `blobs` (list of blob info dicts), `blob_count` (int), `container` (str)

**Fan-out:** Each blob becomes an individual parcel for downstream processing.

---

### azure_blob_delete

Delete a blob from Azure Blob Storage.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **connection_string** | str (secret) | — | Azure Storage connection string |
| **container** | str | — | Container name |
| **blob_name** | str | — | Blob to delete |
| **delete_snapshots** | select | `include` | `include` (delete blob + snapshots) or `only` (snapshots only) |

**Output:** `blob_name` (str), `container` (str), `deleted` (bool)

---

### azure_blob_trigger

Detect new, modified, or deleted blobs by polling a container. Compare the current blob state against a previous snapshot to find changes.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **connection_string** | str (secret) | — | Azure Storage connection string |
| **container** | str | — | Container name |
| **prefix** | str | — | Only watch blobs with this prefix |
| **events** | list | `["created"]` | Event types: `created`, `modified`, `deleted` |
| **previous_state** | dict | `{}` | Previous blob state snapshot (from last run) |

**Output:** `events` (list of `BlobEvent`), `current_state` (dict — pass to next run's `previous_state`), `checked_at` (str), `container` (str)

**Fan-out:** Each blob event becomes an individual parcel.

Use with a timer trigger to periodically check for new files:
```
[Timer Trigger] → [Azure Blob Trigger] → [Azure Blob Read] → [Process...]
```

---

## Resource Block

### azure_blob_resource

Define an Azure Blob Storage target for streaming writes. This is a **resource block** — it is not executed as a pipeline step. Instead, it provides a `Sink` to connected action blocks.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **connection_string** | str (secret) | — | Azure Storage connection string |
| **container** | str | — | Container name |
| **base_path** | str | — | Prefix for all uploaded blobs |
| **retention_days** | int | `60` | Auto-delete blobs after N days (0 = no expiry) |

**Output:** `files_written` (int), `total_bytes` (int), `base_path` (str), `container` (str), `retention_days` (int)

**How it works:**

1. Connect an action block (e.g., `web_crawler`) to this resource block with a pipe.
2. During execution, the action block receives a `Sink` object via `ctx.sink`.
3. The action block calls `ctx.sink.write(path, content)` to stream individual files directly to Azure.
4. After execution, the sink's `finalize()` method returns a summary (files written, total bytes).

Each blob gets:
- Automatic content-type detection from file extension
- `expires_at` metadata for lifecycle management (requires Azure lifecycle policy for enforcement)

**Visual distinction:** Resource blocks appear with a dashed border and italic "resource" label in the UI editor.

**File browser:** Blocks connected to a resource block support the built-in [file browser](file-browser.md) — browse, search, and preview files from past runs directly in the UI.

**Example pipeline — crawl to cloud:**
```
[Web Crawler] ──pipe──→ [Azure Blob Resource]
```
The crawler streams each page as `{domain}/{date}/html/{slug}.html` and `text/{slug}.txt` directly to Azure — no memory buffering.
