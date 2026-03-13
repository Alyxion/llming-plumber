# Web Blocks

> Crawling, scraping, monitoring, and change detection for websites.

---

## Crawling

### web_crawler

Crawl a website starting from a URL, discovering and following links up to a configurable depth and page limit.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **start_url** | str | — | URL to start crawling from |
| **max_pages** | int | `100` | Maximum number of pages to crawl |
| **max_depth** | int | `3` | Maximum link depth from the start URL |
| **delay_seconds** | float | `1.0` | Delay between requests (be polite to servers) |
| **url_pattern** | str | — | Regex pattern — only follow URLs matching this |
| **exclude_pattern** | str | — | Regex pattern — skip URLs matching this |
| **user_agent** | str | — | Custom User-Agent header |
| **timeout** | float | `30.0` | Per-request timeout in seconds |
| **extract_text** | bool | `true` | Extract visible text from HTML |

**Output:**

| Field | Type | Description |
|-------|------|-------------|
| **pages** | list | List of page dicts with `url`, `title`, `text`, `html`, `status_code`, `content_hash` |
| **page_count** | int | Total pages crawled |
| **domain** | str | Domain of the start URL |
| **crawl_duration_seconds** | float | Total crawl time |
| **errors** | list | Any errors encountered |

**Sink streaming:** When connected to a resource block (e.g., Azure Blob Storage), the crawler streams each page as individual files instead of buffering in memory:
- `{domain}/{date}/html/{slug}.html` — raw HTML
- `{domain}/{date}/text/{slug}.txt` — extracted text
- `{domain}/{date}/content.json` — manifest with page metadata and URL-to-file mapping

This enables crawling large sites without memory pressure.

**Notes:**
- Respects trailing slashes — some servers return different responses for `/path` vs `/path/`
- Sends realistic browser headers to avoid bot detection
- Deduplicates URLs during crawl
- Pages are yielded as they are fetched, enabling real-time progress in the UI

---

## Scraping

### html_extractor

Extract specific content from HTML using CSS selectors.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **html** | str | — | HTML content to parse |
| **selector** | str | — | CSS selector (e.g. `div.product-price`, `h1`, `table tr`) |
| **extract_all** | bool | `true` | Return all matches or just the first |
| **attribute** | str | — | Extract a specific attribute (e.g. `href`, `src`) instead of text |

**Output:** `results` (list of extracted strings), `count` (int)

Common patterns:
- `a[href]` with attribute `href` — extract all links
- `table tr td` — extract table cell text
- `meta[name="description"]` with attribute `content` — extract meta tags

---

## Snapshots

### snapshot_save

Save crawl results as a JSON snapshot on disk for later comparison. Automatically rotates the previous snapshot.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **snapshot_id** | str | — | Unique identifier for this snapshot (e.g. `my-site`) |
| **pages** | list | `[]` | Crawled page dicts to store |
| **storage_dir** | str | `/tmp/plumber_snapshots` | Directory for snapshot files |

**Output:** `path` (str), `page_count` (int), `snapshot_id` (str), `timestamp` (str), `size_bytes` (int), `previous_exists` (bool)

Each save rotates: `current → previous`. This means you always have two snapshots available for diffing.

---

### snapshot_load

Load a previously saved crawl snapshot.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **snapshot_id** | str | — | The snapshot ID used when saving |
| **which** | select | `previous` | Load the `current` or `previous` snapshot |
| **storage_dir** | str | `/tmp/plumber_snapshots` | Directory where snapshots are stored |

**Output:** `pages` (list), `page_count` (int), `snapshot_id` (str), `timestamp` (str), `exists` (bool)

---

## Change Detection

### content_diff

Compare two text snapshots and detect changes. Produces a unified diff and change statistics.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **previous** | str | — | Previous text content |
| **current** | str | — | Current text content |
| **context_lines** | int | `3` | Lines of context around each change |
| **min_change_threshold** | float | `0.0` | Minimum change ratio to consider as "changed" (0.0–1.0) |
| **label** | str | — | Label for the diff summary |

**Output:**

| Field | Type | Description |
|-------|------|-------------|
| **has_changes** | bool | Whether any changes were detected |
| **change_ratio** | float | Ratio of changed lines (0.0–1.0) |
| **added_lines** | int | Number of added lines |
| **removed_lines** | int | Number of removed lines |
| **diff_text** | str | Unified diff output |
| **added_content** | str | All added text |
| **removed_content** | str | All removed text |
| **summary** | str | Human-readable summary |
| **label** | str | The label passed in |

---

### site_diff

Compare two full site crawls page-by-page. Detects new, removed, and modified pages.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **previous_pages** | list | `[]` | Pages from previous crawl |
| **current_pages** | list | `[]` | Pages from current crawl |
| **min_change_ratio** | float | `0.01` | Minimum change ratio to count a page as modified |
| **include_diff_text** | bool | `false` | Include unified diff text for modified pages |
| **label** | str | — | Label for the report |

**Output:**

| Field | Type | Description |
|-------|------|-------------|
| **has_changes** | bool | Whether any changes were detected |
| **new_pages** | list | Pages that exist only in current crawl |
| **removed_pages** | list | Pages that exist only in previous crawl |
| **modified_pages** | list | Pages with content changes |
| **new_count** | int | Number of new pages |
| **removed_count** | int | Number of removed pages |
| **modified_count** | int | Number of modified pages |
| **unchanged_count** | int | Number of unchanged pages |
| **total_previous** | int | Total pages in previous crawl |
| **total_current** | int | Total pages in current crawl |
| **report** | str | Human-readable change report |
| **label** | str | The label passed in |

---

## Common Pipelines

**Website monitoring** — detect changes on a site over time:
```
[Timer Trigger] → [Web Crawler] → [Snapshot Save]
                                        ↓
                  [Snapshot Load (previous)] → [Site Diff] → [Filter (has_changes)] → [Send Email]
```

**Scrape and extract** — pull structured data from a page:
```
[Web Crawler (max_pages=1)] → [HTML Extractor (selector="table tr")] → [Excel Builder]
```

**Crawl to cloud storage** — stream a full crawl to Azure without memory pressure:
```
[Web Crawler] ──→ [Azure Blob Resource]
```
The crawler writes each page individually to the connected storage resource.

After a run completes, use the built-in [file browser](file-browser.md) to browse, search, and preview crawled files from any past run.
