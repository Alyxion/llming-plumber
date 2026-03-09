# News Blocks

> RSS feeds, news APIs, and public broadcaster content for monitoring industry news and media.

---

### rss_reader

Read and parse RSS or Atom feeds.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **feed_url** | str | — | URL of the RSS/Atom feed |
| **max_entries** | int | `50` | Maximum entries to return |

**Output:**

| Field | Type | Description |
|-------|------|-------------|
| **entries** | list | Feed entries with `title`, `link`, `summary`, `published`, `author` |
| **entry_count** | int | Number of entries |
| **last_updated** | str | Feed last update timestamp |

**Cache TTL:** 300 seconds (5 minutes)

Works with any standard RSS 2.0 or Atom feed URL.

---

### news_api

Search news articles via the NewsAPI service.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **query** | str | — | Search query (e.g. `"steel prices" OR "raw materials"`) |
| **api_key** | str (secret) | — | NewsAPI key (or set `NEWSAPI_KEY` env var) |
| **from_date** | str | — | Start date (`YYYY-MM-DD`) |
| **to_date** | str | — | End date (`YYYY-MM-DD`) |
| **sort_by** | select | `relevancy` | `relevancy`, `popularity`, or `publishedAt` |
| **language** | str | `en` | Language code (e.g. `en`, `de`, `fr`) |

**Output:**

| Field | Type | Description |
|-------|------|-------------|
| **articles** | list | Articles with `title`, `description`, `url`, `source`, `published_at`, `content` |
| **total_results** | int | Total matching articles |

**Cache TTL:** 900 seconds (15 minutes)

---

### tagesschau

Fetch headlines and articles from the Tagesschau API — Germany's public broadcaster. Free, no API key required.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **section** | select | `home` | `home`, `inland`, `ausland`, `wirtschaft`, or `kultur` |
| **limit** | int | `20` | Maximum articles |

**Output:**

| Field | Type | Description |
|-------|------|-------------|
| **articles** | list | Articles with `title`, `date`, `content`, `url`, `tags` |
| **count** | int | Number of articles |

**Cache TTL:** 300 seconds (5 minutes)

Sections:
- `home` — top stories
- `inland` — domestic news
- `ausland` — international
- `wirtschaft` — business/economy
- `kultur` — culture

---

## Common Pipelines

**Industry news monitoring:**
```
[Timer Trigger (daily)] → [News API (query="your topic")] → [LLM Summarizer] → [Send Email]
```

**RSS aggregation:**
```
[Timer Trigger] → [RSS Reader (feed 1)] ──┐
[Timer Trigger] → [RSS Reader (feed 2)] ──┤→ [Merge] → [Deduplicator] → [Excel Builder]
[Timer Trigger] → [RSS Reader (feed 3)] ──┘
```
