# Building Blocks

> Plumber ships with **118 blocks** across 9 categories. Each block is a self-contained unit that can be used standalone or wired into pipelines.

---

## Block Reference

Detailed documentation for every block, organized by topic:

| Category | Blocks | Documentation |
|----------|--------|---------------|
| **Core** — triggers, flow control, data transformation, utilities | 33 | [blocks-core.md](blocks-core.md) |
| **Web** — crawling, scraping, monitoring, change detection | 6 | [blocks-web.md](blocks-web.md) |
| **Documents** — Excel, PDF, Word, PowerPoint, Parquet, YAML | 22 | [blocks-documents.md](blocks-documents.md) |
| **LLM** — chat, summarization, classification, extraction, translation | 10 | [blocks-llm.md](blocks-llm.md) |
| **Data** — file operations, archives, Redis, MongoDB, storage I/O | 34 | [blocks-data.md](blocks-data.md) |
| **Azure** — Blob Storage read/write/list/delete, triggers, resource | 6 | [blocks-azure.md](blocks-azure.md) |
| **Weather** — OpenWeatherMap, DWD (German Meteorological Service) | 2 | [blocks-weather.md](blocks-weather.md) |
| **News** — RSS feeds, NewsAPI, Tagesschau | 3 | [blocks-news.md](blocks-news.md) |
| **Government** — German federal APIs (Autobahn, NINA, Pegel, Feiertage) | 4 | [blocks-government.md](blocks-government.md) |

---

## Category Taxonomy

Every block belongs to one or more categories using a hierarchical tag system. This enables filtering in the no-code editor and API (`GET /api/blocks?category=government.weather`).

```
core/               — triggers, flow control, data transformation
  core/trigger
  core/flow
  core/transform
  core/data
web/                — scraping, crawling, monitoring
  web/crawl
  web/monitor
documents/          — files, parsing, generation
  documents/parsing
  documents/generation
news/               — RSS, news APIs
  news/feeds
  news/api
weather/            — weather data and forecasts
government/         — public sector APIs and open data
  government/news
  government/weather
  government/transport
  government/environment
  government/safety
  government/legal
llm/                — LLM-powered processing
  llm/chat
  llm/text
  llm/analysis
  llm/transform
data/               — file, archive, redis, mongodb
  data/file
  data/archive
  data/redis
  data/mongodb
azure/              — Azure cloud services
  azure/storage
```

---

## Block Kinds

Blocks have one of two kinds:

| Kind | Count | Behavior |
|------|-------|----------|
| **action** (default) | 112 | Executed as a pipeline step. Receives input, produces output. |
| **resource** | 1 | Not executed. Defines a storage target. Connected action blocks receive a `Sink` for streaming writes. |

See [Architecture — Block Kinds](architecture.md) for the technical details of the Sink abstraction.

---

## Special Execution Modes

### Fan-Out Blocks

These blocks split their output into individual items. Each downstream block runs once per item.

| Block | Fan-out field | Category |
|-------|---------------|----------|
| `split` | `items` | core/flow |
| `range` | `items` | core/flow |
| `file_list` | `files` | data/file |
| `file_collector` | `files` | data/file |
| `zip_extract` | `files` / `entries` | data/archive |
| `zip_list` | `entries` | data/archive |
| `azure_blob_list` | `blobs` | azure/storage |
| `azure_blob_trigger` | `events` | azure/storage |
| `redis_subscribe` | `messages` | data/redis |
| `mongo_find` | `documents` | data/mongodb |
| `mongo_aggregate` | `documents` | data/mongodb |
| `mongo_watch` | `events` | data/mongodb |

Fan-out is bounded by `MAX_FAN_OUT_ITEMS` (default 10,000) and processed in batches of `FAN_OUT_BATCH_SIZE` (200) with configurable concurrency.

### Fan-In Block

| Block | Category |
|-------|----------|
| `collect` | core/flow |

Gathers all upstream fan-out results into a single list.

### Trigger Blocks

| Block | Description |
|-------|-------------|
| `manual_trigger` | On-demand execution from UI or API |
| `timer_trigger` | Interval or cron-based scheduling |
| `azure_blob_trigger` | Detect new/modified/deleted blobs |
| `mongo_watch` | Watch MongoDB for real-time changes |

---

## Roadmap

The following block types are planned but not yet implemented:

### Communication & Notifications
- Send Email (SMTP), Microsoft 365 / Outlook, Microsoft Teams, Slack, SMS / WhatsApp

### ERP & Business Systems
- SAP Connector, Generic Database (SQL), SOAP / XML, GraphQL Client

### CRM & Sales
- Salesforce, HubSpot, Generic CRM Adapter

### Documents & Files
- SharePoint / OneDrive, Google Drive / Sheets, FTP / SFTP

### Cloud Infrastructure
- AWS S3, GCS, Message Queue (AMQP / MQTT), Kafka

### Other
- Code Block (Python / JS), Error Handler / Retry, Human Approval
- Embedding / Vector Search, OCR / Document AI
- Jira, Zendesk, Dashboard, Audit Log, Alerting
- Social Media Monitor
- Remaining bund.dev APIs (Lebensmittelwarnungen, Jobsuche, DESTATIS, Reisewarnungen, SMARD, Ladesäulenregister, Hochwasserzentralen, Luftqualität, DIP Bundestag, Handelsregister)

### Priority Tiers

**Tier 1 — Build first:** Webhook, REST API, Email, Conditional, Code Block, Error Handler, OAuth2 Manager

**Tier 2 — High value:** SAP, SQL Database, SharePoint, Teams, FTP/SFTP, Wait/Approval, Alerting

**Tier 3 — Important but can wait:** CRM connectors, Kafka, Vector Search, OCR, Social Media, remaining bund.dev APIs
