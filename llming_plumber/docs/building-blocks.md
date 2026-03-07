# Workflow Automation Platform — Building Blocks

> Core building blocks for a workflow automation platform.

---

## Category Taxonomy

Every block belongs to one or more categories using a hierarchical tag
system. This enables filtering in the no-code editor and API (`GET /api/blocks?category=government.weather`).

```
core/               — triggers, flow control, data transformation
communication/      — email, chat, SMS, notifications
  communication/email
  communication/chat
  communication/sms
business/           — ERP, CRM, sales, finance
  business/erp
  business/crm
  business/database
  business/api
documents/          — files, storage, parsing, generation
  documents/cloud-storage
  documents/parsing
  documents/generation
  documents/transfer
web/                — scraping, monitoring, feeds
news/               — RSS, news APIs, press
  news/feeds
  news/api
  news/monitoring
weather/            — weather data and forecasts
government/         — public sector APIs and open data
  government/news
  government/weather
  government/transport
  government/environment
  government/employment
  government/safety
  government/energy
  government/statistics
  government/legal
  government/health
  government/education
infrastructure/     — cloud, queues, streaming
ai/                 — LLM, OCR, embeddings, translation
project/            — ticketing, task management
auth/               — authentication, credentials
observability/      — dashboards, logs, alerting
```

---

## 1 — Triggers & Scheduling

| # | Block | Categories | Purpose |
|---|-------|------------|---------|
| 1 | **Cron / Scheduler** | `core/trigger` | Run workflows on a fixed schedule (every 5 min, daily, weekly). The heartbeat of any automation. |
| 2 | **Webhook Receiver** | `core/trigger` | Accept incoming HTTP calls from external systems (SAP events, shop-floor sensors, partner portals). |
| 3 | **File Watcher** | `core/trigger`, `documents/transfer` | Trigger when a file appears or changes in a directory, SFTP server, or cloud bucket (e.g. incoming EDI orders). |
| 4 | **Email Trigger** | `core/trigger`, `communication/email` | Start a workflow when a matching email arrives (customer inquiries, supplier confirmations, RFQs). |
| 5 | **Database Trigger** | `core/trigger`, `business/database` | Poll or listen for row changes in a database table (new order, stock level change, price update). |

## 2 — Communication & Notifications

| # | Block | Categories | Purpose |
|---|-------|------------|---------|
| 6 | **Send Email (SMTP)** | `communication/email` | Send transactional emails — order confirmations, shipping notices, internal alerts. |
| 7 | **Microsoft 365 / Outlook** | `communication/email`, `communication/chat` | Read/send mail, calendar events, contacts via the Graph API. Standard in German Mittelstand. |
| 8 | **Microsoft Teams** | `communication/chat` | Post messages, create channels, receive commands. Primary internal chat for many manufacturers. |
| 9 | **Slack** | `communication/chat` | Alternative team messaging — common in tech-leaning departments and with external dev partners. |
| 10 | **SMS / WhatsApp** | `communication/sms` | Send SMS or WhatsApp messages for urgent alerts (machine down, delivery exception, approval needed). |

## 3 — ERP & Business Systems

| # | Block | Categories | Purpose |
|---|-------|------------|---------|
| 11 | **SAP Connector (RFC / OData / BAPI)** | `business/erp` | Read/write master data, post orders, query stock, trigger goods movements in SAP S/4HANA. The single most critical integration for a manufacturer on SAP. |
| 12 | **Generic Database (SQL)** | `business/database` | Query and write to PostgreSQL, MySQL, MS SQL, Oracle. For legacy systems, data warehouses, reporting DBs. |
| 13 | **Generic REST API** | `business/api` | Call any REST endpoint with configurable auth, headers, body. The universal adapter. |
| 14 | **SOAP / XML Web Service** | `business/api` | Many older industrial systems and EDI gateways still speak SOAP. Required for legacy B2B integrations. |
| 15 | **GraphQL Client** | `business/api` | Modern API pattern used by newer SaaS tools and internal microservices. |

## 4 — CRM & Sales

| # | Block | Categories | Purpose |
|---|-------|------------|---------|
| 16 | **Salesforce** | `business/crm` | Manage leads, opportunities, accounts, contacts. Widely used CRM in global B2B companies. |
| 17 | **HubSpot** | `business/crm` | Marketing automation, CRM, lead scoring. Common alternative for mid-market. |
| 18 | **Generic CRM Adapter** | `business/crm` | Abstract create/read/update on contacts, companies, deals for any CRM (SAP CRM, MS Dynamics, Pipedrive). |

## 5 — Documents & Files

| # | Block | Categories | Purpose |
|---|-------|------------|---------|
| 19 | **SharePoint / OneDrive** | `documents/cloud-storage` | Read/write files and lists in SharePoint. Document management backbone in Microsoft shops. |
| 20 | **Google Drive / Sheets** | `documents/cloud-storage`, `documents/parsing` | Access spreadsheets and files. Common for lighter collaboration and data exchange with partners. |
| 21 | **PDF Generator** | `documents/generation` | Create PDFs from templates — quotes, invoices, delivery notes, certificates of conformity. Critical in B2B manufacturing. |
| 22 | **Excel / CSV Parser** | `documents/parsing` | Read and write Excel and CSV files. Price lists, export data, supplier catalogs all live in spreadsheets. |
| 23 | **FTP / SFTP** | `documents/transfer` | Upload and download files from FTP servers. Still the backbone of EDI and partner data exchange in manufacturing. |

## 6 — Web, News & Content

| # | Block | Categories | Purpose |
|---|-------|------------|---------|
| 24 | **RSS Feed Reader** | `news/feeds`, `web` | Monitor industry news feeds, competitor blogs, trade publications (e.g. Process Engineering, Chemical Engineering). |
| 25 | **Web Scraper / HTML Parser** | `web` | Extract structured data from web pages — competitor pricing, regulatory updates, raw material indices. |
| 26 | **News API** | `news/api` | Query news aggregators for articles mentioning the company, competitors, or key industry terms (steel prices, REACH regulation, etc.). |
| 27 | **Website Monitor (Change Detection)** | `web`, `news/monitoring` | Detect changes on specific web pages — supplier portals, regulatory bodies, standards organizations (DIN, ISO). |
| 28 | **Social Media Monitor** | `web`, `news/monitoring` | Track mentions on LinkedIn, X/Twitter — brand monitoring, industry trends, recruiting signals. |
| 29 | **Weather API (OpenWeatherMap)** | `weather` | Current conditions, hourly and 5-day forecasts by city. Useful for logistics planning, outdoor operations, agriculture-related workflows, and dashboard widgets. |

## 7 — Cloud Storage & Infrastructure

| # | Block | Categories | Purpose |
|---|-------|------------|---------|
| 30 | **AWS S3 / Azure Blob / GCS** | `infrastructure`, `documents/cloud-storage` | Read and write objects to cloud storage. For backups, data lake ingestion, large file transfers. |
| 31 | **Message Queue (AMQP / MQTT)** | `infrastructure` | Publish and consume messages from RabbitMQ, Azure Service Bus, or MQTT brokers (IoT sensors). |
| 32 | **Kafka** | `infrastructure` | High-throughput event streaming for real-time data pipelines (shop-floor events, telemetry). |

## 8 — Logic, Flow Control & Data Transformation

| # | Block | Categories | Purpose |
|---|-------|------------|---------|
| 33 | **If / Switch (Conditional)** | `core/flow` | Branch workflow based on conditions — route by country, order value, product line, customer tier. |
| 34 | **Split** | `core/flow` | Fan-out — take a list and re-run downstream blocks once per item. Uses `fan_out_field` on `BaseBlock`. |
| 35 | **Collect** | `core/flow` | Fan-in — gather all upstream parcels back into a single list. Uses `fan_in` on `BaseBlock`. |
| 36 | **Range** | `core/flow` | Generate a numbered sequence (`[{index: 0}, {index: 1}, ...]`) for iteration, like Python's `range()`. Fans out via `fan_out_field = "items"`. |
| 37 | **Wait** | `core/flow` | Async sleep, capped at `MAX_WAIT_SECONDS` (default 300s). For rate limiting, polling intervals, or staged execution. |
| 38 | **Log** | `core/flow` | Write a message to the run console via `ctx.log()`. Supports `{expression}` interpolation with piped variables. |
| 39 | **Text Template** | `core/transform` | Render templates with `{expression}` placeholders using the safe expression evaluator. Accepts piped fields as variables. |
| 40 | **Merge / Join** | `core/flow` | Combine data from multiple branches or sources — enrich an order with CRM data and stock levels. |
| 41 | **Data Mapper / Transformer (JSON, XML)** | `core/transform` | Reshape, filter, and map data between different formats and schemas. The glue between every system. |
| 42 | **Code Block (Python / JS)** | `core/transform` | Run custom logic when visual nodes aren't enough — unit conversions, calculations, business rules. |
| 43 | **Error Handler / Retry** | `core/flow` | Catch failures, retry with backoff, send alerts. Essential for production-grade workflows. |
| 44 | **Wait / Delay / Human Approval** | `core/flow` | Pause a workflow for a timer or until a human approves (purchase orders above threshold, new supplier onboarding). |

### Fan-Out / Fan-In Patterns

The **Split** and **Collect** blocks (together with **Range**) enable
iteration without building batch variants of blocks:

```
[Range(0..5)] ──items──► [Log("Processing #{index}")] ──► [Collect] ──items──► [Excel Builder]
```

- **Range** generates `[{index: 0}, ..., {index: 4}]` and fans out.
- Each downstream block runs once per item (Log writes to the console).
- **Collect** gathers all results back into a single list.
- The list can be piped into document builders (Excel, PDF, etc.).

Fan-out is bounded by `MAX_FAN_OUT_ITEMS` (default 10,000) and processed
in batches of `FAN_OUT_BATCH_SIZE` (200) with configurable concurrency
(`DEFAULT_FAN_OUT_CONCURRENCY` = 10). Wall-clock timeout applies between
batches.

## 9 — AI & Analytics

| # | Block | Categories | Purpose |
|---|-------|------------|---------|
| 45 | **LLM / Chat Completion (OpenAI, Anthropic, local)** | `ai` | Summarize documents, classify emails, extract data from unstructured text, draft responses. |
| 46 | **Embedding / Vector Search** | `ai` | Semantic search over product documentation, technical specs, past customer tickets. |
| 47 | **OCR / Document AI** | `ai`, `documents/parsing` | Extract text and fields from scanned documents — incoming invoices, delivery notes, certificates. |
| 48 | **Translation** | `ai` | Auto-translate emails, documents, support tickets across DE/EN/FR/ES/ZH for global operations. |

## 10 — Project Management & Ticketing

| # | Block | Categories | Purpose |
|---|-------|------------|---------|
| 49 | **Jira** | `project` | Create/update issues, sync with development workflows. For engineering and IT teams. |
| 50 | **Generic Ticketing (Zendesk, Freshdesk, ServiceNow)** | `project` | Customer support ticket management — create, update, escalate, close. |
| 51 | **Todoist / Asana / Trello** | `project` | Lightweight task management for non-engineering teams (marketing campaigns, trade show planning). |

## 11 — Authentication & Security

| # | Block | Categories | Purpose |
|---|-------|------------|---------|
| 52 | **OAuth2 / API Key Manager** | `auth` | Centralized credential store for all connected services. Handles token refresh, rotation, scoping. |
| 53 | **LDAP / Active Directory** | `auth` | Look up users, validate roles, sync groups. For workflows that need to check who can approve what. |

## 12 — Reporting & Observability

| # | Block | Categories | Purpose |
|---|-------|------------|---------|
| 54 | **Dashboard / KPI Widget** | `observability` | Display live workflow metrics — runs per day, error rate, average processing time. |
| 55 | **Audit Log** | `observability` | Immutable record of every workflow run, every data transformation, every approval. Required for ISO 9001 and regulated industries (pharma, food). |
| 56 | **Alerting (PagerDuty / Opsgenie / Email)** | `observability`, `communication/email` | Escalate critical failures to on-call staff — SAP sync broken, order stuck, sensor offline. |

## 13 — German Government & Public Data (bund.dev)

Free, open APIs from German federal agencies. No API keys required for most.
Full catalog at [bund.dev](https://bund.dev/apis) / [github.com/bundesAPI](https://github.com/bundesapi).

| # | Block | Categories | Purpose |
|---|-------|------------|---------|
| 52 | **Tagesschau API** | `government/news`, `news/api` | Public broadcaster news feed — headlines, articles, videos. Free, full-text, no key needed. |
| 53 | **DWD Weather (Deutscher Wetterdienst)** | `government/weather`, `weather` | Official German weather data from all stations — forecasts, warnings, historical data. Free, no key. |
| 54 | **NINA Warnings** | `government/safety` | Civil protection alerts — severe weather, floods, industrial accidents, police warnings. |
| 55 | **Pegel-Online** | `government/environment` | Real-time water level measurements from rivers and waterways across Germany. |
| 56 | **Autobahn API** | `government/transport` | Real-time highway data — construction sites, traffic jams, webcams, EV charging stations. |
| 57 | **Lebensmittelwarnungen** | `government/safety` | Official food and product safety warnings from the federal portal. |
| 58 | **Feiertage API** | `government/legal` | Public holidays by state and year. Useful for scheduling and business day calculations. |
| 59 | **Jobsuche API** | `government/employment` | Germany's largest job database (Bundesagentur für Arbeit). Search listings, salaries, training. |
| 60 | **DESTATIS / Dashboard Deutschland** | `government/statistics` | Official federal statistics — economic indicators, demographics, trade data. |
| 61 | **Reisewarnungen** | `government/safety`, `government/transport` | Travel warnings from the Auswärtiges Amt (Foreign Office). |
| 62 | **SMARD Strommarktdaten** | `government/energy` | Electricity market data from the Bundesnetzagentur — generation, consumption, prices. |
| 63 | **Ladesäulenregister** | `government/energy`, `government/transport` | Official registry of public EV charging stations across Germany. |
| 64 | **Hochwasserzentralen** | `government/environment`, `government/safety` | Cross-regional flood warning portal — water levels, flood alerts. |
| 65 | **Luftqualität** | `government/environment` | Air quality measurements and visualizations from the Umweltbundesamt. |
| 66 | **DIP Bundestag** | `government/legal` | Legislative processes — bills, votes, protocols, documents from the German parliament. |
| 67 | **Handelsregister** | `government/legal`, `business/crm` | German business registry — company lookup, filings, ownership data. |

---

## Priority Tiers for Implementation

**Tier 1 — Build first (can't run without these):**
Cron, Webhook, REST API, Email Send/Receive, Conditional, Split, Collect,
Range, Wait, Log, Text Template, Data Mapper, Code Block, Error Handler,
Audit Log, OAuth2 Manager

**Tier 2 — High value (cover 80% of real workflows):**
SAP Connector, SQL Database, SharePoint, Microsoft Teams, Excel/CSV Parser,
PDF Generator, FTP/SFTP, RSS Reader, Web Scraper, LLM, Wait/Approval,
Alerting, Tagesschau, DWD Weather

**Tier 3 — Important but can wait:**
Everything else — CRM connectors, Kafka, Vector Search, OCR, Translation,
Social Media, SMS, Ticketing, Dashboard, remaining bund.dev APIs
