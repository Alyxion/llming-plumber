# Workflow Automation Platform — Building Blocks

> 51 core building blocks for a workflow automation platform.

---

## 1 — Triggers & Scheduling

| # | Block | Purpose |
|---|-------|---------|
| 1 | **Cron / Scheduler** | Run workflows on a fixed schedule (every 5 min, daily, weekly). The heartbeat of any automation. |
| 2 | **Webhook Receiver** | Accept incoming HTTP calls from external systems (SAP events, shop-floor sensors, partner portals). |
| 3 | **File Watcher** | Trigger when a file appears or changes in a directory, SFTP server, or cloud bucket (e.g. incoming EDI orders). |
| 4 | **Email Trigger** | Start a workflow when a matching email arrives (customer inquiries, supplier confirmations, RFQs). |
| 5 | **Database Trigger** | Poll or listen for row changes in a database table (new order, stock level change, price update). |

## 2 — Communication & Notifications

| # | Block | Purpose |
|---|-------|---------|
| 6 | **Send Email (SMTP)** | Send transactional emails — order confirmations, shipping notices, internal alerts. |
| 7 | **Microsoft 365 / Outlook** | Read/send mail, calendar events, contacts via the Graph API. Standard in German Mittelstand. |
| 8 | **Microsoft Teams** | Post messages, create channels, receive commands. Primary internal chat for many manufacturers. |
| 9 | **Slack** | Alternative team messaging — common in tech-leaning departments and with external dev partners. |
| 10 | **SMS / WhatsApp** | Send SMS or WhatsApp messages for urgent alerts (machine down, delivery exception, approval needed). |

## 3 — ERP & Business Systems

| # | Block | Purpose |
|---|-------|---------|
| 11 | **SAP Connector (RFC / OData / BAPI)** | Read/write master data, post orders, query stock, trigger goods movements in SAP S/4HANA. The single most critical integration for a manufacturer on SAP. |
| 12 | **Generic Database (SQL)** | Query and write to PostgreSQL, MySQL, MS SQL, Oracle. For legacy systems, data warehouses, reporting DBs. |
| 13 | **Generic REST API** | Call any REST endpoint with configurable auth, headers, body. The universal adapter. |
| 14 | **SOAP / XML Web Service** | Many older industrial systems and EDI gateways still speak SOAP. Required for legacy B2B integrations. |
| 15 | **GraphQL Client** | Modern API pattern used by newer SaaS tools and internal microservices. |

## 4 — CRM & Sales

| # | Block | Purpose |
|---|-------|---------|
| 16 | **Salesforce** | Manage leads, opportunities, accounts, contacts. Widely used CRM in global B2B companies. |
| 17 | **HubSpot** | Marketing automation, CRM, lead scoring. Common alternative for mid-market. |
| 18 | **Generic CRM Adapter** | Abstract create/read/update on contacts, companies, deals for any CRM (SAP CRM, MS Dynamics, Pipedrive). |

## 5 — Documents & Files

| # | Block | Purpose |
|---|-------|---------|
| 19 | **SharePoint / OneDrive** | Read/write files and lists in SharePoint. Document management backbone in Microsoft shops. |
| 20 | **Google Drive / Sheets** | Access spreadsheets and files. Common for lighter collaboration and data exchange with partners. |
| 21 | **PDF Generator** | Create PDFs from templates — quotes, invoices, delivery notes, certificates of conformity. Critical in B2B manufacturing. |
| 22 | **Excel / CSV Parser** | Read and write Excel and CSV files. Price lists, export data, supplier catalogs all live in spreadsheets. |
| 23 | **FTP / SFTP** | Upload and download files from FTP servers. Still the backbone of EDI and partner data exchange in manufacturing. |

## 6 — Web, News & Content

| # | Block | Purpose |
|---|-------|---------|
| 24 | **RSS Feed Reader** | Monitor industry news feeds, competitor blogs, trade publications (e.g. Process Engineering, Chemical Engineering). |
| 25 | **Web Scraper / HTML Parser** | Extract structured data from web pages — competitor pricing, regulatory updates, raw material indices. |
| 26 | **News API** | Query news aggregators for articles mentioning the company, competitors, or key industry terms (steel prices, REACH regulation, etc.). |
| 27 | **Website Monitor (Change Detection)** | Detect changes on specific web pages — supplier portals, regulatory bodies, standards organizations (DIN, ISO). |
| 28 | **Social Media Monitor** | Track mentions on LinkedIn, X/Twitter — brand monitoring, industry trends, recruiting signals. |
| 29 | **Weather API (OpenWeatherMap)** | Current conditions, hourly and 5-day forecasts by city. Useful for logistics planning, outdoor operations, agriculture-related workflows, and dashboard widgets. |

## 7 — Cloud Storage & Infrastructure

| # | Block | Purpose |
|---|-------|---------|
| 30 | **AWS S3 / Azure Blob / GCS** | Read and write objects to cloud storage. For backups, data lake ingestion, large file transfers. |
| 31 | **Message Queue (AMQP / MQTT)** | Publish and consume messages from RabbitMQ, Azure Service Bus, or MQTT brokers (IoT sensors). |
| 32 | **Kafka** | High-throughput event streaming for real-time data pipelines (shop-floor events, telemetry). |

## 8 — Logic, Flow Control & Data Transformation

| # | Block | Purpose |
|---|-------|---------|
| 33 | **If / Switch (Conditional)** | Branch workflow based on conditions — route by country, order value, product line, customer tier. |
| 34 | **Loop / Iterator** | Process a list of items one by one — line items in an order, batch of emails, list of products to check. |
| 35 | **Merge / Join** | Combine data from multiple branches or sources — enrich an order with CRM data and stock levels. |
| 36 | **Data Mapper / Transformer (JSON, XML)** | Reshape, filter, and map data between different formats and schemas. The glue between every system. |
| 37 | **Code Block (Python / JS)** | Run custom logic when visual nodes aren't enough — unit conversions, calculations, business rules. |
| 38 | **Error Handler / Retry** | Catch failures, retry with backoff, send alerts. Essential for production-grade workflows. |
| 39 | **Wait / Delay / Human Approval** | Pause a workflow for a timer or until a human approves (purchase orders above threshold, new supplier onboarding). |

## 9 — AI & Analytics

| # | Block | Purpose |
|---|-------|---------|
| 40 | **LLM / Chat Completion (OpenAI, Anthropic, local)** | Summarize documents, classify emails, extract data from unstructured text, draft responses. |
| 41 | **Embedding / Vector Search** | Semantic search over product documentation, technical specs, past customer tickets. |
| 42 | **OCR / Document AI** | Extract text and fields from scanned documents — incoming invoices, delivery notes, certificates. |
| 43 | **Translation** | Auto-translate emails, documents, support tickets across DE/EN/FR/ES/ZH for global operations. |

## 10 — Project Management & Ticketing

| # | Block | Purpose |
|---|-------|---------|
| 44 | **Jira** | Create/update issues, sync with development workflows. For engineering and IT teams. |
| 45 | **Generic Ticketing (Zendesk, Freshdesk, ServiceNow)** | Customer support ticket management — create, update, escalate, close. |
| 46 | **Todoist / Asana / Trello** | Lightweight task management for non-engineering teams (marketing campaigns, trade show planning). |

## 11 — Authentication & Security

| # | Block | Purpose |
|---|-------|---------|
| 47 | **OAuth2 / API Key Manager** | Centralized credential store for all connected services. Handles token refresh, rotation, scoping. |
| 48 | **LDAP / Active Directory** | Look up users, validate roles, sync groups. For workflows that need to check who can approve what. |

## 12 — Reporting & Observability

| # | Block | Purpose |
|---|-------|---------|
| 49 | **Dashboard / KPI Widget** | Display live workflow metrics — runs per day, error rate, average processing time. |
| 50 | **Audit Log** | Immutable record of every workflow run, every data transformation, every approval. Required for ISO 9001 and regulated industries (pharma, food). |
| 51 | **Alerting (PagerDuty / Opsgenie / Email)** | Escalate critical failures to on-call staff — SAP sync broken, order stuck, sensor offline. |

---

## Priority Tiers for Implementation

**Tier 1 — Build first (can't run without these):**
Cron, Webhook, REST API, Email Send/Receive, Conditional, Loop, Data Mapper,
Code Block, Error Handler, Audit Log, OAuth2 Manager

**Tier 2 — High value (cover 80% of real workflows):**
SAP Connector, SQL Database, SharePoint, Microsoft Teams, Excel/CSV Parser,
PDF Generator, FTP/SFTP, RSS Reader, Web Scraper, LLM, Wait/Approval,
Alerting

**Tier 3 — Important but can wait:**
Everything else — CRM connectors, Kafka, Vector Search, OCR, Translation,
Social Media, SMS, Ticketing, Dashboard
