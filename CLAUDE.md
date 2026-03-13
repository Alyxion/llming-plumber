# Plumber

## Naming

Plumber uses themed naming. See the glossary in [Data Piping](llming_plumber/docs/data-piping.md) for the full mapping.

| Plumber Term | Industry Equivalent |
|---|---|
| **Pipeline** | Workflow |
| **Block** | Node |
| **Fitting** | Socket/Port |
| **Pipe** | Edge/Wire |
| **Parcel** | Item/Message |
| **Lemming** | Worker |
| **Run** | Job/Execution |
| **Catalog** | Registry |

## Documentation

The docs are the source of truth. **Always keep them up to date** — when you change behavior, update the relevant doc in the same change.

- [Architecture](llming_plumber/docs/architecture.md) — deployment modes, ARQ + Redis dispatch, MongoDB collections, run lifecycle, lemming config
- [Building Blocks](llming_plumber/docs/building-blocks.md) — index of all 112 blocks with links to per-category reference docs
  - [Core](llming_plumber/docs/blocks-core.md) — triggers, flow control, transformation, cache, variables (30 blocks)
  - [Web](llming_plumber/docs/blocks-web.md) — crawler, scraper, snapshots, change detection (6 blocks)
  - [Documents](llming_plumber/docs/blocks-documents.md) — Excel, PDF, Word, PowerPoint, Parquet, YAML (22 blocks)
  - [LLM](llming_plumber/docs/blocks-llm.md) — chat, summarization, classification, extraction, translation (9 blocks)
  - [Data](llming_plumber/docs/blocks-data.md) — files, archives, Redis, MongoDB (32 blocks)
  - [Azure](llming_plumber/docs/blocks-azure.md) — Blob Storage actions + resource block (6 blocks)
  - [Weather](llming_plumber/docs/blocks-weather.md) — OpenWeatherMap, DWD (2 blocks)
  - [News](llming_plumber/docs/blocks-news.md) — RSS, NewsAPI, Tagesschau (3 blocks)
  - [Government](llming_plumber/docs/blocks-government.md) — Autobahn, NINA, Pegel, Feiertage (4 blocks)
- [File Browser](llming_plumber/docs/file-browser.md) — browse, search, and preview files from sink blocks across runs
- [Coding Principles](llming_plumber/docs/coding-principles.md) — async everywhere, type safety, testing, block architecture, standalone usability, MCP tool generation, backward compatibility, extensibility & block discovery, no-code/visual editor parity, i18n
- [Data Piping](llming_plumber/docs/data-piping.md) — fitting types, type compatibility, coercions, pipe definitions, piping patterns, runtime execution

## Testing

Two test layers are **always required** for every feature:

| Layer | Command | Runs in CI | Purpose |
|---|---|---|---|
| **Unit tests** | `pytest -m "not integration"` | Yes | Mocked, fast, 99%+ coverage |
| **Integration tests** | `pytest -m integration` | No (needs API keys) | Real API calls, verify actual behavior |

- **Unit tests** (`tests/blocks/`, `tests/llm/test_providers.py`) — mock all external I/O (respx, `patch.dict`). Must pass without any API keys or network.
- **Integration tests** (`tests/llm/test_providers_integration.py`) — hit real APIs using keys from `.env`. Every LLM provider must have integration tests covering `invoke`, `ainvoke`, `stream`, `astream`.
- **When adding a new provider or block with external dependencies:** always add both unit tests AND integration tests.

## LLM Providers

Synced from `llming-lodge` via `python llming_plumber/scripts/sync_providers.py`. Do NOT edit files in `llming_plumber/llm/` directly — re-run the sync script instead.

Available providers: `openai`, `azure_openai`, `anthropic`, `google`, `mistral`.

## Quick Reference

- **Stack:** Python 3.13, FastAPI, Pydantic v2, MongoDB (motor), Redis + ARQ, structlog
- **Package:** `llming_plumber/` (import as `import llming_plumber`)
- **Unit tests:** `pytest -m "not integration" --cov=llming_plumber --cov-fail-under=99`
- **Integration tests:** `pytest -m integration -v` (requires `.env` with API keys)
- **Types:** `mypy llming_plumber/ --strict`
- **Lint:** `ruff check llming_plumber/ tests/`
- **Sync providers:** `python llming_plumber/scripts/sync_providers.py`
- **Run:** `llming-plumber serve --mode=all|ui|worker`
