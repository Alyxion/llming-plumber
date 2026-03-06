# Plumber

## Naming

Plumber uses themed naming. See the glossary in [Data Piping](docs/data-piping.md) for the full mapping.

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

- [Architecture](docs/architecture.md) — deployment modes, ARQ + Redis dispatch, MongoDB collections, run lifecycle, lemming config
- [Building Blocks](docs/building-blocks.md) — 67 core block types organized by category with implementation priority tiers
- [Coding Principles](docs/coding-principles.md) — async everywhere, type safety, testing, block architecture, standalone usability, MCP tool generation, backward compatibility, extensibility & block discovery, no-code/visual editor parity, i18n
- [Data Piping](docs/data-piping.md) — fitting types, type compatibility, coercions, pipe definitions, piping patterns, runtime execution

## Quick Reference

- **Stack:** Python 3.13, FastAPI, Pydantic v2, MongoDB (motor), Redis + ARQ, structlog
- **Package:** `llming_plumber/` (import as `import llming_plumber`)
- **Tests:** `pytest --cov=llming_plumber --cov-fail-under=99 -n auto`
- **Types:** `mypy llming_plumber/ --strict`
- **Lint:** `ruff check llming_plumber/ tests/`
- **Run:** `llming-plumber serve --mode=all|ui|worker`
