# Plumber

## Documentation

The docs are the source of truth. **Always keep them up to date** — when you change behavior, update the relevant doc in the same change.

- [Architecture](docs/architecture.md) — deployment modes, ARQ + Redis dispatch, MongoDB collections, job lifecycle, worker config
- [Building Blocks](docs/building-blocks.md) — 51 core node types organized by category with implementation priority tiers
- [Coding Principles](docs/coding-principles.md) — type safety, testing, node architecture, standalone usability, sockets, piping, backward compatibility, extensibility & node discovery, no-code/visual editor parity

## Quick Reference

- **Stack:** Python 3.13, FastAPI, Pydantic v2, MongoDB (motor), Redis + ARQ, structlog
- **Package:** `llming_plumber/` (import as `import llming_plumber`)
- **Tests:** `pytest --cov=llming_plumber --cov-fail-under=99 -n auto`
- **Types:** `mypy llming_plumber/ --strict`
- **Lint:** `ruff check llming_plumber/ tests/`
- **Run:** `llming-plumber serve --mode=all|ui|worker`
