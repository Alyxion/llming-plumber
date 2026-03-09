"""Auto-discovery registry for BaseBlock subclasses."""

from __future__ import annotations

import importlib
import logging
import pkgutil
from typing import Any, ClassVar, get_args

from pydantic import BaseModel

from llming_plumber.blocks.base import BaseBlock, BlockInput, BlockOutput, FittingDescriptor

logger = logging.getLogger(__name__)


class FittingMeta(BaseModel):
    """Fitting descriptor for the block catalog API."""

    uid: str
    label: str
    kind: str
    color: str = ""
    description: str = ""


class BlockMeta(BaseModel):
    """Metadata for a registered block type."""

    block_type: str
    block_kind: str = "action"
    description: str
    icon: str
    categories: list[str]
    cache_ttl: int
    llm_tier: str | None = None
    input_schema: dict[str, Any]
    output_schema: dict[str, Any]
    input_fittings: list[FittingMeta] = []
    output_fittings: list[FittingMeta] = []


def _get_type_args(
    cls: type[BaseBlock],
) -> tuple[type[BlockInput], type[BlockOutput]] | None:
    """Extract InputT and OutputT from a BaseBlock subclass via __orig_bases__."""
    for base in getattr(cls, "__orig_bases__", ()):
        origin = getattr(base, "__origin__", None)
        if origin is None:
            continue
        # Walk MRO to check if origin is BaseBlock or a subclass of it
        if origin is BaseBlock or (
            isinstance(origin, type) and issubclass(origin, BaseBlock)
        ):
            args = get_args(base)
            if len(args) == 2:
                input_t, output_t = args
                if (
                    isinstance(input_t, type)
                    and isinstance(output_t, type)
                    and issubclass(input_t, BlockInput)
                    and issubclass(output_t, BlockOutput)
                ):
                    return input_t, output_t
    return None


def _walk_subclasses(cls: type) -> set[type]:
    """Iteratively collect all subclasses of cls (no recursion limit)."""
    result: set[type] = set()
    stack = list(cls.__subclasses__())
    while stack:
        sub = stack.pop()
        if sub not in result:
            result.add(sub)
            stack.extend(sub.__subclasses__())
    return result


class BlockRegistry:
    """Auto-discovers BaseBlock subclasses. Singleton pattern."""

    _registry: ClassVar[dict[str, type[BaseBlock]]] = {}
    _discovered: ClassVar[bool] = False

    @classmethod
    def discover(cls) -> None:
        """Import all modules under llming_plumber.blocks/ recursively,
        then collect all BaseBlock subclasses by walking __subclasses__() recursively.
        Register each by its block_type ClassVar.
        Also scan importlib.metadata.entry_points(group='llming_plumber.blocks').
        """
        if cls._discovered:
            return

        # 1. Import all modules under llming_plumber.blocks
        import llming_plumber.blocks as blocks_pkg

        for pkg_path in blocks_pkg.__path__:
            for module_info in pkgutil.walk_packages(
                [pkg_path],
                prefix="llming_plumber.blocks.",
            ):
                try:
                    importlib.import_module(module_info.name)
                except Exception:
                    logger.warning(
                        "Failed to import block module %s",
                        module_info.name,
                        exc_info=True,
                    )

        # 2. Scan entry points for third-party blocks
        try:
            from importlib.metadata import entry_points

            eps = entry_points(group="llming_plumber.blocks")
            for ep in eps:
                try:
                    ep.load()
                except Exception:
                    logger.warning(
                        "Failed to load entry point %s",
                        ep.name,
                        exc_info=True,
                    )
        except Exception:
            logger.debug("No entry points found for llming_plumber.blocks")

        # 3. Walk all BaseBlock subclasses and register
        for sub in _walk_subclasses(BaseBlock):
            block_type = getattr(sub, "block_type", None)
            if not block_type or not isinstance(block_type, str):
                continue
            # Skip abstract classes
            if getattr(sub, "__abstractmethods__", frozenset()):
                continue
            if block_type in cls._registry:
                existing = cls._registry[block_type]
                if existing is not sub:
                    logger.warning(
                        "Duplicate block_type %r: %s vs %s — keeping first",
                        block_type,
                        existing.__name__,
                        sub.__name__,
                    )
                continue
            cls._registry[block_type] = sub

        cls._discovered = True

    @classmethod
    def get(cls, block_type: str) -> type[BaseBlock]:
        """Get block class by type. Raises KeyError if not found."""
        if not cls._discovered:
            cls.discover()
        return cls._registry[block_type]

    @classmethod
    def create(cls, block_type: str) -> BaseBlock:
        """Instantiate a block by type."""
        block_cls = cls.get(block_type)
        return block_cls()

    @classmethod
    def catalog(cls) -> list[BlockMeta]:
        """Return metadata for all registered blocks."""
        if not cls._discovered:
            cls.discover()

        result: list[BlockMeta] = []
        for block_type, block_cls in sorted(cls._registry.items()):
            type_args = _get_type_args(block_cls)
            if type_args is None:
                input_schema: dict[str, Any] = {}
                output_schema: dict[str, Any] = {}
            else:
                input_t, output_t = type_args
                input_schema = input_t.model_json_schema()
                output_schema = output_t.model_json_schema()

            # Build fitting metadata
            raw_in: list[FittingDescriptor] = getattr(block_cls, "input_fittings", [])
            raw_out: list[FittingDescriptor] = getattr(block_cls, "output_fittings", [])
            in_fittings = [
                FittingMeta(uid=f.uid, label=f.label, kind="input", color=f.color, description=f.description)
                for f in raw_in
            ]
            out_fittings = [
                FittingMeta(uid=f.uid, label=f.label, kind="output", color=f.color, description=f.description)
                for f in raw_out
            ]

            result.append(
                BlockMeta(
                    block_type=block_type,
                    block_kind=getattr(block_cls, "block_kind", "action"),
                    description=getattr(block_cls, "description", ""),
                    icon=getattr(block_cls, "icon", "tabler/puzzle"),
                    categories=getattr(block_cls, "categories", []),
                    cache_ttl=getattr(block_cls, "cache_ttl", 0),
                    llm_tier=getattr(block_cls, "llm_tier", None),
                    input_schema=input_schema,
                    output_schema=output_schema,
                    input_fittings=in_fittings,
                    output_fittings=out_fittings,
                )
            )
        return result

    @classmethod
    def reset(cls) -> None:
        """Reset for testing."""
        cls._registry.clear()
        cls._discovered = False
