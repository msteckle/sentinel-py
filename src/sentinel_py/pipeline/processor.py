"""Processor contracts and registration for declarative pipelines."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Mapping, Protocol, runtime_checkable

from pydantic import BaseModel


@runtime_checkable
class ProcessorConfig(Protocol):
    """Structural contract implemented by Pydantic processor configurations."""

    def model_dump(self, **kwargs: Any) -> dict[str, Any]:
        """Return the validated configuration as ordinary Python values."""


@dataclass(frozen=True)
class ProcessorContext:
    """Pipeline services and configuration supplied while executing one node."""

    pipeline: Any
    node: Any
    outputs: tuple[Any, ...]
    logger: logging.Logger


@runtime_checkable
class Processor(Protocol):
    """Runtime contract for one registered semantic processing operation."""

    type_name: str
    config_model: type[BaseModel]

    def validate(
        self,
        config: ProcessorConfig,
        *,
        node_id: str,
        inputs: Mapping[str, str],
        sources: Mapping[str, Any],
        outputs: tuple[Any, ...],
        output_grid: Any,
    ) -> None:
        """Validate references and constraints outside the Pydantic model."""

    def execute(
        self,
        config: ProcessorConfig,
        inputs: Mapping[str, Any],
        context: ProcessorContext,
    ) -> Any:
        """Execute the node and return its artifact for downstream nodes."""


class ProcessorRegistry:
    """Explicit mapping from YAML node type names to processor implementations."""

    def __init__(self) -> None:
        self._processors: dict[str, Processor] = {}

    def register(self, processor: Processor) -> None:
        """Register a processor, rejecting accidental type-name replacement."""
        type_name = processor.type_name.strip()
        if not type_name:
            raise ValueError("Processor type names must be non-empty")
        if type_name in self._processors:
            raise ValueError(f"Processor type is already registered: {type_name}")
        self._processors[type_name] = processor

    def get(self, type_name: str) -> Processor:
        """Return a registered processor or raise a concise lookup error."""
        try:
            return self._processors[type_name]
        except KeyError as error:
            raise KeyError(f"Unknown processor type: {type_name}") from error

    @property
    def type_names(self) -> tuple[str, ...]:
        """Return registered type names in deterministic order."""
        return tuple(sorted(self._processors))


_DEFAULT_REGISTRY: ProcessorRegistry | None = None


def get_default_registry() -> ProcessorRegistry:
    """Return the process-wide registry populated with built-in processors."""
    global _DEFAULT_REGISTRY
    if _DEFAULT_REGISTRY is None:
        from sentinel_py.pipeline.processors import register_builtin_processors

        registry = ProcessorRegistry()
        register_builtin_processors(registry)
        _DEFAULT_REGISTRY = registry
    return _DEFAULT_REGISTRY
