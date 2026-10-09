"""Processor contracts and registration for declarative pipelines."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import Any, ClassVar, Protocol, cast, runtime_checkable

from pydantic import BaseModel

from sentinel_py.pipeline.artifacts import ArtifactKind, PointSampleArtifact, RasterArtifact


@runtime_checkable
class ProcessorConfig(Protocol):
    """Structural contract implemented by Pydantic processor configurations."""

    def model_dump(self, **kwargs: Any) -> dict[str, Any]:
        """Return the validated configuration as ordinary Python values."""
        ...


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


class BaseProcessor(ABC):
    """Formulaic processor base for raster and point artifact implementations.

    Subclasses declare metadata once and implement both spatial execution hooks.
    The public ``execute`` method is intentionally final in practice: it selects a
    hook from the upstream artifact kind and prevents ad-hoc dispatch in processors.
    """

    type_name: ClassVar[str]
    config_model: ClassVar[type[BaseModel]]
    input_kind: ClassVar[ArtifactKind | None] = None
    output_kind: ClassVar[ArtifactKind] = ArtifactKind.RASTER
    supported_output_formats: ClassVar[frozenset[str]] = frozenset({"cog", "xarray"})

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
        """Validate common writer compatibility before subclass-specific checks."""
        del config, node_id, inputs, sources, output_grid
        unsupported = {output.format for output in outputs} - self.supported_output_formats
        if unsupported:
            raise ValueError(
                f"{self.type_name} does not support output format(s): "
                + ", ".join(sorted(unsupported))
            )

    def execute(
        self,
        config: ProcessorConfig,
        inputs: Mapping[str, Any],
        context: ProcessorContext,
    ) -> Any:
        """Dispatch a node by its AOI mode while preserving one processor API."""
        if self.input_kind is not None:
            invalid = [name for name, artifact in inputs.items()
                       if getattr(artifact, "kind", None) != self.input_kind]
            if invalid:
                raise TypeError(
                    f"{self.type_name} requires {self.input_kind.value} input artifacts; "
                    f"invalid input(s): {', '.join(sorted(invalid))}"
                )
        plan = getattr(context.pipeline, "aoi_plan", None)
        point_mode = plan is not None and plan.kind == "point"
        result = (
            self.execute_points(config, inputs, context)
            if point_mode else self.execute_raster(config, inputs, context)
        )
        expected_type = PointSampleArtifact if point_mode else RasterArtifact
        # Internal raster helpers may recursively transform one xarray dataset;
        # only public pipeline artifacts participate in this contract check.
        if hasattr(result, "kind") and not isinstance(result, expected_type):
            raise TypeError(
                f"{self.type_name} returned {type(result).__name__}; expected "
                f"{expected_type.__name__}"
            )
        return result

    @abstractmethod
    def execute_raster(
        self, config: ProcessorConfig, inputs: Mapping[str, Any], context: ProcessorContext
    ) -> Any:
        """Build the raster-AOI artifact for this processor."""

    @abstractmethod
    def execute_points(
        self, config: ProcessorConfig, inputs: Mapping[str, Any], context: ProcessorContext
    ) -> Any:
        """Build the point-AOI artifact for this processor."""


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


def get_default_registry() -> ProcessorRegistry:
    """Return the process-wide registry populated with built-in processors."""
    from sentinel_py.pipeline.processors import register_builtin_processors

    registry = ProcessorRegistry()
    register_builtin_processors(cast(Any, registry))
    _DEFAULT_REGISTRY = registry
    return _DEFAULT_REGISTRY
