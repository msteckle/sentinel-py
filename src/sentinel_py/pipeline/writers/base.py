"""Output-writer contracts and registration, separate from processors."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from sentinel_py.enums import ArtifactKind, OutputFormat
from sentinel_py.pipeline.base import Registry


@dataclass(frozen=True)
class OutputWritePlan:
    """Delayed tasks and resumable results prepared for one named output."""

    output: Any
    tasks: tuple[Any, ...] = ()
    completed: tuple[Any, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class OutputExecutionResult:
    """Centralized result summary for one named output."""

    output_id: str
    output_format: str
    path: Any
    results: tuple[Any, ...]

    @property
    def failed(self) -> bool:
        """Return whether any task result reports ``status == "failed"``."""
        return any(
            getattr(result, "status", None) == "failed" for result in self.results
        )


@runtime_checkable
class OutputWriter(Protocol):
    """Build delayed materialization tasks and finalize their central state."""

    format_name: str
    accepted_artifact_kinds: frozenset[ArtifactKind]

    def build(self, artifact: Any, output: Any, pipeline: Any) -> OutputWritePlan:
        """Return delayed tasks without computing them."""

    def finalize(
        self,
        plan: OutputWritePlan,
        computed: tuple[Any, ...],
    ) -> tuple[Any, ...]:
        """Persist centralized state and return every structured result."""


class ArtifactOutputWriter:
    """Expose an artifact without requesting Dask computation or persistence."""

    def __init__(self, format_name: str):
        """Create a pass-through writer registered under ``format_name``."""
        self.format_name = format_name
        self.accepted_artifact_kinds = frozenset({ArtifactKind.raster, ArtifactKind.point})

    def build(self, artifact: Any, output: Any, pipeline: Any) -> OutputWritePlan:
        """Return an empty plan; the lazy artifact is handed to the caller as is."""
        del artifact, pipeline
        return OutputWritePlan(output)

    def finalize(
        self,
        plan: OutputWritePlan,
        computed: tuple[Any, ...],
    ) -> tuple[Any, ...]:
        """Return the (empty) computed results unchanged."""
        del plan
        return computed


class OutputWriterRegistry(Registry[OutputWriter]):
    """Explicit mapping from output formats to writer implementations."""

    kind = "output format"

    @staticmethod
    def key_of(item: OutputWriter) -> str:
        """Return the output ``format`` handled by a writer."""
        return item.format_name

    @staticmethod
    def normalize(name: str) -> str:
        """Make output-format lookups case-insensitive."""
        return name.strip().lower()


_DEFAULT_WRITERS: OutputWriterRegistry | None = None


def get_default_writer_registry() -> OutputWriterRegistry:
    """Return the process-wide registry populated with built-in writers."""
    global _DEFAULT_WRITERS
    if _DEFAULT_WRITERS is None:
        from sentinel_py.pipeline.writers.cog import COGOutputWriter
        from sentinel_py.pipeline.writers.parquet import ParquetOutputWriter

        registry = OutputWriterRegistry()
        registry.register(COGOutputWriter())
        registry.register(ParquetOutputWriter())
        registry.register(ArtifactOutputWriter(OutputFormat.xarray))
        _DEFAULT_WRITERS = registry
    return _DEFAULT_WRITERS
