"""Output-writer contracts and registration, separate from processors."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable


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
        return any(
            getattr(result, "status", None) == "failed" for result in self.results
        )


@runtime_checkable
class OutputWriter(Protocol):
    """Build delayed materialization tasks and finalize their central state."""

    format_name: str

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
        self.format_name = format_name

    def build(self, artifact: Any, output: Any, pipeline: Any) -> OutputWritePlan:
        del artifact, pipeline
        return OutputWritePlan(output)

    def finalize(
        self,
        plan: OutputWritePlan,
        computed: tuple[Any, ...],
    ) -> tuple[Any, ...]:
        del plan
        return computed


class OutputWriterRegistry:
    """Explicit mapping from output formats to writer implementations."""

    def __init__(self) -> None:
        self._writers: dict[str, OutputWriter] = {}

    def register(self, writer: OutputWriter) -> None:
        format_name = writer.format_name.strip().lower()
        if not format_name:
            raise ValueError("Output writer formats must be non-empty")
        if format_name in self._writers:
            raise ValueError(f"Output writer is already registered: {format_name}")
        self._writers[format_name] = writer

    def get(self, format_name: str) -> OutputWriter:
        try:
            return self._writers[format_name.lower()]
        except KeyError as error:
            raise KeyError(f"Unknown output format: {format_name}") from error


_DEFAULT_WRITERS: OutputWriterRegistry | None = None


def get_default_writer_registry() -> OutputWriterRegistry:
    """Return the process-wide registry populated with built-in writers."""
    global _DEFAULT_WRITERS
    if _DEFAULT_WRITERS is None:
        from sentinel_py.pipeline.writers.cog import COGOutputWriter

        registry = OutputWriterRegistry()
        registry.register(COGOutputWriter())
        registry.register(ArtifactOutputWriter("xarray"))
        _DEFAULT_WRITERS = registry
    return _DEFAULT_WRITERS
