"""Point-AOI Parquet output for normalized observation artifacts."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from sentinel_py.cache import write_parquet_atomic
from sentinel_py.enums import ArtifactKind, OutputFormat, WriteStatus
from sentinel_py.pipeline.artifacts import PointSampleArtifact
from sentinel_py.pipeline.writers.base import BaseOutputWriter, OutputWritePlan


@dataclass(frozen=True)
class ParquetWriteResult:
    """Result returned by a point sample Parquet write task."""

    task_id: str
    output_path: Path
    status: str
    error: str | None = None


def _write_rows(rows: pd.DataFrame, *, output_path: Path) -> ParquetWriteResult:
    """Write normalized, already-sampled point rows atomically."""
    try:
        write_parquet_atomic(rows, output_path)
        return ParquetWriteResult("points", output_path, WriteStatus.written)
    except Exception as error:
        return ParquetWriteResult(
            "points",
            output_path,
            WriteStatus.failed,
            f"{type(error).__name__}: {error}",
        )


class ParquetOutputWriter(BaseOutputWriter):
    """Materialize one normalized point artifact as an atomic Parquet table."""

    format_name = OutputFormat.parquet
    accepted_artifact_kinds = frozenset({ArtifactKind.point})

    def build(self, artifact: Any, output: Any, pipeline: Any) -> OutputWritePlan:
        """Plan one Dask task that writes ``samples.parquet`` for a point artifact."""
        from dask import delayed

        if not isinstance(artifact, PointSampleArtifact):
            raise ValueError("Parquet output requires a PointSampleArtifact")
        return OutputWritePlan(
            output,
            (
                delayed(_write_rows, pure=False)(
                    artifact.rows, output_path=output.path / "samples.parquet"
                ),
            ),
        )

    def finalize(self, plan: OutputWritePlan, computed: tuple[Any, ...]) -> tuple[Any, ...]:
        """Return completed point write results."""
        del plan
        return tuple(computed)
