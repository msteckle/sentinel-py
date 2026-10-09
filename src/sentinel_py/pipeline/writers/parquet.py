"""Point-AOI Parquet output for normalized observation artifacts."""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from sentinel_py.pipeline.writers.base import OutputWritePlan
from sentinel_py.pipeline.artifacts import ArtifactKind, PointSampleArtifact


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
        output_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = output_path.with_name(f".{output_path.name}.{uuid.uuid4().hex}.tmp")
        rows.to_parquet(temporary, index=False)
        os.replace(temporary, output_path)
        return ParquetWriteResult("points", output_path, "written")
    except Exception as error:
        return ParquetWriteResult("points", output_path, "failed", f"{type(error).__name__}: {error}")


class ParquetOutputWriter:
    """Materialize one normalized point artifact as an atomic Parquet table."""

    format_name = "parquet"
    accepted_artifact_kinds = frozenset({ArtifactKind.POINT})

    def build(self, artifact: Any, output: Any, pipeline: Any) -> OutputWritePlan:
        from dask import delayed

        if not isinstance(artifact, PointSampleArtifact):
            raise ValueError("Parquet output requires a PointSampleArtifact")
        return OutputWritePlan(
            output,
            (delayed(_write_rows, pure=False)(artifact.rows, output_path=output.path / "samples.parquet"),),
        )

    def finalize(self, plan: OutputWritePlan, computed: tuple[Any, ...]) -> tuple[Any, ...]:
        """Return completed point write results."""
        del plan
        return tuple(computed)
