"""Typed artifacts exchanged between pipeline readers, processors, and writers."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Mapping

import pandas as pd


class ArtifactKind(StrEnum):
    """Materialization shape exposed by a pipeline node."""

    RASTER = "raster"
    POINT = "point"


@dataclass(frozen=True)
class RasterArtifact:
    """Per-AOI lazy xarray datasets with their AOI planning metadata."""

    plan: Any
    datasets: Mapping[tuple[str, str], Any]

    kind: ArtifactKind = ArtifactKind.RASTER

    def map_datasets(self, function: Any) -> "RasterArtifact":
        """Apply a lazy transformation to every independently planned raster."""
        return RasterArtifact(
            self.plan,
            {key: function(dataset, self.plan.by_key[key]) for key, dataset in self.datasets.items()},
        )


@dataclass(frozen=True)
class PointSampleArtifact:
    """Normalized, source-observation point rows and pipeline provenance."""

    rows: pd.DataFrame
    plan: Any
    kind: ArtifactKind = ArtifactKind.POINT

