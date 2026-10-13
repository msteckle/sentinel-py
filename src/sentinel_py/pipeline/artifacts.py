"""Typed artifacts exchanged between pipeline readers, processors, and writers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

import pandas as pd

from sentinel_py.enums import ArtifactKind

@dataclass(frozen=True)
class RasterArtifact:
    """Per-AOI lazy xarray datasets with their AOI planning metadata."""

    plan: Any
    datasets: Mapping[tuple[str, str], Any]

    kind: ArtifactKind = ArtifactKind.raster

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
    kind: ArtifactKind = ArtifactKind.point

