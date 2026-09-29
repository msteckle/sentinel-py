"""Canonical output-grid geometry and chunk-window planning."""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import geopandas as gpd
from affine import Affine
from pyproj import CRS, Transformer


class GridPlanningError(ValueError):
    """Raised when an AOI cannot define a valid canonical output grid."""


@dataclass(frozen=True)
class ChunkShape:
    """Maximum spatial shape of one Dask array chunk."""

    y: int
    x: int


@dataclass(frozen=True)
class GridWindow:
    """One row-major spatial chunk within a canonical grid."""

    row_index: int
    column_index: int
    row_offset: int
    column_offset: int
    height: int
    width: int
    bounds: tuple[float, float, float, float]


@dataclass(frozen=True)
class GridSpec:
    """Complete pixel geometry and chunk plan for a raster output."""

    crs: str
    resolution: tuple[float, float]
    affine: Affine
    bounds: tuple[float, float, float, float]
    width: int
    height: int
    chunks: ChunkShape

    @property
    def shape(self) -> tuple[int, int]:
        """Return raster shape in array order: rows, columns."""
        return self.height, self.width

    @property
    def chunk_grid_shape(self) -> tuple[int, int]:
        """Return the number of chunk rows and columns."""
        return (
            math.ceil(self.height / self.chunks.y),
            math.ceil(self.width / self.chunks.x),
        )

    def iter_windows(self) -> Iterator[GridWindow]:
        """Yield complete and edge chunk windows in deterministic row-major order."""
        xmin, _, _, ymax = self.bounds
        x_resolution, y_resolution = self.resolution
        chunk_rows, chunk_columns = self.chunk_grid_shape
        for row_index in range(chunk_rows):
            row_offset = row_index * self.chunks.y
            height = min(self.chunks.y, self.height - row_offset)
            window_ymax = ymax - row_offset * y_resolution
            window_ymin = window_ymax - height * y_resolution
            for column_index in range(chunk_columns):
                column_offset = column_index * self.chunks.x
                width = min(self.chunks.x, self.width - column_offset)
                window_xmin = xmin + column_offset * x_resolution
                window_xmax = window_xmin + width * x_resolution
                yield GridWindow(
                    row_index=row_index,
                    column_index=column_index,
                    row_offset=row_offset,
                    column_offset=column_offset,
                    height=height,
                    width=width,
                    bounds=(window_xmin, window_ymin, window_xmax, window_ymax),
                )


def _aligned_floor(value: float, anchor: float, resolution: float) -> float:
    pixel = (value - anchor) / resolution
    return anchor + math.floor(pixel + 1e-9) * resolution


def _aligned_ceil(value: float, anchor: float, resolution: float) -> float:
    pixel = (value - anchor) / resolution
    return anchor + math.ceil(pixel - 1e-9) * resolution


def plan_aoi_grid(
    aoi: Path,
    *,
    crs: str,
    resolution: tuple[float, float],
    anchor: tuple[float, float],
    chunks: ChunkShape,
) -> GridSpec:
    """Transform an AOI and snap its bounds outward to a shared pixel lattice."""
    try:
        target_crs = CRS.from_user_input(crs)
    except Exception as error:
        raise GridPlanningError(f"Invalid output grid CRS {crs!r}: {error}") from error

    try:
        aoi_frame = gpd.read_file(aoi)
    except Exception as error:
        raise GridPlanningError(f"Could not read selection AOI {aoi}: {error}") from error
    if aoi_frame.empty:
        raise GridPlanningError(f"Selection AOI contains no geometry: {aoi}")
    if aoi_frame.crs is None:
        raise GridPlanningError(f"Selection AOI has no CRS metadata: {aoi}")
    valid_geometry = aoi_frame.geometry.notna() & ~aoi_frame.geometry.is_empty
    if not valid_geometry.any():
        raise GridPlanningError(f"Selection AOI contains no geometry: {aoi}")
    source_bounds = tuple(
        float(value) for value in aoi_frame.loc[valid_geometry].total_bounds
    )
    if not all(math.isfinite(value) for value in source_bounds):
        raise GridPlanningError("Selection AOI bounds must be finite")
    try:
        transformer = Transformer.from_crs(
            aoi_frame.crs, target_crs, always_xy=True
        )
        xmin, ymin, xmax, ymax = transformer.transform_bounds(
            *source_bounds,
            densify_pts=21,
            errcheck=True,
        )
    except Exception as error:
        raise GridPlanningError(
            f"Could not transform selection AOI to {target_crs.to_string()}: {error}"
        ) from error
    xmin, ymin, xmax, ymax = (float(value) for value in (xmin, ymin, xmax, ymax))
    if not all(math.isfinite(value) for value in (xmin, ymin, xmax, ymax)):
        raise GridPlanningError("Transformed AOI bounds must be finite")
    if xmin >= xmax or ymin >= ymax:
        raise GridPlanningError("Transformed AOI must have a non-zero area")

    x_resolution, y_resolution = resolution
    anchor_x, anchor_y = anchor
    aligned_xmin = _aligned_floor(xmin, anchor_x, x_resolution)
    aligned_ymin = _aligned_floor(ymin, anchor_y, y_resolution)
    aligned_xmax = _aligned_ceil(xmax, anchor_x, x_resolution)
    aligned_ymax = _aligned_ceil(ymax, anchor_y, y_resolution)
    width = round((aligned_xmax - aligned_xmin) / x_resolution)
    height = round((aligned_ymax - aligned_ymin) / y_resolution)
    if width < 1 or height < 1:
        raise GridPlanningError("Aligned output grid must contain at least one pixel")
    bounds = (aligned_xmin, aligned_ymin, aligned_xmax, aligned_ymax)
    affine = Affine(
        x_resolution,
        0.0,
        aligned_xmin,
        0.0,
        -y_resolution,
        aligned_ymax,
    )
    return GridSpec(
        crs=target_crs.to_string(),
        resolution=resolution,
        affine=affine,
        bounds=bounds,
        width=width,
        height=height,
        chunks=chunks,
    )
