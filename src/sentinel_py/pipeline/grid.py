"""Canonical output grids, AOI planning, and lazy raster window primitives.

This module is the geometry core of the pipeline:

* ``Grid planning`` turns AOI geometry into a canonical :class:`GridSpec` and chunk
  windows.
* ``AOI planning`` splits an AOI file into independently planned feature units.
* ``Lazy raster primitives`` are source-agnostic native-window and reprojection
  helpers. Callers own raster I/O locking via :data:`RASTERIO_LOCK`.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from typing import Any, Iterator

import geopandas as gpd
import numpy as np
from affine import Affine
from pyproj import CRS, Transformer
from shapely.geometry.base import BaseGeometry

from sentinel_py.enums import AOIKind

########################################################################################
# Grid planning
########################################################################################


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


def plan_geometry_grid(
    geometry: BaseGeometry,
    geometry_crs: str,
    *,
    crs: str,
    resolution: tuple[float, float],
    anchor: tuple[float, float],
    chunks: ChunkShape,
) -> GridSpec:
    """Transform one AOI geometry and snap its bounds to a shared pixel lattice."""
    try:
        target_crs = CRS.from_user_input(crs)
    except Exception as error:
        raise GridPlanningError(f"Invalid output grid CRS {crs!r}: {error}") from error

    if geometry is None or geometry.is_empty:
        raise GridPlanningError("AOI geometry must be non-empty")
    source_bounds = tuple(float(value) for value in geometry.bounds)
    if not all(math.isfinite(value) for value in source_bounds):
        raise GridPlanningError("Selection AOI bounds must be finite")
    try:
        transformer = Transformer.from_crs(
            geometry_crs, target_crs, always_xy=True
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


def plan_aoi_grid(
    aoi: Path,
    *,
    crs: str,
    resolution: tuple[float, float],
    anchor: tuple[float, float],
    chunks: ChunkShape,
) -> GridSpec:
    """Backward-compatible helper for planning one single-feature AOI file.

    Pipeline execution plans each feature/component with ``plan_geometry_grid``.
    This helper remains for external callers that have a one-feature file.
    """
    try:
        frame = gpd.read_file(aoi)
    except Exception as error:
        raise GridPlanningError(f"Could not read selection AOI {aoi}: {error}") from error
    if frame.empty or frame.crs is None:
        raise GridPlanningError(f"Selection AOI contains no geometry or CRS: {aoi}")
    geometries = frame.geometry.dropna()
    geometries = geometries[~geometries.is_empty]
    if len(geometries) != 1:
        raise GridPlanningError("plan_aoi_grid requires exactly one non-empty geometry")
    return plan_geometry_grid(
        geometries.iloc[0], str(frame.crs), crs=crs, resolution=resolution,
        anchor=anchor, chunks=chunks,
    )


########################################################################################
# AOI planning
########################################################################################


@dataclass(frozen=True)
class AOIUnit:
    """One independently planned polygon component or point group."""

    aoi_id: str
    component_id: str
    kind: AOIKind
    geometry: BaseGeometry
    geometry_wgs84: BaseGeometry
    grid: GridSpec | None


@dataclass(frozen=True)
class AOIPlan:
    """Validated, homogeneous AOI units keyed by stable feature identity."""

    kind: AOIKind
    units: tuple[AOIUnit, ...]

    @property
    def by_key(self) -> Mapping[tuple[str, str], AOIUnit]:
        """Return units indexed by their stable output key."""
        return {(unit.aoi_id, unit.component_id): unit for unit in self.units}


def _parts(geometry: BaseGeometry) -> list[BaseGeometry]:
    """Return non-empty atomic parts while retaining the parent feature identity."""
    if geometry.geom_type.startswith("Multi") or geometry.geom_type == "GeometryCollection":
        result: list[BaseGeometry] = []
        for part in geometry.geoms:
            result.extend(_parts(part))
        return result
    return [] if geometry.is_empty else [geometry]


def load_aoi_plan(
    path: Path,
    *,
    id_field: str,
    crs: str,
    resolution: tuple[float, float],
    anchor: tuple[float, float],
    chunks: ChunkShape,
) -> AOIPlan:
    """Read and validate an AOI file into independently planned feature units."""
    frame = gpd.read_file(path)
    if frame.empty:
        raise ValueError("selection.aoi contains no features")
    if frame.crs is None:
        raise ValueError("selection.aoi must have CRS metadata")
    if id_field not in frame.columns:
        raise ValueError(f"selection.aoi_id_field {id_field!r} is not an AOI field")
    ids = frame[id_field]
    if ids.isna().any() or any(not str(value).strip() for value in ids):
        raise ValueError("selection.aoi_id_field values must be non-empty")
    identifiers = tuple(str(value).strip() for value in ids)
    if len(set(identifiers)) != len(identifiers):
        raise ValueError("selection.aoi_id_field values must be unique")

    kinds: set[str] = set()
    for geometry in frame.geometry:
        if geometry is None or geometry.is_empty:
            raise ValueError("selection.aoi cannot contain empty geometries")
        parts = _parts(geometry)
        part_kinds = {
            AOIKind.point
            if part.geom_type == "Point"
            else AOIKind.polygon
            if part.geom_type == "Polygon"
            else "unsupported"
            for part in parts
        }
        kinds.update(part_kinds)
    if "unsupported" in kinds or len(kinds) != 1:
        raise ValueError("selection.aoi must contain only points or only polygons")
    kind = next(iter(kinds))
    wgs84 = frame.to_crs("EPSG:4326")
    target = frame.to_crs(crs)
    units: list[AOIUnit] = []
    for feature_index, aoi_id in enumerate(identifiers):
        original_parts = _parts(target.geometry.iloc[feature_index])
        wgs_parts = _parts(wgs84.geometry.iloc[feature_index])
        for part_index, (geometry, geometry_wgs84) in enumerate(zip(original_parts, wgs_parts)):
            component_id = f"part-{part_index:04d}"
            grid = (
                plan_geometry_grid(
                    geometry, crs, crs=crs, resolution=resolution,
                    anchor=anchor, chunks=chunks,
                )
                if kind == AOIKind.polygon
                else None
            )
            units.append(AOIUnit(aoi_id, component_id, kind, geometry, geometry_wgs84, grid))
    return AOIPlan(kind, tuple(units))


def point_pixel(unit: AOIUnit, *, crs: str, resolution: tuple[float, float], anchor: tuple[float, float]) -> tuple[float, float]:
    """Return the aligned output pixel centre containing a point AOI."""
    transformer = Transformer.from_crs("EPSG:4326", crs, always_xy=True)
    x, y = transformer.transform(unit.geometry_wgs84.x, unit.geometry_wgs84.y)
    pixel_x = np.floor((x - anchor[0]) / resolution[0])
    pixel_y = np.floor((y - anchor[1]) / resolution[1])
    return (
        anchor[0] + (pixel_x + 0.5) * resolution[0],
        anchor[1] + (pixel_y + 0.5) * resolution[1],
    )


def grid_for_unit(unit: AOIUnit, output_grid: Any) -> GridSpec:
    """Return the canonical grid used to process one AOI unit.

    Parameters
    ----------
    unit : AOIUnit
        Planned AOI unit.
    output_grid : OutputGridConfig
        Pipeline output-grid request supplying ``crs``, ``resolution``, ``anchor``,
        and ``chunks`` (kept untyped here to avoid importing the config module).

    Returns
    -------
    GridSpec
        The unit's planned polygon grid, or the one-pixel grid of a point unit.
    """
    return unit.grid or point_grid(
        unit,
        crs=output_grid.crs,
        resolution=output_grid.resolution,
        anchor=output_grid.anchor,
        chunks=output_grid.chunks,
    )


def point_grid(unit: AOIUnit, *, crs: str, resolution: tuple[float, float], anchor: tuple[float, float], chunks: ChunkShape) -> GridSpec:
    """Build the one-pixel canonical grid used to sample a point AOI."""
    centre_x, centre_y = point_pixel(unit, crs=crs, resolution=resolution, anchor=anchor)
    xmin = centre_x - resolution[0] / 2
    ymax = centre_y + resolution[1] / 2
    return GridSpec(
        crs=crs, resolution=resolution,
        affine=Affine(resolution[0], 0.0, xmin, 0.0, -resolution[1], ymax),
        bounds=(xmin, ymax - resolution[1], xmin + resolution[0], ymax),
        width=1, height=1, chunks=ChunkShape(y=1, x=1),
    )


########################################################################################
# Lazy raster primitives
########################################################################################

# Some GDAL raster drivers use mutable process-global or dataset-adjacent state.
# Different Dask worker processes may run raster work concurrently, while threads
# within one worker serialize native reads, reprojection, and writes through this lock.
# Reference it as ``pipeline_grid.RASTERIO_LOCK`` (module attribute) rather than importing
# the name: Dask may pickle closures by value and a lock itself cannot be pickled.
RASTERIO_LOCK = RLock()


# Window geometry ----------------------------------------------------------------------


def window_transform(grid: GridSpec, window: GridWindow) -> Affine:
    """Return the affine transform of one canonical-grid output window.

    Parameters
    ----------
    grid : GridSpec
        Canonical output grid supplying the pixel size.
    window : GridWindow
        Output window whose upper-left corner is the transform origin.

    Returns
    -------
    affine.Affine
        North-up transform of the window in the grid CRS.
    """
    xmin, _, _, ymax = window.bounds
    return Affine(grid.resolution[0], 0.0, xmin, 0.0, -grid.resolution[1], ymax)


def native_pixel_window(
    dataset: Any,
    target_bounds: tuple[float, float, float, float],
    target_crs: str,
    *,
    padding: int = 3,
) -> tuple[int, int, int, int] | None:
    """Find the native-pixel envelope needed for one output window.

    Parameters
    ----------
    dataset : rasterio.io.DatasetReader
        Open north-up source raster with a CRS.
    target_bounds : tuple[float, float, float, float]
        Output window bounds ``(xmin, ymin, xmax, ymax)`` in ``target_crs``.
    target_crs : str
        CRS of ``target_bounds``.
    padding : int, default 3
        Native pixels added on every side to cover resampling support.

    Returns
    -------
    tuple[int, int, int, int] | None
        ``(x_offset, y_offset, width, height)`` clipped to the raster, or
        ``None`` when the window does not intersect the raster.
    """
    from rasterio.warp import transform_bounds

    source_transform = dataset.transform
    if source_transform.b != 0 or source_transform.d != 0:
        raise ValueError("Lazy raster reads require north-up source rasters")
    if dataset.crs is None:
        raise ValueError(f"Raster source has no CRS: {dataset.name}")
    source_bounds = transform_bounds(
        target_crs, dataset.crs, *target_bounds, densify_pts=21
    )
    inverse = ~source_transform
    xmin, ymin, xmax, ymax = source_bounds
    pixels = [
        inverse @ (x, y)
        for x, y in ((xmin, ymin), (xmin, ymax), (xmax, ymin), (xmax, ymax))
    ]
    x_offset = max(0, math.floor(min(point[0] for point in pixels)) - padding)
    y_offset = max(0, math.floor(min(point[1] for point in pixels)) - padding)
    x_end = min(dataset.width, math.ceil(max(point[0] for point in pixels)) + padding)
    y_end = min(dataset.height, math.ceil(max(point[1] for point in pixels)) + padding)
    if x_end <= x_offset or y_end <= y_offset:
        return None
    return x_offset, y_offset, x_end - x_offset, y_end - y_offset


# Reprojection -------------------------------------------------------------------------


def select_resampling(source_resolution: float, grid: GridSpec) -> str:
    """Pick a resampling method from the native-to-grid resolution ratio.

    Parameters
    ----------
    source_resolution : float
        Native pixel size in the units of the grid CRS.
    grid : GridSpec
        Canonical output grid.

    Returns
    -------
    str
        ``"nearest"`` for equal resolutions, ``"average"`` when aggregating to a
        coarser grid, and ``"bilinear"`` when interpolating to a finer grid.
    """
    target_resolution = min(grid.resolution)
    if math.isclose(source_resolution, target_resolution, abs_tol=1e-9):
        return "nearest"
    return "average" if source_resolution < target_resolution else "bilinear"


def warp_to_window(
    source: np.ndarray | None,
    source_transform: Affine | None,
    source_crs: Any | None,
    grid: GridSpec,
    window: GridWindow,
    *,
    resampling: str,
    src_nodata: float | None,
    dst_nodata: float | None,
    dtype: np.dtype | type = np.uint16,
) -> np.ndarray:
    """Reproject one native array onto its exact canonical-grid window.

    Parameters
    ----------
    source : numpy.ndarray | None
        Two-dimensional native array. ``None`` yields a window filled with
        ``dst_nodata`` (or zero when it is ``None``), for sources that do not
        intersect the window.
    source_transform : affine.Affine | None
        Transform of ``source``; required when ``source`` is provided.
    source_crs : object | None
        CRS of ``source``; required when ``source`` is provided.
    grid : GridSpec
        Canonical output grid supplying CRS and pixel size.
    window : GridWindow
        Output window to produce.
    resampling : str
        Name of a :class:`rasterio.enums.Resampling` member.
    src_nodata, dst_nodata : float | None
        Nodata values of the source and of the output.
    dtype : numpy.dtype | type, default numpy.uint16
        Output dtype.

    Returns
    -------
    numpy.ndarray
        Array of shape ``(window.height, window.width)``.
    """
    fill = 0 if dst_nodata is None else dst_nodata
    destination = np.full((window.height, window.width), fill, dtype=dtype)
    if source is None:
        return destination
    if source_transform is None or source_crs is None:
        raise ValueError("Source transform and CRS are required for reprojection")

    from rasterio.enums import Resampling
    from rasterio.warp import reproject

    reproject(
        source,
        destination,
        src_transform=source_transform,
        src_crs=source_crs,
        dst_transform=window_transform(grid, window),
        dst_crs=grid.crs,
        src_nodata=src_nodata,
        dst_nodata=dst_nodata,
        resampling=getattr(Resampling, resampling),
        num_threads=1,
        init_dest_nodata=True,
    )
    return destination
