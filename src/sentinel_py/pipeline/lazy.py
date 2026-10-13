"""Shared lazy raster window and reprojection primitives.

These helpers are source-agnostic: they know about native rasters, the canonical
output grid, and its chunk windows, but nothing about Sentinel-2 or DEM products.
Callers own raster I/O locking (:data:`sentinel_py.raster_io.RASTERIO_LOCK`).
"""

from __future__ import annotations

import math
from typing import Any

import numpy as np
from affine import Affine

from sentinel_py.pipeline.grid import GridSpec, GridWindow

########################################################################################
# Window geometry
########################################################################################


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


########################################################################################
# Reprojection
########################################################################################


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
