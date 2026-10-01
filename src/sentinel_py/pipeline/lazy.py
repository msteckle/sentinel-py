"""Shared lazy raster window and reprojection helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
from affine import Affine

from sentinel_py import raster_io
from sentinel_py.pipeline.grid import GridSpec, GridWindow


def read_reprojected_window(
    paths: tuple[Path, ...],
    grid: GridSpec,
    window: GridWindow,
    *,
    band: int = 1,
    resampling: str = "bilinear",
    nodata: float = np.nan,
) -> np.ndarray:
    """Read a DEM mosaic and reproject one output window to the canonical grid."""
    try:
        import rasterio
        from rasterio.enums import Resampling
        from rasterio.merge import merge
        from rasterio.warp import reproject, transform_bounds
    except ImportError as error:
        raise RuntimeError("DEM processing requires rasterio") from error

    with raster_io.RASTERIO_LOCK:
        datasets = [rasterio.open(path) for path in paths]
        try:
            if not datasets:
                return np.full((window.height, window.width), nodata, dtype=np.float32)
            source_crs = datasets[0].crs
            if source_crs is None:
                raise ValueError(f"DEM source has no CRS: {datasets[0].name}")
            source_bounds = transform_bounds(
                grid.crs, source_crs, *window.bounds, densify_pts=21
            )
            source, source_transform = merge(
                datasets,
                bounds=source_bounds,
                indexes=band,
                nodata=datasets[0].nodata,
                masked=True,
            )
            values = source[0].filled(np.nan).astype(np.float32)
            destination = np.full(
                (window.height, window.width), nodata, dtype=np.float32
            )
            destination_transform = Affine(
                grid.resolution[0], 0.0, window.bounds[0],
                0.0, -grid.resolution[1], window.bounds[3],
            )
            reproject(
                values,
                destination,
                src_transform=source_transform,
                src_crs=source_crs,
                dst_transform=destination_transform,
                dst_crs=grid.crs,
                src_nodata=np.nan,
                dst_nodata=nodata,
                resampling=getattr(Resampling, resampling),
                init_dest_nodata=True,
                num_threads=1,
            )
            return destination
        finally:
            for dataset in datasets:
                dataset.close()
