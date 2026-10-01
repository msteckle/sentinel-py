"""Lazy Sentinel-2 reads on a canonical output grid."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import numpy as np
from affine import Affine

from sentinel_py import raster_io
from sentinel_py.pipeline.grid import GridSpec, GridWindow
from sentinel_py.s2.base import S2Granule
from sentinel_py.s2.discover import read_l2a_radiometry, read_l2a_special_values


@dataclass(frozen=True)
class S2SceneSource:
    """Serializable source metadata shared by every chunk of one scene."""

    granule: S2Granule
    bands: tuple[str, ...]
    offsets: dict[str, int]
    special_values: tuple[int, ...]
    quantification_value: int
    mask_classes: tuple[int, ...]
    nodata: int


def _rasterio_modules():
    try:
        import rasterio
        from rasterio.enums import Resampling
        from rasterio.warp import reproject, transform_bounds
        from rasterio.windows import Window
    except ImportError as error:
        raise RuntimeError(
            "Rasterio is required when lazy Sentinel-2 chunks are computed. "
            "Install the processing dependencies."
        ) from error
    return rasterio, Resampling, reproject, transform_bounds, Window


def _source_window(
    dataset,
    target_bounds: tuple[float, float, float, float],
    target_crs: str,
    *,
    padding: int = 3,
) -> tuple[int, int, int, int] | None:
    """Find the native-pixel envelope needed for one output window."""
    _, _, _, transform_bounds, _ = _rasterio_modules()
    source_transform = dataset.transform
    if source_transform.b != 0 or source_transform.d != 0:
        raise ValueError("Lazy S2 reads require north-up source rasters")
    if dataset.crs is None:
        raise ValueError(f"Raster source has no CRS: {dataset.name}")
    source_bounds = transform_bounds(
        target_crs,
        dataset.crs,
        *target_bounds,
        densify_pts=21,
    )
    inverse = ~source_transform
    xmin, ymin, xmax, ymax = source_bounds
    pixels = [
        inverse @ (x, y)
        for x, y in ((xmin, ymin), (xmin, ymax), (xmax, ymin), (xmax, ymax))
    ]
    x_offset = max(0, math.floor(min(point[0] for point in pixels)) - padding)
    y_offset = max(0, math.floor(min(point[1] for point in pixels)) - padding)
    x_end = min(
        dataset.width,
        math.ceil(max(point[0] for point in pixels)) + padding,
    )
    y_end = min(
        dataset.height,
        math.ceil(max(point[1] for point in pixels)) + padding,
    )
    if x_end <= x_offset or y_end <= y_offset:
        return None
    return x_offset, y_offset, x_end - x_offset, y_end - y_offset


def _correct_dn_values(
    values: np.ndarray,
    *,
    offset: int,
    nodata: int,
    invalid_values: tuple[int, ...],
) -> np.ndarray:
    """Mask XML special DNs before applying the signed BOA offset."""
    invalid = np.isin(values, invalid_values)
    corrected = np.maximum(values.astype(np.int32) + offset, 0)
    if corrected.max(initial=0) >= np.iinfo(np.uint16).max:
        raise ValueError("Corrected DN exceeds the UInt16 valid range")
    return np.where(invalid, nodata, corrected).astype(np.uint16)


def _window_dataset(
    path: Path,
    grid: GridSpec,
    window: GridWindow,
    *,
    nodata: int | None,
    offset: int | None,
    invalid_values: tuple[int, ...] = (),
) -> tuple[np.ndarray | None, Affine | None, object | None, float]:
    """Decode one native source window and retain its exact spatial metadata."""
    rasterio, _, _, _, Window = _rasterio_modules()
    with rasterio.open(path) as source:
        source_resolution = abs(float(source.transform.a))
        source_window = _source_window(source, window.bounds, grid.crs)
        if source_window is None:
            return None, None, None, source_resolution
        x_offset, y_offset, width, height = source_window
        raster_window = Window(x_offset, y_offset, width, height)
        values = source.read(1, window=raster_window)
        source_transform = source.window_transform(raster_window)
        source_crs = source.crs
    if offset is not None:
        if nodata is None:
            raise ValueError("A nodata value is required when applying a BOA offset")
        values = _correct_dn_values(
            values,
            offset=offset,
            nodata=nodata,
            invalid_values=invalid_values,
        )
    else:
        values = values.astype(np.uint16, copy=False)
    return values, source_transform, source_crs, source_resolution


def _warp_window(
    source: np.ndarray | None,
    source_transform: Affine | None,
    source_crs: object | None,
    grid: GridSpec,
    window: GridWindow,
    *,
    resampling: str,
    nodata: int | None,
) -> np.ndarray:
    """Reproject one native array onto its exact canonical-grid window."""
    rasterio, Resampling, reproject, _, _ = _rasterio_modules()
    if source is None:
        fill = 0 if nodata is None else nodata
        return np.full((window.height, window.width), fill, dtype=np.uint16)
    if source_transform is None or source_crs is None:
        raise ValueError("Source transform and CRS are required for reprojection")
    fill = 0 if nodata is None else nodata
    destination_values = np.full((window.height, window.width), fill, dtype=np.uint16)
    xmin, _, _, ymax = window.bounds
    destination_transform = Affine(
        grid.resolution[0],
        0.0,
        xmin,
        0.0,
        -grid.resolution[1],
        ymax,
    )
    source_options = {
        "driver": "GTiff",
        "width": source.shape[1],
        "height": source.shape[0],
        "count": 1,
        "dtype": source.dtype,
        "crs": source_crs,
        "transform": source_transform,
        "nodata": nodata,
    }
    destination_options = {
        "driver": "GTiff",
        "width": window.width,
        "height": window.height,
        "count": 1,
        "dtype": destination_values.dtype,
        "crs": grid.crs,
        "transform": destination_transform,
        "nodata": nodata,
    }
    with rasterio.io.MemoryFile() as source_memory:
        with source_memory.open(**source_options) as source_dataset:
            source_dataset.write(source, 1)
            with rasterio.io.MemoryFile() as destination_memory:
                with destination_memory.open(**destination_options) as destination:
                    destination.write(destination_values, 1)
                    reproject(
                        source=rasterio.band(source_dataset, 1),
                        destination=rasterio.band(destination, 1),
                        src_nodata=nodata,
                        dst_nodata=nodata,
                        resampling=getattr(Resampling, resampling),
                        num_threads=1,
                        init_dest_nodata=True,
                    )
                    return destination.read(1)


def _resampling(source_resolution: float, grid: GridSpec) -> str:
    target_resolution = min(grid.resolution)
    if math.isclose(source_resolution, target_resolution, abs_tol=1e-9):
        return "nearest"
    return "average" if source_resolution < target_resolution else "bilinear"


def _read_scene_chunk(
    scene: S2SceneSource,
    grid: GridSpec,
    window: GridWindow,
) -> np.ndarray:
    """Read every band and SCL for one scene/window task."""
    with raster_io.RASTERIO_LOCK:
        return _read_scene_chunk_locked(scene, grid, window)


def _read_scene_chunk_locked(
    scene: S2SceneSource,
    grid: GridSpec,
    window: GridWindow,
) -> np.ndarray:
    """Run one scene/window task while holding the process-local raster lock."""
    if scene.granule.scl_path is None:
        raise FileNotFoundError(f"Missing SCL asset for {scene.granule.granule_id}")
    scl_source, scl_transform, scl_crs, _ = _window_dataset(
        scene.granule.scl_path,
        grid,
        window,
        nodata=None,
        offset=None,
    )
    scl = _warp_window(
        scl_source,
        scl_transform,
        scl_crs,
        grid,
        window,
        resampling="nearest",
        nodata=None,
    )
    mask = (scl == 0) | (scl > 11)
    for value in scene.mask_classes:
        mask |= scl == value

    output = np.empty(
        (len(scene.bands) + 1, window.height, window.width), dtype=np.uint16
    )
    for index, band_name in enumerate(scene.bands):
        try:
            path = scene.granule.band_paths[band_name]
        except KeyError as error:
            raise FileNotFoundError(
                f"Missing {band_name} asset for {scene.granule.granule_id}"
            ) from error
        source, source_transform, source_crs, source_resolution = _window_dataset(
            path,
            grid,
            window,
            nodata=scene.nodata,
            offset=scene.offsets[band_name],
            invalid_values=scene.special_values,
        )
        values = _warp_window(
            source,
            source_transform,
            source_crs,
            grid,
            window,
            resampling=_resampling(source_resolution, grid),
            nodata=scene.nodata,
        )
        output[index] = np.where(
            mask | (values == scene.nodata), scene.nodata, values
        ).astype(np.uint16)
    output[-1] = scl
    return output


def _scene_source(
    granule: S2Granule,
    bands: tuple[str, ...],
    mask_classes: tuple[int, ...],
    nodata: int,
) -> S2SceneSource:
    offsets, quantification_value = read_l2a_radiometry(granule.product_metadata)
    special_values = tuple(read_l2a_special_values(granule.product_metadata).values())
    return S2SceneSource(
        granule,
        bands,
        offsets,
        special_values,
        quantification_value,
        mask_classes,
        nodata,
    )


def _source_fingerprint(scene: S2SceneSource) -> str:
    paths = [
        scene.granule.product_metadata,
        *[scene.granule.band_paths[band] for band in scene.bands],
    ]
    if scene.granule.scl_path is not None:
        paths.append(scene.granule.scl_path)
    values = []
    for path in sorted(paths, key=str):
        stat = path.stat()
        values.append((str(path.resolve()), stat.st_size, stat.st_mtime_ns))
    serialized = json.dumps(values, separators=(",", ":"))
    return hashlib.sha256(serialized.encode()).hexdigest()


def build_lazy_s2_dataset(
    granules: list[S2Granule],
    grid: GridSpec,
    *,
    bands: tuple[str, ...],
    mask_classes: tuple[int, ...],
    nodata: int,
):
    """Build an xarray Dataset without reading raster pixels or writing files."""
    try:
        import dask.array as da
        import xarray as xr
        from dask import delayed
    except ImportError as error:
        raise RuntimeError(
            "Lazy S2 processing requires xarray and Dask. Install the processing "
            "dependencies."
        ) from error

    windows = tuple(grid.iter_windows())
    scene_arrays = []
    scene_sources = [
        _scene_source(granule, bands, mask_classes, nodata) for granule in granules
    ]
    for scene in scene_sources:
        rows = []
        for row_index in range(grid.chunk_grid_shape[0]):
            blocks = []
            for window in windows:
                if window.row_index != row_index:
                    continue
                task = delayed(_read_scene_chunk, pure=True)(scene, grid, window)
                blocks.append(
                    da.from_delayed(
                        task,
                        shape=(len(bands) + 1, window.height, window.width),
                        dtype=np.uint16,
                    )
                )
            rows.append(da.concatenate(blocks, axis=2))
        scene_arrays.append(da.concatenate(rows, axis=1))
    stacked = da.stack(scene_arrays, axis=0)

    x = grid.bounds[0] + (np.arange(grid.width) + 0.5) * grid.resolution[0]
    y = grid.bounds[3] - (np.arange(grid.height) + 0.5) * grid.resolution[1]
    times = np.array(
        [
            np.datetime64(
                datetime.strptime(
                    source.granule.acquisition_time, "%Y%m%dT%H%M%S"
                ).isoformat(),
                "ns",
            )
            for source in scene_sources
        ]
    )
    dataset = xr.Dataset(
        data_vars={
            "reflectance": (
                ("time", "band", "y", "x"),
                stacked[:, : len(bands)],
                {
                    "long_name": "offset-corrected SCL-masked BOA reflectance DN",
                    "nodata": nodata,
                },
            ),
            "scl": (
                ("time", "y", "x"),
                stacked[:, -1].astype(np.uint8),
                {"long_name": "Sentinel-2 scene classification", "nodata": 0},
            ),
        },
        coords={
            "time": times,
            "band": list(bands),
            "y": y,
            "x": x,
            "product_id": (
                "time",
                [source.granule.product_id for source in scene_sources],
            ),
            "granule_id": (
                "time",
                [source.granule.granule_id for source in scene_sources],
            ),
            "quantification_value": (
                "time",
                [source.quantification_value for source in scene_sources],
            ),
            "source_fingerprint": (
                "time",
                [_source_fingerprint(source) for source in scene_sources],
            ),
        },
        attrs={
            "crs": grid.crs,
            "transform": tuple(grid.affine[:6]),
            "bounds": grid.bounds,
            "nodata": nodata,
        },
    )
    # Keep all spectral bands together so downstream band-wise operations and
    # multi-band COG writes do not need to rechunk the reflectance cube.
    dataset["reflectance"] = dataset.reflectance.chunk({"band": -1})
    return dataset
