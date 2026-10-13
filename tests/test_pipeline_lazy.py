"""Tests for shared lazy raster window and reprojection primitives."""

from __future__ import annotations

import numpy as np
import pytest
from affine import Affine

from sentinel_py.pipeline.grid import ChunkShape, GridSpec, GridWindow
from sentinel_py.pipeline.lazy import (
    native_pixel_window,
    select_resampling,
    warp_to_window,
    window_transform,
)

rasterio = pytest.importorskip("rasterio")


def _grid(resolution: float = 20.0) -> GridSpec:
    bounds = (500000.0, 999000.0, 500400.0, 999400.0)
    width = int((bounds[2] - bounds[0]) / resolution)
    height = int((bounds[3] - bounds[1]) / resolution)
    return GridSpec(
        crs="EPSG:32606",
        resolution=(resolution, resolution),
        affine=Affine(resolution, 0, bounds[0], 0, -resolution, bounds[3]),
        bounds=bounds,
        width=width,
        height=height,
        chunks=ChunkShape(y=height, x=width),
    )


def _window(grid: GridSpec) -> GridWindow:
    return GridWindow(
        row_index=0,
        column_index=0,
        row_offset=0,
        column_offset=0,
        height=grid.height,
        width=grid.width,
        bounds=grid.bounds,
    )


def _legacy_memoryfile_warp(source, transform, crs, grid, window, resampling, nodata):
    """Reference implementation: the former MemoryFile-based Sentinel-2 warp."""
    from rasterio.enums import Resampling
    from rasterio.warp import reproject

    fill = 0 if nodata is None else nodata
    destination_values = np.full((window.height, window.width), fill, dtype=np.uint16)
    xmin, _, _, ymax = window.bounds
    destination_transform = Affine(
        grid.resolution[0], 0.0, xmin, 0.0, -grid.resolution[1], ymax
    )
    source_options = {
        "driver": "GTiff", "width": source.shape[1], "height": source.shape[0],
        "count": 1, "dtype": source.dtype, "crs": crs, "transform": transform,
        "nodata": nodata,
    }
    destination_options = {
        "driver": "GTiff", "width": window.width, "height": window.height,
        "count": 1, "dtype": destination_values.dtype, "crs": grid.crs,
        "transform": destination_transform, "nodata": nodata,
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


@pytest.mark.parametrize("resampling", ["nearest", "average", "bilinear"])
@pytest.mark.parametrize("nodata", [None, 65535])
@pytest.mark.parametrize("source_resolution", [10.0, 20.0, 60.0])
def test_warp_to_window_matches_legacy_memoryfile_warp(
    resampling: str, nodata: int | None, source_resolution: float
):
    grid = _grid()
    window = _window(grid)
    rng = np.random.default_rng(7)
    size = int(500 / source_resolution)
    source = rng.integers(0, 4000, size=(size, size), dtype=np.uint16)
    if nodata is not None:
        source[: size // 4, : size // 4] = nodata
    # Offset and rotate-free but CRS-shifted source so reprojection is non-trivial.
    transform = Affine(source_resolution, 0, 499900.0, 0, -source_resolution, 999500.0)

    expected = _legacy_memoryfile_warp(
        source, transform, "EPSG:32606", grid, window, resampling, nodata
    )
    actual = warp_to_window(
        source, transform, "EPSG:32606", grid, window,
        resampling=resampling, src_nodata=nodata, dst_nodata=nodata,
    )

    np.testing.assert_array_equal(actual, expected)
    assert actual.dtype == np.uint16


def test_warp_to_window_without_source_returns_fill():
    grid = _grid()
    window = _window(grid)
    filled = warp_to_window(
        None, None, None, grid, window,
        resampling="nearest", src_nodata=7, dst_nodata=7,
    )
    assert filled.shape == (window.height, window.width)
    assert (filled == 7).all()
    zeros = warp_to_window(
        None, None, None, grid, window,
        resampling="nearest", src_nodata=None, dst_nodata=None,
    )
    assert (zeros == 0).all()


def test_warp_to_window_float_nan_nodata():
    grid = _grid()
    window = _window(grid)
    source = np.full((25, 25), 3.5, dtype=np.float32)
    source[:5] = np.nan
    transform = Affine(20.0, 0, 500000.0, 0, -20.0, 999400.0)
    result = warp_to_window(
        source, transform, "EPSG:32606", grid, window,
        resampling="bilinear", src_nodata=np.nan, dst_nodata=np.nan,
        dtype=np.float32,
    )
    assert result.dtype == np.float32
    assert np.isnan(result[0]).all()
    assert result[-1, 0] == pytest.approx(3.5)


def test_window_transform_and_resampling_selection():
    grid = _grid(20.0)
    window = _window(grid)
    assert window_transform(grid, window) == Affine(
        20.0, 0.0, 500000.0, 0.0, -20.0, 999400.0
    )
    assert select_resampling(20.0, grid) == "nearest"
    assert select_resampling(10.0, grid) == "average"
    assert select_resampling(60.0, grid) == "bilinear"


def test_native_pixel_window_pads_clips_and_detects_misses(tmp_path):
    path = tmp_path / "native.tif"
    transform = Affine(10.0, 0, 500000.0, 0, -10.0, 999400.0)
    with rasterio.open(
        path, "w", driver="GTiff", width=40, height=40, count=1, dtype="uint16",
        crs="EPSG:32606", transform=transform,
    ) as dataset:
        dataset.write(np.zeros((1, 40, 40), dtype=np.uint16))

    with rasterio.open(path) as dataset:
        inside = native_pixel_window(
            dataset, (500100.0, 999100.0, 500200.0, 999200.0), "EPSG:32606", padding=2
        )
        outside = native_pixel_window(
            dataset, (600000.0, 999100.0, 600100.0, 999200.0), "EPSG:32606"
        )
        clipped = native_pixel_window(
            dataset, (499900.0, 999300.0, 500050.0, 999500.0), "EPSG:32606", padding=2
        )

    assert inside == (8, 18, 14, 14)
    assert outside is None
    assert clipped[0] == 0 and clipped[1] == 0
