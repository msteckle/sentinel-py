"""Sentinel-2 source discovery and lazy AOI-unit reads."""

from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Any

import pandas as pd

from pydantic import BaseModel

from sentinel_py.pipeline.aoi import AOIUnit, point_grid
from sentinel_py.pipeline.grid import ChunkShape
from sentinel_py.pipeline.readers.base import BaseReader, apply_polygon_mask


def _read_scene_point(
    granule: Any,
    unit: AOIUnit,
    *,
    crs: str,
    resolution: tuple[float, float],
    anchor: tuple[float, float],
    bands: tuple[str, ...],
    mask_classes: tuple[int, ...],
    nodata: int,
) -> dict[str, Any]:
    """Read one scene's canonical pixel for one point in an isolated process."""
    from sentinel_py.s2.lazy import _read_scene_chunk, _scene_source, _source_fingerprint

    grid = point_grid(unit, crs=crs, resolution=resolution, anchor=anchor,
                      chunks=ChunkShape(y=1, x=1))
    scene = _scene_source(granule, bands, mask_classes, nodata)
    values = _read_scene_chunk(scene, grid, next(grid.iter_windows()))[:, 0, 0]
    row = {
        "aoi_id": unit.aoi_id,
        "component_id": unit.component_id,
        "point_index": int(unit.component_id.rsplit("-", 1)[-1]),
        "longitude": float(unit.geometry_wgs84.x),
        "latitude": float(unit.geometry_wgs84.y),
        "time": granule.acquisition_time,
        "product_id": granule.product_id,
        "granule_id": granule.granule_id,
        "time_representation": "acquisition",
        "source_fingerprint": _source_fingerprint(scene),
    }
    row.update({band: value.item() for band, value in zip(bands, values)})
    return row


def sample_s2_points(
    units: tuple[AOIUnit, ...],
    *,
    data_dir: Any,
    bands: tuple[str, ...],
    resolution_m: int,
    years: tuple[int, ...] | None,
    speriod: tuple[int, int],
    eperiod: tuple[int, int],
    mask_classes: tuple[int, ...],
    nodata: int,
    crs: str,
    resolution: tuple[float, float],
    anchor: tuple[float, float],
    workers: int,
    progress: Callable[[int, int], None] | None = None,
) -> pd.DataFrame:
    """Discover once and sample all intersecting scene/point pairs in parallel."""
    from sentinel_py.s2.discover import discover_s2_granules

    granules = discover_s2_granules(
        data_dir, bands, resolution_m, years=years, speriod=speriod, eperiod=eperiod,
    )
    work = [
        (granule, unit)
        for granule in granules
        for unit in units
        if granule.footprint is not None and granule.footprint.intersects(unit.geometry_wgs84)
    ]
    if not work:
        raise RuntimeError("Found 0 Sentinel-2 granules intersecting the point AOIs")
    if progress is not None:
        progress(0, len(work))
    rows: list[dict[str, Any]] = []
    with ProcessPoolExecutor(max_workers=max(1, workers)) as executor:
        futures = [
            executor.submit(
                _read_scene_point, granule, unit, crs=crs, resolution=resolution,
                anchor=anchor, bands=bands, mask_classes=mask_classes, nodata=nodata,
            )
            for granule, unit in work
        ]
        for completed, future in enumerate(as_completed(futures), start=1):
            rows.append(future.result())
            if progress is not None:
                progress(completed, len(work))
    return pd.DataFrame.from_records(rows)


class _S2ReaderConfig(BaseModel):
    """Marker model for the built-in local Sentinel-2 reader."""


class S2Reader(BaseReader):
    """Reader adapter for downloaded Sentinel-2 L2A SAFE products."""

    source_type = "s2.l2a.local"
    config_model = _S2ReaderConfig

    def parse_config(self, source_id: str, raw: dict[str, Any], base_dir: Any) -> Any:
        """Parse the source configuration without coupling YAML parsing to readers."""
        from sentinel_py.pipeline.config import (
            PipelineConfigError,
            S2SourceConfig,
            _only_keys,
            _required,
            _resolve_path,
        )

        _only_keys(raw, {"type", "data_dir"}, f"sources.{source_id}")
        data_dir = _resolve_path(
            _required(raw, "data_dir", f"sources.{source_id}"),
            base_dir,
            f"sources.{source_id}.data_dir",
        )
        if not data_dir.is_dir():
            raise PipelineConfigError(f"S2 source directory does not exist: {data_dir}")
        return S2SourceConfig(source_id, data_dir)

    def read_raster(self, unit: AOIUnit, **kwargs: Any) -> Any:
        """Build one lazy canonical-grid raster read."""
        return read_s2_unit(unit, **kwargs)

    def sample_points(self, units: tuple[AOIUnit, ...], **kwargs: Any) -> pd.DataFrame:
        """Read all requested S2 point observations through the bulk sampler."""
        return sample_s2_points(units, **kwargs)


def read_s2_unit(
    unit: AOIUnit,
    *,
    data_dir: Any,
    bands: tuple[str, ...],
    resolution_m: int,
    years: tuple[int, ...] | None,
    speriod: tuple[int, int],
    eperiod: tuple[int, int],
    mask_classes: tuple[int, ...],
    nodata: int,
    grid: Any | None = None,
) -> Any:
    """Build a lazy Sentinel-2 dataset for one planned polygon AOI unit."""
    from sentinel_py.s2.discover import discover_s2_granules
    from sentinel_py.s2.lazy import build_lazy_s2_dataset

    grid = grid or unit.grid
    if grid is None:
        raise ValueError("Sentinel-2 raster reads require a polygon AOI unit")
    granules = discover_s2_granules(
        data_dir, bands, resolution_m, aoi=unit.geometry_wgs84, years=years,
        speriod=speriod, eperiod=eperiod,
    )
    if not granules:
        raise RuntimeError(f"Found 0 Sentinel-2 granules for AOI {unit.aoi_id}")
    dataset = build_lazy_s2_dataset(
        granules, grid, bands=bands, mask_classes=mask_classes, nodata=nodata,
    )
    return apply_polygon_mask(dataset, unit)
