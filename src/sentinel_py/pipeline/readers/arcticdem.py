"""Local ArcticDEM discovery helpers for AOI-unit processing."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pydantic import BaseModel

from sentinel_py.enums import SourceType
from sentinel_py.pipeline.aoi import AOIUnit
from sentinel_py.pipeline.readers.base import BaseReader


class _ArcticDEMReaderConfig(BaseModel):
    """Marker model for the built-in local ArcticDEM reader."""


class ArcticDEMReader(BaseReader):
    """Reader adapter for a local ArcticDEM GeoTIFF collection."""

    source_type = SourceType.dem_local
    config_model = _ArcticDEMReaderConfig

    def parse_config(self, source_id: str, raw: dict[str, Any], base_dir: Any) -> Any:
        """Parse a local GeoTIFF source mapping."""
        from sentinel_py.pipeline.config import (
            DEMSourceConfig,
            PipelineConfigError,
            _only_keys,
            _required,
            _resolve_path,
        )

        _only_keys(raw, {"type", "data_dir", "pattern"}, f"sources.{source_id}")
        data_dir = _resolve_path(
            _required(raw, "data_dir", f"sources.{source_id}"),
            base_dir,
            f"sources.{source_id}.data_dir",
        )
        if not data_dir.is_dir():
            raise PipelineConfigError(f"DEM source directory does not exist: {data_dir}")
        pattern = raw.get("pattern", "*.tif")
        if not isinstance(pattern, str) or not pattern.strip():
            raise PipelineConfigError(f"sources.{source_id}.pattern must be a string")
        return DEMSourceConfig(source_id, data_dir, pattern.strip())

    def read_raster(self, *args: Any, **kwargs: Any) -> Any:
        """DEM raster execution is owned by the terrain processor."""
        raise NotImplementedError("DEM raster reads require terrain feature settings")

    def sample_points(self, *args: Any, **kwargs: Any) -> Any:
        """DEM point execution is owned by the terrain processor."""
        raise NotImplementedError("DEM point reads require terrain feature settings")


def select_dem_paths(paths: tuple[Path, ...], unit: AOIUnit, *, grid: Any | None = None) -> tuple[Path, ...]:
    """Return DEM files whose raster bounds intersect one AOI component.

    Metadata is inspected once while building a unit plan; worker tasks receive only
    the selected paths rather than the source directory's complete glob.
    """
    import geopandas as gpd
    from shapely.geometry import box

    selected: list[Path] = []
    target_crs = (grid or unit.grid).crs
    for path in paths:
        import rasterio
        with rasterio.open(path) as dataset:
            footprint = box(*dataset.bounds)
            projected = gpd.GeoSeries([footprint], crs=dataset.crs).to_crs(target_crs).iloc[0]
            if projected.intersects(unit.geometry):
                selected.append(path)
    return tuple(selected)
