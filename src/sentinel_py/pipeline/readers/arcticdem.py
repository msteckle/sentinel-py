"""ArcticDEM tile discovery for AOI-unit processing, from local files or PGC."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from shapely.geometry.base import BaseGeometry

from sentinel_py.cache import DEFAULT_PGC_CACHE_DIR
from sentinel_py.enums import SourceType
from sentinel_py.pipeline.grid import AOIPlan, AOIUnit
from sentinel_py.pipeline.readers.base import BaseReader

# Footprints of remote tiles are straight-edged polygons in WGS84, while the tiles are
# squares in a polar projection, so their true edges bulge slightly. Tiles are selected
# with this margin (degrees) so a point near a tile edge never misses its neighbour.
DEM_FOOTPRINT_BUFFER_DEG = 0.05


########################################################################################
# DEM tile records
########################################################################################


@dataclass(frozen=True)
class DEMTile:
    """One DEM GeoTIFF, local or remote, as handed to the terrain processor.

    Attributes
    ----------
    path
        File path or URL that ``rasterio`` can open.
    fingerprint
        Stable identity of the tile contents folded into provenance and cache keys
        (the path for local tiles; URL and ETag for remote tiles).
    footprint
        WGS84 footprint when known without opening the file. ``None`` means the
        raster bounds must be read from the file.
    """

    path: str | Path
    fingerprint: str
    footprint: BaseGeometry | None = None


def local_dem_tiles(source: Any) -> tuple[DEMTile, ...]:
    """List the GeoTIFFs of a ``dem.local`` source in deterministic order.

    Raises
    ------
    RuntimeError
        If no file matches the source pattern.
    """
    paths = sorted(source.data_dir.glob(source.pattern))
    if not paths:
        raise RuntimeError(f"No DEM GeoTIFFs matched {source.data_dir / source.pattern}")
    return tuple(DEMTile(path, str(path)) for path in paths)


def remote_dem_tiles(
    plan: AOIPlan,
    *,
    cache_dir: Path = DEFAULT_PGC_CACHE_DIR,
    logger: logging.Logger | None = None,
) -> tuple[DEMTile, ...]:
    """List the PGC ArcticDEM tiles covering every unit of an AOI plan.

    One STAC query covers the bounds of all units, so thousands of points cost one
    search. The result is cached like ``sentinel-py pgc query`` caches its manifest, and
    shares that cache for identical geometries.

    Raises
    ------
    RuntimeError
        If PGC returns no tile for the AOIs.
    """
    import datetime as dt

    from shapely import wkt
    from shapely.geometry import box

    from sentinel_py.cache import QueryCache
    from sentinel_py.download.pgc import arcticdem_query_payload, query_arcticdem

    logger = logger or logging.getLogger(__name__)
    bounds = [unit.geometry_wgs84.bounds for unit in plan.units]
    west, south, east, north = (
        min(item[0] for item in bounds),
        min(item[1] for item in bounds),
        max(item[2] for item in bounds),
        max(item[3] for item in bounds),
    )
    # The margin also keeps the polygon valid when the plan is a single point.
    query_area = box(
        round(west, 3), round(south, 3), round(east, 3), round(north, 3)
    ).buffer(DEM_FOOTPRINT_BUFFER_DEG)
    payload = arcticdem_query_payload([query_area.wkt])
    query_cache = QueryCache.for_payload(cache_dir, payload, "manifest.parquet")
    if query_cache.exists():
        manifest = query_cache.load()
        logger.info("Loaded cached PGC query: %s", query_cache.table_path)
    else:
        manifest = query_arcticdem([query_area], logger=logger)
        query_cache.save(
            manifest,
            {
                **payload,
                "created": dt.datetime.now().isoformat(),
                "num_tiles": len(manifest),
            },
        )
        logger.info("Cached PGC query: %s", query_cache.table_path)
    if manifest.empty:
        raise RuntimeError("PGC returned no ArcticDEM tiles for the pipeline AOIs")
    manifest = manifest.sort_values("url")
    return tuple(
        DEMTile(row.url, f"{row.url}#{row.etag or ''}", wkt.loads(row.geometry_wkt))
        for row in manifest.itertuples()
    )


########################################################################################
# DEM tile selection
########################################################################################


def select_dem_tiles(
    tiles: tuple[DEMTile, ...], unit: AOIUnit, *, grid: Any | None = None
) -> tuple[DEMTile, ...]:
    """Return the tiles whose bounds intersect one AOI component.

    Tiles with a known footprint are tested without any I/O; for the others the raster
    bounds are read once, while building a unit plan. Worker tasks receive only the
    selected paths rather than the complete tile list.
    """
    import geopandas as gpd
    import rasterio
    from shapely.geometry import box

    target_crs = (grid or unit.grid).crs
    selected: list[DEMTile] = []
    for tile in tiles:
        if tile.footprint is not None:
            hit = unit.geometry_wgs84.intersects(
                tile.footprint.buffer(DEM_FOOTPRINT_BUFFER_DEG)
            )
        else:
            with rasterio.open(tile.path) as dataset:
                footprint = box(*dataset.bounds)
                projected = (
                    gpd.GeoSeries([footprint], crs=dataset.crs).to_crs(target_crs).iloc[0]
                )
                hit = projected.intersects(unit.geometry)
        if hit:
            selected.append(tile)
    return tuple(selected)


def select_dem_paths(
    paths: tuple[Path, ...], unit: AOIUnit, *, grid: Any | None = None
) -> tuple[Path, ...]:
    """Return DEM files whose raster bounds intersect one AOI component."""
    tiles = tuple(DEMTile(path, str(path)) for path in paths)
    return tuple(tile.path for tile in select_dem_tiles(tiles, unit, grid=grid))


########################################################################################
# Pipeline reader adapter
########################################################################################


class ArcticDEMReader(BaseReader):
    """Reader adapter for ArcticDEM GeoTIFFs, local (``dem.local``) or PGC (``dem.remote``)."""

    def __init__(self, source_type: SourceType = SourceType.dem_local) -> None:
        self.source_type = source_type

    def parse_config(self, source_id: str, raw: dict[str, Any], base_dir: Any) -> Any:
        """Parse a local GeoTIFF directory or a remote PGC source mapping."""
        from sentinel_py.pipeline.config import (
            DEMSourceConfig,
            PipelineConfigError,
            only_keys,
            required,
            resolve_path,
        )

        if self.source_type == SourceType.dem_remote:
            only_keys(raw, {"type"}, f"sources.{source_id}")
            return DEMSourceConfig(source_id, None, source_type=self.source_type)
        only_keys(raw, {"type", "data_dir", "pattern"}, f"sources.{source_id}")
        data_dir = resolve_path(
            required(raw, "data_dir", f"sources.{source_id}"),
            base_dir,
            f"sources.{source_id}.data_dir",
        )
        if not data_dir.is_dir():
            raise PipelineConfigError(f"DEM source directory does not exist: {data_dir}")
        pattern = raw.get("pattern", "*.tif")
        if not isinstance(pattern, str) or not pattern.strip():
            raise PipelineConfigError(f"sources.{source_id}.pattern must be a string")
        return DEMSourceConfig(source_id, data_dir, pattern.strip())

    def tiles(
        self, source: Any, plan: AOIPlan, *, logger: logging.Logger | None = None
    ) -> tuple[DEMTile, ...]:
        """Return the DEM tiles of ``source`` that the terrain processor may read."""
        if source.source_type == SourceType.dem_remote:
            return remote_dem_tiles(plan, logger=logger)
        return local_dem_tiles(source)

    def read_raster(self, *args: Any, **kwargs: Any) -> Any:
        """DEM raster execution is owned by the terrain processor."""
        raise NotImplementedError("DEM raster reads require terrain feature settings")

    def sample_points(self, *args: Any, **kwargs: Any) -> Any:
        """DEM point execution is owned by the terrain processor."""
        raise NotImplementedError("DEM point reads require terrain feature settings")
