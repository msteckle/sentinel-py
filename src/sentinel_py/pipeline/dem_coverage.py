"""Find where a pipeline's DEM tiles do not cover the ground SNAP will process.

For every ``snap_gpt`` node the processing region of each selected scene (the AOI
overlap plus margin, or the whole footprint) and the larger DEM extent around it are
compared with the footprints of the node's local DEM tiles. The result is purely
geometric and needs neither SNAP nor the network, so it can run on a login node
before jobs are submitted; the missing areas feed ``sentinel-py pgc query``.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from shapely.geometry import Polygon, box
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union

from sentinel_py.pipeline import snap
from sentinel_py.pipeline.readers.s1 import build_scene_catalog, select_unit_scenes

NODE_TYPE = "snap_gpt"


@dataclass(frozen=True)
class DemGap:
    """The uncovered part of one scene's DEM requirement for one AOI unit.

    Attributes
    ----------
    node_id, dem_source : str
        The ``snap_gpt`` node and the ``dem.local`` source it uses.
    dem_dir : pathlib.Path
        Directory where missing tiles should be downloaded.
    aoi_id, component_id, scene : str
        The AOI unit and scene that need the DEM.
    region : tuple[float, float, float, float]
        Processing region ``(west, south, east, north)`` in EPSG:4326.
    required : shapely.geometry.base.BaseGeometry
        Part of the region with no DEM tile (empty when covered). SNAP output over
        it would be empty, so the processor refuses to run.
    margin : shapely.geometry.base.BaseGeometry
        Part of the DEM margin outside the region with no DEM tile (empty when
        covered); not blocking, but geocoding near the region edge benefits.
    """

    node_id: str
    dem_source: str
    dem_dir: Path
    aoi_id: str
    component_id: str
    scene: str
    region: tuple[float, float, float, float]
    required: BaseGeometry
    margin: BaseGeometry


def _significant(geometry: BaseGeometry) -> BaseGeometry:
    """Drop float-noise slivers from a gap; return an empty geometry if none remain."""
    parts = [
        part
        for part in getattr(geometry, "geoms", [geometry])
        if part.area > snap.MIN_GAP_DEG2
    ]
    return unary_union(parts) if parts else Polygon()


def find_dem_gaps(
    config: Any,
    node_ids: Iterable[str] | None = None,
    *,
    include_margin: bool = True,
) -> list[DemGap]:
    """List scenes whose processing region or DEM margin lacks DEM tiles.

    Parameters
    ----------
    config : PipelineConfig
        Validated pipeline configuration.
    node_ids : iterable of str, optional
        Restrict the check to these nodes; by default every ``snap_gpt`` node.
    include_margin : bool, default True
        Also report scenes whose region is covered but whose DEM margin is not.

    Returns
    -------
    list[DemGap]
        One entry per (node, AOI unit, scene) with a gap; empty when everything is
        covered. Units that select no scenes are skipped (the processor reports
        that case itself).
    """
    wanted = None if node_ids is None else set(node_ids)
    gaps: list[DemGap] = []
    for node in config.ordered_nodes:
        if node.node_type != NODE_TYPE or (wanted is not None and node.node_id not in wanted):
            continue
        settings = node.config
        source = config.sources[settings.source]
        dem = config.sources[settings.dem]
        tiles = tuple(sorted(Path(dem.data_dir).glob(dem.pattern)))
        covered = unary_union([box(*snap.tile_footprint_wgs84(tile)) for tile in tiles])
        catalog = build_scene_catalog(source.data_dir)
        for unit in config.aoi_plan.units:
            scenes = select_unit_scenes(catalog, unit, source, config.selection)
            for row in scenes.itertuples():
                region, dem_bounds = snap.scene_region_bounds(
                    unit.geometry_wgs84, row.footprint_wkt,
                    subset=settings.subset,
                    subset_margin_m=settings.subset_margin_m,
                    dem_margin_m=settings.dem_margin_m,
                )
                region_box = box(*region)
                required = _significant(region_box.difference(covered))
                margin = _significant(
                    box(*dem_bounds).difference(region_box).difference(covered)
                )
                if required.is_empty and (margin.is_empty or not include_margin):
                    continue
                gaps.append(
                    DemGap(
                        node.node_id, settings.dem, Path(dem.data_dir), unit.aoi_id,
                        unit.component_id, row.scene, region, required,
                        margin if include_margin else Polygon(),
                    )
                )
    return gaps
