"""AOI feature planning for per-feature raster and point pipeline outputs."""

from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Literal

import geopandas as gpd
import numpy as np
from pyproj import Transformer
from shapely.geometry.base import BaseGeometry

from affine import Affine
from sentinel_py.pipeline.grid import ChunkShape, GridSpec, plan_geometry_grid


@dataclass(frozen=True)
class AOIUnit:
    """One independently planned polygon component or point group."""

    aoi_id: str
    component_id: str
    kind: Literal["polygon", "point"]
    geometry: BaseGeometry
    geometry_wgs84: BaseGeometry
    grid: GridSpec | None


@dataclass(frozen=True)
class AOIPlan:
    """Validated, homogeneous AOI units keyed by stable feature identity."""

    kind: Literal["polygon", "point"]
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
        part_kinds = {"point" if part.geom_type == "Point" else "polygon" if part.geom_type == "Polygon" else "unsupported" for part in parts}
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
                if kind == "polygon"
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
