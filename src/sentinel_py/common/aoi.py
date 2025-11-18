from __future__ import annotations

from pathlib import Path
from typing import Union, Tuple

import numpy as np
import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon
from shapely.geometry.base import BaseGeometry


GeometryLike = Union[BaseGeometry, gpd.GeoSeries, gpd.GeoDataFrame, str, Path]

def create_aoi_geojson(
    xmin: float,
    ymin: float,
    xmax: float,
    ymax: float,
    *,
    crs: str = "EPSG:4326",
    out_file: str | Path | None = None,
) -> gpd.GeoDataFrame:
    """
    Create an AOI polygon from bounding-box coordinates and optionally
    write it to a GeoJSON file.

    Parameters
    ----------
    xmin, ymin, xmax, ymax : float
        Bounding box coordinates. Assumed to be in the CRS given.
    crs : str, optional
        CRS of the bounding box. Default: EPSG:4326 (lat/lon degrees).
    out_file : str or Path, optional
        If provided, writes the AOI as a GeoJSON FeatureCollection.

    Returns
    -------
    gdf : geopandas.GeoDataFrame
        A GeoDataFrame containing the AOI polygon.
    """
    # build shapely polygon
    poly = Polygon(
        [
            (xmin, ymin),
            (xmax, ymin),
            (xmax, ymax),
            (xmin, ymax),
        ]
    )
    gdf = gpd.GeoDataFrame({"name": ["AOI"], "geometry": [poly]}, crs=crs)

    # export to GeoJSON if requested
    if out_file is not None:
        out_file = Path(out_file)
        out_file.parent.mkdir(parents=True, exist_ok=True)
        gdf.to_file(out_file, driver="GeoJSON")

    return gdf


def _load_aoi(aoi: GeometryLike, crs: str) -> gpd.GeoDataFrame:
    """Normalize AOI input into a GeoDataFrame in the given CRS."""
    # load AOI of certain object types
    if isinstance(aoi, gpd.GeoDataFrame):
        gdf = aoi.copy()
    elif isinstance(aoi, gpd.GeoSeries):
        gdf = gpd.GeoDataFrame(geometry=aoi.copy())
    elif isinstance(aoi, BaseGeometry):
        gdf = gpd.GeoDataFrame(geometry=[aoi], crs=crs)

    # load from file path
    elif isinstance(aoi, (str, Path)):
        gdf = gpd.read_file(aoi)
        if gdf.empty:
            raise ValueError(f"No features found in {aoi}")
    else:
        raise TypeError(f"Unsupported AOI type: {type(aoi)}")

    # ensure correct CRS
    if gdf.crs is None:
        gdf = gdf.set_crs(crs)
    elif gdf.crs.to_string() != crs:
        gdf = gdf.to_crs(crs)

    return gdf


def _remove_holes(geom: BaseGeometry) -> BaseGeometry:
    """Return a copy of geom with interior rings (holes) removed."""
    if geom.is_empty:
        return geom

    if isinstance(geom, Polygon):
        return Polygon(geom.exterior)

    if isinstance(geom, MultiPolygon):
        return MultiPolygon([Polygon(p.exterior) for p in geom.geoms])

    # For other geometry types, just return as-is
    return geom


def overlay_latlon_grid(
    aoi: GeometryLike,
    cell_size_deg: Union[float, Tuple[float, float]],
    *,
    crs: str = "EPSG:4326",
    clip_to_aoi: bool = True,
    fill_aoi_holes: bool = True,
    fill_cell_holes: bool = True,
    out_file: Union[str, Path, None] = None,
) -> gpd.GeoDataFrame:
    """
    Build a regular lat/lon grid over an AOI and clip to it.

    Parameters
    ----------
    aoi : geometry-like
        One of:
        - shapely geometry (Polygon, MultiPolygon, etc.)
        - GeoSeries
        - GeoDataFrame
        - path to a GeoJSON / Shapefile / any vector file readable by GeoPandas.
    cell_size_deg : float or (float, float)
        Grid cell size in degrees. If float, uses same size in x (lon) and y (lat).
    crs : str, optional
        CRS to assume/convert to for the AOI. Default "EPSG:4326".
    clip_to_aoi : bool, optional
        If True (default), cells are clipped to the AOI boundary.
        If False, un-clipped grid cells are returned but only where they
        intersect the AOI.
    fill_aoi_holes : bool, optional
        If True, interior holes in the AOI geometry are removed before building
        the grid. This prevents donut-shaped gaps inside the grid.
    fill_cell_holes : bool, optional
        If True, interior holes in each resulting cell geometry are removed
        after overlay.

    Returns
    -------
    grid : geopandas.GeoDataFrame
        Columns:
        - 'row', 'col': grid indices
        - 'minx', 'miny': lower-left corner of cell
        - 'geometry': polygon or multipolygon for each cell (no holes if
          fill_cell_holes=True)
    """
    # prep the AOI
    gdf = _load_aoi(aoi, crs)
    aoi_union = gdf.union_all()  # dissolve into single geometry
    aoi_union = aoi_union.buffer(0)  # fix potential geometry issues

    # fill holes in AOI if requested
    if fill_aoi_holes:
        aoi_union = _remove_holes(aoi_union)
    if aoi_union.is_empty:
        raise ValueError("AOI union is empty after cleaning.")

    # pull out cell x/y sizes
    if isinstance(cell_size_deg, (tuple, list)):
        dx, dy = cell_size_deg
    else:
        dx = dy = float(cell_size_deg)
    if dx <= 0 or dy <= 0:
        raise ValueError("cell_size_deg must be positive")

    # ensure AOI is fully covered by grid
    minx, miny, maxx, maxy = aoi_union.bounds
    xs = np.arange(minx, maxx, dx)
    ys = np.arange(miny, maxy, dy)

    # build grid cells and clip to AOI
    rows = []
    for j, y in enumerate(ys):
        for i, x in enumerate(xs):

            # build cell polygon
            cell = Polygon(
                [
                    (x, y),
                    (x + dx, y),
                    (x + dx, y + dy),
                    (x, y + dy),
                ]
            )
            # skip cells that don't intersect AOI
            if not cell.intersects(aoi_union):
                continue

            # clip to AOI if requested
            if clip_to_aoi:
                geom_out = cell.intersection(aoi_union)
                if geom_out.is_empty:
                    continue
            else:
                geom_out = cell

            # remove holes in grid if requested
            if fill_cell_holes:
                geom_out = _remove_holes(geom_out)

            rows.append(
                {
                    "row": j,
                    "col": i,
                    "minx": x,
                    "miny": y,
                    "geometry": geom_out,
                }
            )
    grid = gpd.GeoDataFrame(rows, crs=crs)

    # optionally write to file
    if out_file is not None:
        out_file = Path(out_file)
        out_file.parent.mkdir(parents=True, exist_ok=True)
        grid.to_file(out_file, driver="GeoJSON")
    return grid
