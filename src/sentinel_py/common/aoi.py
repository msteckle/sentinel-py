from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional, Union, Tuple, Literal
import itertools

import numpy as np
import geopandas as gpd
import pandas as pd
import shapely
from shapely.geometry import GeometryCollection, MultiPolygon, Polygon, box
from shapely.geometry.base import BaseGeometry, BaseMultipartGeometry


GeometryLike = Union[BaseGeometry, gpd.GeoSeries, gpd.GeoDataFrame, str, Path]


def _geom_wkt_len(geom: BaseGeometry) -> int:
    return len(geom.wkt)


def simplify_aoi_for_cdse(
    aoi: BaseGeometry,
    *,
    logger: logging.Logger,
    max_wkt_chars: int = 20000,
    simplify_tolerances_deg: Tuple[float, ...] = (0.001, 0.0025, 0.005, 0.01, 0.02, 0.05),
    allow_convex_hull: bool = True,
    allow_bbox_fallback: bool = True,
) -> BaseGeometry:
    """
    Reduce AOI complexity to avoid CDSE OData GET URL blowups.

    Strategy:
      - If already short enough: return as-is
      - Try simplify(preserve_topology=True) with increasing tolerances (degrees)
      - Optionally try convex hull
      - Optionally fall back to bbox polygon

    Notes:
      - tolerances are in degrees because AOI is assumed EPSG:4326
      - This is intended for *search footprint filtering*, not precision clipping.
    """
    # Some AOIs are invalid after reprojection / dateline wrap; buffer(0) fixes many
    try:
        if not aoi.is_valid:
            aoi = aoi.buffer(0)
    except Exception:
        pass

    orig_len = _geom_wkt_len(aoi)
    if orig_len <= max_wkt_chars:
        logger.info("AOI WKT length %d <= %d; no simplification needed.", orig_len, max_wkt_chars)
        return aoi

    logger.warning(
        "AOI WKT length is %d chars (>%d). Simplifying for CDSE query stability.",
        orig_len,
        max_wkt_chars,
    )

    # Try successive simplifications
    for tol in simplify_tolerances_deg:
        try:
            simplified = aoi.simplify(tol, preserve_topology=True)
            if simplified.is_empty:
                continue
            # Fix again if simplification creates minor invalidities
            if not simplified.is_valid:
                simplified = simplified.buffer(0)

            new_len = _geom_wkt_len(simplified)
            logger.info("AOI simplified with tol=%.6f deg → WKT length %d", tol, new_len)

            if new_len <= max_wkt_chars:
                return simplified
        except Exception as e:
            logger.debug("AOI simplify failed at tol=%.6f: %s", tol, e)

    # Convex hull is a very good “won’t miss anything” fallback
    if allow_convex_hull:
        try:
            hull = aoi.convex_hull
            hull_len = _geom_wkt_len(hull)
            logger.warning("AOI convex hull WKT length %d", hull_len)
            if hull_len <= max_wkt_chars:
                return hull
        except Exception as e:
            logger.debug("AOI convex_hull failed: %s", e)

    # Final fallback: bbox polygon (will over-download, but always works)
    if allow_bbox_fallback:
        try:
            minx, miny, maxx, maxy = aoi.bounds
            bbox = box(minx, miny, maxx, maxy)
            bbox_len = _geom_wkt_len(bbox)
            logger.warning("AOI bbox fallback WKT length %d", bbox_len)
            return bbox
        except Exception as e:
            logger.debug("AOI bbox fallback failed: %s", e)

    # If everything fails, return original and let query fail loudly
    logger.error("Failed to simplify AOI below max_wkt_chars=%d; using original AOI.", max_wkt_chars)
    return aoi


def parse_bbox(aoi: str) -> Optional[Tuple[float, float, float, float]]:
    """
    Return (xmin, ymin, xmax, ymax) if `aoi` looks like 4 floats, else None.
    Accepts commas or whitespace.
    """
    try:
        raw = aoi.replace(",", " ").split()
        parts = [float(p) for p in raw]
        if len(parts) != 4:
            return None
        xmin, ymin, xmax, ymax = parts

        if xmin >= xmax or ymin >= ymax:
            return None

        return xmin, ymin, xmax, ymax
    except Exception:
        return None

def bbox_to_geojson(
    bbox: Tuple[float, float, float, float],
    *,
    crs: str = "EPSG:4326",
    output: str | Path | None = None,
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
    output : str or Path, optional
        If provided, writes the AOI as a GeoJSON FeatureCollection.

    Returns
    -------
    gdf : geopandas.GeoDataFrame
        A GeoDataFrame containing the AOI polygon.
    """

    xmin, ymin, xmax, ymax = bbox

    # validate bounds
    if xmin >= xmax:
        raise ValueError(f"xmin {xmin=} must be less than xmax {xmax=}.")
    if ymin >= ymax:
        raise ValueError(f"ymin {ymin=} must be less than ymax {ymax=}.")

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

    # optionally, make sure output directory exists and write to GeoJSON
    if output is not None:
        outpath = Path(output)
        outpath.parent.mkdir(parents=True, exist_ok=True)
        if outpath.suffix.lower() != ".geojson":
            outpath = outpath.with_suffix(".geojson")
        gdf.to_file(outpath, driver="GeoJSON")

    return gdf


def csv_to_geojson(
    csv: Union[str, Path],
    lon: str,
    lat: str,
    crs: str = "EPSG:4326",
    output: Union[str, Path, None] = None,
) -> gpd.GeoDataFrame:
    """
    Convert a CSV file with point coordinates into a GeoDataFrame and optionally
    write it to a GeoJSON file.

    Parameters
    ----------
    csv : str or Path
        Path to the input CSV file.
    lon : str
        Name of the column containing longitude (x) coordinates.
    lat : str
        Name of the column containing latitude (y) coordinates.
    crs : str, optional
        CRS of the input coordinates. Default "EPSG:4326" (lat/lon degrees).
    output : str or Path, optional
        If provided, writes the resulting GeoDataFrame as a GeoJSON file.

    Returns
    -------
    gdf : geopandas.GeoDataFrame
        A GeoDataFrame containing Point geometries created from the CSV coordinates.
    """
    df = pd.read_csv(csv)
    if lon not in df.columns or lat not in df.columns:
        raise ValueError(f"CSV must contain columns '{lon}' and '{lat}'")

    geometry = gpd.points_from_xy(df[lon], df[lat])
    gdf = gpd.GeoDataFrame(df.drop(columns=[lon, lat]), geometry=geometry, crs=crs)

    # optionally, make sure output directory exists and write to GeoJSON
    if output is not None:
        outpath = Path(output)
        outpath.parent.mkdir(parents=True, exist_ok=True)
        if outpath.suffix.lower() != ".geojson":
            outpath = outpath.with_suffix(".geojson")
        gdf.to_file(outpath, driver="GeoJSON")

    return gdf


def aoi_as_gdf(aoi: GeometryLike, crs: str = "EPSG:4326") -> gpd.GeoDataFrame:
    """Normalize aoi input into a GeoDataFrame in the given CRS."""

    if isinstance(aoi, gpd.GeoDataFrame):
        gdf = aoi.copy()
    elif isinstance(aoi, gpd.GeoSeries):
        gdf = gpd.GeoDataFrame(geometry=aoi.copy())
    elif isinstance(aoi, BaseGeometry):
        bounds = aoi.bounds  # (minx, miny, maxx, maxy)
        if not (
            -180 <= bounds[0] <= 180 and
            -180 <= bounds[2] <= 180 and
            -90  <= bounds[1] <= 90  and
            -90  <= bounds[3] <= 90
        ):
            raise ValueError(
                "Shapely geometry has no CRS metadata and coordinates do not appear to "
                "be lat/lon (EPSG:4326). Please pass a GeoDataFrame or GeoSeries with "
                "a CRS set."
            )
        gdf = gpd.GeoDataFrame(geometry=[aoi], crs=crs)
    elif isinstance(aoi, (str, Path)):
        aoi = Path(aoi)
        if not aoi.is_file():
            raise ValueError(f"AOI path does not exist: {aoi}")
        gdf = gpd.read_file(aoi)
    else:
        raise TypeError(f"Unsupported AOI type: {type(aoi)}")

    if gdf.empty:
        raise ValueError(f"No features found in {aoi}")

    if gdf.crs is None:
        gdf = gdf.set_crs(crs)
    elif not gdf.crs.equals(crs):
        gdf = gdf.to_crs(crs)

    return gdf


def aoi_as_geom(aoi: GeometryLike, crs: str = "EPSG:4326") -> gpd.GeoSeries:
    """Normalize AOI input into a GeoSeries in the given CRS."""
    return aoi_as_gdf(aoi, crs).geometry


def _remove_holes(geom: BaseGeometry) -> BaseGeometry:
    """Return a copy of geom with interior rings (holes) removed."""

    # handle empty or None geometries
    if geom is None:
        raise TypeError("geom must not be None")
    if geom.is_empty:
        return geom

    # handle Polygons and MultiPolygons by keeping only the exterior ring(s)
    if isinstance(geom, Polygon):
        if geom.exterior is None:
            return Polygon()
        result = Polygon(geom.exterior)
        return shapely.make_valid(result) if not result.is_valid else result
    if isinstance(geom, MultiPolygon):
        parts = []
        for p in geom.geoms:
            if p.exterior is None:
                continue
            part = Polygon(p.exterior)
            parts.append(shapely.make_valid(part) if not part.is_valid else part)
        return MultiPolygon(parts) if parts else MultiPolygon()

    # handle GeometryCollections by recursively removing holes from each part
    if isinstance(geom, GeometryCollection):
        cleaned = [_remove_holes(part) for part in geom.geoms]
        return GeometryCollection(cleaned)

    return geom


def overlay_latlon_grid(
    aoi: GeometryLike,
    cell_size_deg: Union[float, Tuple[float, float]],
    *,
    crs: str = "EPSG:4326",
    fill_holes: bool = True,
    clip: Optional[Literal["intersect", "within", "all"]] = "all",
    output: Union[str, Path, None] = None,
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
    fill_holes : bool, optional
        If True, interior holes in the AOI geometry are removed before building
        the grid. This prevents donut-shaped gaps inside the grid.
    clip : str, optional
        One of "intersect", "within", or "all" to select grid cells that intersect, 
        are fully within, or all cells in the AOI outer bounding box.

    Returns
    -------
    grid : geopandas.GeoDataFrame
        Columns:
        - 'row', 'col': grid indices
        - 'minx', 'miny': lower-left corner of cell
        - 'geometry': polygon or multipolygon for each cell
    """

    # simplify the AOI
    gdf = aoi_as_gdf(aoi, crs)
    aoi_union = gdf.union_all()  # dissolve into single geometry
    aoi_union = aoi_union.buffer(0)  # fix potential geometry issues
    if aoi_union.is_empty:
        raise ValueError("AOI is empty after dissolving and buffering.")

    # optionally fill holes in AOI before building grid
    if fill_holes:
        aoi_union = _remove_holes(aoi_union)
        if aoi_union.is_empty:
            raise ValueError("AOI is empty after removing holes.")

    # handle tuple or single value for cell size
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

    # build cells as GeoDataFrame; geometry is box from (x,y) to (x+dx, y+dy)
    cells = [
        {"row": j, "col": i, "minx": x, "miny": y, "geometry": box(x, y, x+dx, y+dy)}
        for (j, y), (i, x) in itertools.product(enumerate(ys), enumerate(xs))
    ]
    grid = gpd.GeoDataFrame(cells, crs=crs)

    # handle clip mode: intersect, within, or all
    if clip:
        if clip == "intersect":
            grid = grid[grid.intersects(aoi_union)].copy()
        elif clip == "within":
            grid = grid[grid.within(aoi_union)].copy()
        elif clip == "all":
            pass  # keep all cells in bounding box
        else:
            raise ValueError(f"Invalid clip mode: {clip=}")

    # optionally, make sure output directory exists and write to GeoJSON
    if output is not None:
        outpath = Path(output)
        outpath.parent.mkdir(parents=True, exist_ok=True)
        if outpath.suffix.lower() != ".geojson":
            outpath = outpath.with_suffix(".geojson")
        grid.to_file(outpath, driver="GeoJSON")

    return grid