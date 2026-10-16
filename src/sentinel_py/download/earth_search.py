"""Sentinel-2 L2A cloud-optimized GeoTIFF discovery through Element84 Earth Search.

Only querying lives here: the public ``sentinel-cogs`` bucket is read in place by the
pipeline (``s2.l2a.remote`` sources), so there is nothing to download.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import pandas as pd
import shapely
from shapely.geometry import mapping, shape
from shapely.geometry import base as shapely_base
from shapely.ops import unary_union

from sentinel_py.enums import S2_BAND_ASSET_KEYS, EarthSearchCollection

EARTH_SEARCH_URL = "https://earth-search.aws.element84.com/v1"
EARTH_SEARCH_COLUMNS = [
    "item_id",
    "collection",
    "datetime",
    "product_id",
    "tile_id",
    "processing_baseline",
    "geometry_wkt",
    "assets",
]

# Largest number of vertices unioned into one search geometry, and concurrent searches.
# Earth Search publishes no request limit, so both stay small.
MAX_SEARCH_VERTICES = 2000
MAX_SEARCH_THREADS = 4


########################################################################################
# Item rows
########################################################################################


def _asset_record(asset: Any) -> dict[str, Any]:
    """Return the href and radiometry of one COG asset from its ``raster:bands``."""
    raster = (asset.extra_fields.get("raster:bands") or [{}])[0]
    return {
        "href": asset.href,
        "scale": raster.get("scale"),
        "offset": raster.get("offset"),
        "nodata": raster.get("nodata"),
    }


def _item_row(item: Any) -> dict[str, Any]:
    """Convert one STAC item into a cacheable row; assets are stored as JSON."""
    properties = item.properties
    assets = {
        band: _asset_record(item.assets[key])
        for band, key in S2_BAND_ASSET_KEYS.items()
        if key in item.assets
    }
    return {
        "item_id": item.id,
        "collection": item.collection_id,
        "datetime": properties.get("datetime") or item.datetime.isoformat(),
        "product_id": str(properties.get("s2:product_uri", item.id)).removesuffix(
            ".SAFE"
        ),
        "tile_id": "T" + str(properties.get("grid:code", "")).removeprefix("MGRS-"),
        "processing_baseline": properties.get("s2:processing_baseline"),
        "geometry_wkt": shape(item.geometry).wkt,
        "assets": json.dumps(assets, sort_keys=True),
    }


########################################################################################
# Query
########################################################################################


def _search_batches(
    geometries: list[shapely_base.BaseGeometry],
) -> list[shapely_base.BaseGeometry]:
    """Union spatially adjacent AOI geometries into few, bounded search geometries.

    A search per point would cost thousands of requests, and one union of every AOI
    would exceed request-size limits, so geometries are sorted by position and
    unioned in groups of at most ``MAX_SEARCH_VERTICES`` vertices.
    """
    ordered = sorted(geometries, key=lambda geometry: geometry.centroid.coords[0])
    batches: list[shapely_base.BaseGeometry] = []
    current: list[shapely_base.BaseGeometry] = []
    vertices = 0
    for geometry in ordered:
        count = shapely.get_num_coordinates(geometry)
        if current and vertices + count > MAX_SEARCH_VERTICES:
            batches.append(unary_union(current))
            current, vertices = [], 0
        current.append(geometry)
        vertices += count
    if current:
        batches.append(unary_union(current))
    return batches


def query_earth_search(
    aoi_geometries: Iterable[shapely_base.BaseGeometry],
    *,
    collection: str = EarthSearchCollection.c1_l2a,
    years: tuple[int, ...] | None,
    speriod: tuple[int, int],
    eperiod: tuple[int, int],
    logger: logging.Logger | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
) -> pd.DataFrame:
    """Query Earth Search for Sentinel-2 L2A scenes intersecting the AOI geometries.

    Parameters
    ----------
    aoi_geometries : iterable of shapely geometries
        WGS84 AOI geometries.
    collection : str, optional
        Earth Search collection (an ``EarthSearchCollection`` value).
    years : tuple of int or None
        Acquisition years; ``None`` searches every year.
    speriod, eperiod : tuple of int
        Inclusive ``(month, day)`` season bounds applied within each year.
    logger : logging.Logger or None, optional
        Logger used for query diagnostics.
    progress_callback : callable or None, optional
        Called with ``(completed, total)`` searches.

    Returns
    -------
    pandas.DataFrame
        One row per scene with ``EARTH_SEARCH_COLUMNS``, sorted by ``item_id``.

    Raises
    ------
    ValueError
        If no non-empty AOI geometry is given.
    RuntimeError
        If ``pystac-client`` is not installed.
    """
    logger = logger or logging.getLogger(__name__)
    geometries = [geometry for geometry in aoi_geometries if not geometry.is_empty]
    if not geometries:
        raise ValueError("AOI must contain at least one non-empty geometry")
    try:
        import pystac_client
    except ImportError as error:  # pragma: no cover - dependency is declared
        raise RuntimeError("pystac-client is required for Earth Search queries") from error

    if years is None:
        ranges = ["2015-06-23/.."]
    else:
        ranges = [
            f"{year}-{speriod[0]:02d}-{speriod[1]:02d}/"
            f"{year}-{eperiod[0]:02d}-{eperiod[1]:02d}"
            for year in years
        ]
    batches = _search_batches(geometries)
    tasks = [(batch, date_range) for batch in batches for date_range in ranges]
    logger.info(
        "Querying Earth Search collection=%s with %d search(es)", collection, len(tasks)
    )

    def search(batch: shapely_base.BaseGeometry, date_range: str) -> list[dict[str, Any]]:
        """Run one search and convert every returned item to a row."""
        catalog = pystac_client.Client.open(EARTH_SEARCH_URL)
        results = catalog.search(
            collections=[str(collection)],
            intersects=mapping(batch),
            datetime=date_range,
            max_items=None,
        )
        return [_item_row(item) for item in results.items()]

    rows: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=min(len(tasks), MAX_SEARCH_THREADS)) as pool:
        futures = [pool.submit(search, batch, date_range) for batch, date_range in tasks]
        for completed, future in enumerate(as_completed(futures), start=1):
            rows.extend(future.result())
            if progress_callback is not None:
                progress_callback(completed, len(tasks))
    scenes = pd.DataFrame(rows, columns=EARTH_SEARCH_COLUMNS)
    return (
        scenes.drop_duplicates(subset=["item_id"])
        .sort_values("item_id")
        .reset_index(drop=True)
    )
