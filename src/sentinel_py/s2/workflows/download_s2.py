"""
Functions used to download Sentinel-2 scenes from CDSE over (optional) seasonal windows.
"""

import logging
from datetime import date, datetime
from pathlib import Path
from typing import Iterable
import calendar
import geopandas as gpd
import hashlib
import json

import pandas as pd

# up top in case it ever changes:
CDSE_CATALOGUE = "https://catalogue.dataspace.copernicus.eu/odata/v1"

def _fix_date(
    year: int, 
    month: int, 
    day: int, 
    logger: logging.Logger
) -> date:
    """
    Build datetime from year, month, day, and adjust if the day is invalid for the month
    (e.g. Feb 30 -> Feb 28 or 29) and year (e.g. Feb 29 on non-leap year -> Feb 28).
    """

    try:
        return date(year, month, day)
    except ValueError as e:
        logger.warning(
            "Invalid date %04d-%02d-%02d: %s. Adjusting to last valid day of month.",
            year, month, day, e
        )
        last_day = calendar.monthrange(year, month)[1]
        return date(year, month, last_day)


def _query_cache_key(
    aoi: gpd.GeoSeries,
    collection_name: str,
    product_type: str,
    iso_windows: list[tuple[str, str]],
) -> str:
    """Generate a hash key from query parameters for cache invalidation."""
    payload = {
        "collection": collection_name,
        "product": product_type,
        "windows": iso_windows,
        "aoi_wkt": aoi.union_all().wkt,  # stable representation of the AOI
    }
    return hashlib.md5(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def download_s2_scenes(
    aoi: Path,
    crs: str,
    outdir: Path,
    years: Iterable[int],
    speriod: datetime,
    eperiod: datetime,
    s2collection: str,
    s2product: str,
    s2bands: Iterable[str],
    s2res: int,
    include_scl: bool = True,
    max_workers_files: int = 4,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    """
    Download Sentinel-2 scenes for a seasonal window repeated over one or more years.

    Authentication is pulled from environment variables via cdse_auth._fill_creds:
      - CDSE_USERNAME
      - CDSE_PASSWORD or CDSE_PASSWORD_FILE
    """
    from sentinel_py.common.cdse_auth import AutoRefreshSession
    from sentinel_py.common.cdse_search import (
        build_search_query, 
        all_query_results,
        batch_geometries,
    )
    from sentinel_py.s2.cdse_s2_nodes import select_s2_targets
    from sentinel_py.s2.cdse_s2_download import download_s2_targets
    from sentinel_py.common.aoi import aoi_as_geom

    if logger is None:
        logger = logging.getLogger(__name__)

    # convert aoi to just geometry
    aoi = aoi_as_geom(aoi, crs)

    # ensure list of years has at least one entry
    years = list(years)
    if not years:
        raise ValueError("Years must contain at least one year")

    # handle bad start/end periods (e.g. Feb 30 -> Feb 28 or 29)
    # also converts to date objects if they were passed as datetimes
    date_windows: list[tuple[date, date]] = []
    for year in years:
        start = _fix_date(year, speriod.month, speriod.day, logger)
        end = _fix_date(year, eperiod.month, eperiod.day, logger)
        if end < start:
            raise ValueError(
                f"period_end {end} is before period_start {start} in year {year}."
            )
        date_windows.append((start, end))
    iso_windows = [
        (f"{s.isoformat()}T00:00:00.000Z", f"{e.isoformat()}T23:59:59.999Z")
        for s, e in date_windows
    ]

    # start with empty creds; cdse_auth._fill_creds will pull from environment
    credentials: dict = {}
    token_cache: dict = {}
    results: list[dict] = []

    # set up visible caching for queries since they take a long time
    cache_key = _query_cache_key(aoi, s2collection, s2product, iso_windows)
    products_cache = Path.cwd() / "cache" / f"products_{cache_key}.parquet"
    if products_cache.exists():
        logger.info("Loading cached products from %s", products_cache)
        products = pd.read_parquet(products_cache)
    else:
        # split aoi into batches that fit within URL length limit
        aoi_batches = batch_geometries(aoi)
        logger.info("AOI split into %d batch(es) for querying", len(aoi_batches))

        # query products for each time window and aoi batch
        all_rows: list[pd.DataFrame] = []
        for start_iso, end_iso in iso_windows:
            logger.info("Querying CDSE for requested window: %s -> %s", start_iso, end_iso)
            for batch_geom in aoi_batches:
                query_url = build_search_query(
                    aoi=batch_geom,
                    catalogue_odata=CDSE_CATALOGUE,
                    collection_name=s2collection,
                    product_type=s2product,
                    start_iso=start_iso,
                    end_iso=end_iso,
                )
                df = all_query_results(query_url)
                if df.empty:
                    logger.warning(
                        "No products returned for window %s -> %s (batch %d of %d)",
                        start_iso, end_iso, aoi_batches.index(batch_geom) + 1,
                        len(aoi_batches),
                    )
                else:
                    df = df.assign(window_start=start_iso, window_end=end_iso)
                    all_rows.append(df)

        if not all_rows:
            logger.warning("No products found for given AOI and date windows.")
            return pd.DataFrame()

        products = pd.concat(all_rows, ignore_index=True).drop_duplicates(subset="Id")
        logger.info(
            "Found %d unique products across %d window(s) and %d batch(es)",
            len(products), len(iso_windows), len(aoi_batches),
        )

        # save to cache
        outdir.mkdir(parents=True, exist_ok=True)
        products.to_parquet(products_cache)
        logger.info("Cached products to %s", products_cache)

    # iterate over products and download targets for each product
    with AutoRefreshSession(
        credentials=credentials,
        token_cache=token_cache,
        logger=logger,
    ) as sess:
        # for each product, select targets and download
        for _, row in products.iterrows():
            scene_id = row.get("Id")
            scene_name = row.get("Name")

            if not scene_id or not scene_name:
                logger.warning("Skipping row with missing Id/Name: %s", row.to_dict())
                continue

            try:
                targets, safe_root, granule_dir, band_res_map = select_s2_targets(
                    session=sess,
                    scene_id=str(scene_id),
                    scene_name=str(scene_name),
                    bands=s2bands,
                    target_res_m=s2res,
                    include_scl=include_scl,
                )

                failures = download_s2_targets(
                    session=sess,
                    scene_id=str(scene_id),
                    targets=targets,
                    output_root=outdir,
                    max_workers=max_workers_files,
                    logger=logger,
                )

            except Exception as e:
                logger.error(
                    "Scene %s (%s): unexpected error during download: %s",
                    scene_name, scene_id, e,
                    exc_info=True,
                )
                results.append({
                    "scene_id": scene_id,
                    "scene_name": scene_name,
                    "window_start": row["window_start"],
                    "window_end": row["window_end"],
                    "safe_root": None,
                    "granule_dir": None,
                    "band_res_map": None,
                    "n_targets": 0,
                    "n_failures": 1,
                    "failures": [{"error": str(e)}],
                })
                continue

            logger.info(
                "Scene %s (%s): processed %d targets with %d failure(s)",
                scene_name, scene_id, len(targets), len(failures),
            )

            results.append({
                "scene_id": scene_id,
                "scene_name": scene_name,
                "window_start": row["window_start"],
                "window_end": row["window_end"],
                "safe_root": safe_root,
                "granule_dir": granule_dir,
                "band_res_map": band_res_map,
                "n_targets": len(targets),
                "n_failures": len(failures),
                "failures": failures,
            })

    if not results:
        logger.warning("No scenes were successfully processed.")

    logger.info("Finished seasonal download: %d scene(s) processed.", len(results))
    return pd.DataFrame(results)
