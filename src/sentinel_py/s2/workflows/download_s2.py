"""
Functions used to download Sentinel-2 scenes from CDSE over (optional) seasonal windows.
"""

import logging
from datetime import date, datetime
from pathlib import Path
import sys
from typing import Iterable
import calendar
import geopandas as gpd
import hashlib
import json
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

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

    # try to build YYYY-MM-DD date
    try:
        return date(year, month, day)
    # if date invalid, adjust day down to last valid day of month and log a warning
    except ValueError as e:
        logger.warning(
            f"Invalid date {year}-{month:02d}-{day:02d}: {e}. "
            f"Adjusting to last valid day of month."
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
    max_workers_files: int = 2,
    logger: logging.Logger | None = None,
) -> None:
    """
    Download Sentinel-2 scenes for a seasonal window repeated over one or more years.
    Authentication is pulled from environment variables via cdse_auth._fill_creds:
      - CDSE_USERNAME
      - CDSE_PASSWORD or CDSE_PASSWORD_FILE

    Parameters
    ----------
    aoi : Path
        Path to AOI file (e.g. GeoJSON or Shapefile) defining the area of interest for 
        the search query.
    crs : str
        Coordinate reference system for the AOI.
    outdir : Path
        Directory where downloaded files will be saved.
    years : Iterable[int]
        One or more years to repeat the seasonal window search and download (e.g. [2020,
        2021, 2022]).
    speriod : datetime
        Start month and day for the seasonal window (the year is ignored, e.g. Jan 15).
    eperiod : datetime
        End month and day for the seasonal window (the year is ignored, e.g. Mar 15).
    s2collection : str
        Sentinel-2 collection to search (e.g. "Sentinel-2 L2A").
    s2product : str
        Sentinel-2 product type to search (e.g. "S2MSI2A").
    s2bands : Iterable[str]
        Sentinel-2 bands to download (e.g. ["B04", "B08"]). Must be compatible with the 
        specified collection and product.
    s2res : int
        Spatial resolution of the Sentinel-2 bands to download (e.g. 10, 20, 60).
    include_scl : bool, optional
        Whether to include the Scene Classification Layer (SCL) in the downloads.
        Default is True.
    max_workers_files : int, optional
        Maximum number of parallel download threads for files within each scene. 
        Default is 2.
    logger : logging.Logger, optional
        Logger for recording progress and errors. If None, the module logger is used.
        Default is None.
    """

    # import here to avoid circular imports
    from sentinel_py.common.cdse_auth import AutoRefreshSession
    from sentinel_py.common.cdse_search import (
        build_search_query,
        all_query_results,
        batch_geometries,
    )
    from sentinel_py.s2.cdse_s2_nodes import select_s2_targets
    from sentinel_py.s2.cdse_s2_download import download_s2_targets
    from sentinel_py.common.aoi import aoi_as_geom

    # set up logger
    if logger is None:
        logger = logging.getLogger(__name__)

    # convert AOI to geometry
    aoi = aoi_as_geom(aoi, crs)

    # validate and build date windows for each year, adjusting invalid dates as needed
    years = list(years)
    if not years:
        raise ValueError("Years must contain at least one year")
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

    # generate cache key and check for cached products before querying CDSE
    cache_key = _query_cache_key(aoi, s2collection, s2product, iso_windows)
    cache_dir = Path.cwd() / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    products_cache = cache_dir / f"products_{cache_key}.parquet"

    # if cached products exist, load them
    if products_cache.exists():
        logger.info(f"Loading cached products from {products_cache}")
        products = pd.read_parquet(products_cache)
    # otherwise, proceed with download
    else:
        # batch the AOI if the query has too many characters
        aoi_batches = batch_geometries(aoi)
        logger.info(f"AOI split into {len(aoi_batches)} batch(es) for querying")

        # query CDSE for each date window and AOI batch
        all_rows: list[pd.DataFrame] = []
        for start_iso, end_iso in iso_windows:
            logger.info(f"Querying CDSE for requested window: {start_iso} -> {end_iso}")

            # build and execute query for each AOI batch
            for i, batch_geom in enumerate(aoi_batches):
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
                        f"No products returned for window {start_iso} -> {end_iso} "
                        f"(batch {i + 1} of {len(aoi_batches)})")
                else:
                    logger.info(
                        f"Query returned {len(df)} products for window {start_iso} -> "
                        f"{end_iso} (batch {i + 1} of {len(aoi_batches)})"
                    )
                    df = df.assign(window_start=start_iso, window_end=end_iso)
                    all_rows.append(df)

        # if no products were found, log a warning and return
        if not all_rows:
            logger.warning("No products found for given AOI and date windows.")
            return

        # concatenate results, drop duplicates, and cache the products found
        products = pd.concat(all_rows, ignore_index=True).drop_duplicates(subset="Id")
        logger.info(
            f"Found {len(products)} unique products across {len(iso_windows)} "
            f"window(s) and {len(aoi_batches)} batch(es)"
        )

        # cache the products dataframe for future re-runs
        outdir.mkdir(parents=True, exist_ok=True)
        products.to_parquet(products_cache)
        logger.info(f"Cached products to {products_cache}")

    # set up session for downloads (with auto-refreshing credentials) and download
    n_scenes = 0
    credentials: dict = {}
    token_cache: dict = {}
    with logging_redirect_tqdm():
        with tqdm(
            total=len(products),
            desc="Downloading scenes",
            unit="scene",
            disable=not sys.stdout.isatty(),
        ) as pbar:
            with AutoRefreshSession(
                credentials=credentials,
                token_cache=token_cache,
                logger=logger,
            ) as sess:
                for _, row in products.iterrows():
                    scene_id = row.get("Id")
                    scene_name = row.get("Name")

                    if not scene_id or not scene_name:
                        logger.warning(
                            f"Skipping row with missing Id/Name: {row.to_dict()}"
                        )
                        continue

                    try:
                        targets = select_s2_targets(
                            session=sess,
                            scene_id=str(scene_id),
                            scene_name=str(scene_name),
                            bands=s2bands,
                            target_res_m=s2res,
                            include_scl=include_scl,
                        )
                        n_failures = download_s2_targets(
                            session=sess,
                            scene_id=str(scene_id),
                            targets=targets,
                            output_root=outdir,
                            max_workers=max_workers_files,
                            logger=logger,
                        )
                    except Exception as e:
                        logger.error(
                            f"Scene {scene_name} ({scene_id}): unexpected error: {e}",
                            exc_info=True,
                        )
                        continue

                    logger.info(
                        f"Scene {scene_name} ({scene_id}): processed {len(targets)} "
                        f"targets with {n_failures} failure(s)"
                    )
                    n_scenes += 1

                    pbar.update(1)
                    pbar.set_postfix(
                        processed=n_scenes, 
                        failures=len(products) - n_scenes
                    )

    logger.info(f"Finished seasonal download: {n_scenes} scene(s) processed.")
