"""
Functions used to download Sentinel-2 scenes from CDSE over (optional) seasonal windows.
"""

import logging
from datetime import date
from pathlib import Path
from typing import Iterable
import calendar

import pandas as pd
import geopandas as gpd
from shapely.geometry.base import BaseGeometry

from sentinel_py.common.cdse_auth import AutoRefreshSession
from sentinel_py.common.cdse_search import build_search_query, fetch_all_products
from sentinel_py.s2.cdse_s2_nodes import select_s2_targets
from sentinel_py.s2.cdse_s2_download import download_s2_targets
from sentinel_py.common.aoi import load_aoi_as_geom


CDSE_CATALOGUE = "https://catalogue.dataspace.copernicus.eu/odata/v1"

def _safe_date_with_adjust(
    year: int, 
    month: int, 
    day: int, 
    logger: logging.Logger
) -> date:
    """
    Return a valid date, adjusting the day if necessary.
    """
    # ensure valid month
    if not (1 <= month <= 12):
        raise ValueError(f"Invalid month: {month}. Month must be between 1 and 12.")

    # determine the last valid day in this month/year
    last_day = calendar.monthrange(year, month)[1]

    # ensure day is positive
    if day <= 0:
        raise ValueError(f"Invalid day: {day} is not a valid calendar day.")

    # ensure day is not too large
    if day <= last_day:
        return date(year, month, day)

    # if day is too large -> adjust
    logger.warning(
        f"Adjusting invalid date {year}-{month:02d}-{day:02d} -> "
        f"{year}-{month:02d}-{last_day:02d}"
    )
    return date(year, month, last_day)


def download_s2_scenes(
    aoi_path: Path,
    output_root: Path,
    *,
    years: Iterable[int],
    period_start: tuple[int, int],  # (month, day)
    period_end: tuple[int, int],  # (month, day)
    collection_name: str,
    product_type: str,
    bands: Iterable[str],
    target_res_m: int,
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
    if logger is None:
        logger = logging.getLogger(__name__)

    # Read the AOI and convert to shapely geometry
    aoi = load_aoi_as_geom(aoi_path)
    if aoi is None:
        raise ValueError(f"Failed to load AOI from {aoi_path}")

    # Ensure list of years has at least one entry
    years = list(years)
    if not years:
        raise ValueError("years must contain at least one year")

    # Build date windows, with “warn and adjust” behavior
    smonth, sday = period_start
    emonth, eday = period_end
    date_windows: list[tuple[date, date]] = []
    for year in years:
        start = _safe_date_with_adjust(year, smonth, sday, logger)
        end = _safe_date_with_adjust(year, emonth, eday, logger)
        if end < start:
            raise ValueError(
                f"period_end {end} is before period_start {start} in year {year}."
            )
        date_windows.append((start, end))

    logger.info(
        "Downloading Sentinel-2 for years=%s, period=%02d-%02d to %02d-%02d",
        years,
        smonth,
        sday,
        emonth,
        eday,
    )

    iso_windows = [
        (f"{s.isoformat()}T00:00:00.000Z", f"{e.isoformat()}T23:59:59.999Z")
        for s, e in date_windows
    ]

    # Start with empty creds; cdse_auth._fill_creds will pull from env.
    credentials: dict = {}
    token_cache: dict = {}
    results: list[dict] = []

    # Query products for each window
    all_rows: list[pd.DataFrame] = []
    for start_iso, end_iso in iso_windows:
        query_url = build_search_query(
            aoi=aoi,
            catalogue_odata=CDSE_CATALOGUE,
            collection_name=collection_name,
            product_type=product_type,
            start_iso=start_iso,
            end_iso=end_iso,
        )
        logger.info("Querying CDSE: %s", query_url)

        df = fetch_all_products(query_url)
        if df.empty:
            logger.warning(
                "No products returned for window %s → %s", start_iso, end_iso
            )
        df = df.assign(window_start=start_iso, window_end=end_iso)
        all_rows.append(df)

    if not all_rows:
        logger.warning("No products found for given AOI and date windows.")
        return pd.DataFrame()

    products = pd.concat(all_rows, ignore_index=True)
    if products.empty:
        logger.warning("Products DataFrame is empty after concatenation.")
        return pd.DataFrame()

    # Some logging about the products found
    logger.info("Total products fetched: %d", len(products))
    logger.info("Example columns: %s", products.columns.tolist()[:10])
    logger.info("First 3 names: %s", products["Name"].head(3).to_list())

    # Open a CDSE session that auto-refreshes tokens.
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
                    "Skipping row with missing Id/Name: %s",
                    row.to_dict(),
                )
                continue

            targets, safe_root, granule_dir, band_res_map = select_s2_targets(
                session=sess,
                scene_id=str(scene_id),
                scene_name=str(scene_name),
                bands=bands,
                target_res_m=target_res_m,
                include_scl=include_scl,
            )

            logger.info(
                "Scene %s (%s): selected %d targets",
                scene_name,
                scene_id,
                len(targets),
            )

            failures = download_s2_targets(
                session=sess,
                scene_id=str(scene_id),
                targets=targets,
                output_root=output_root,
                max_workers=max_workers_files,
                logger=logger,
            )

            if failures:
                logger.warning(
                    "Scene %s (%s): %d target(s) failed to download",
                    scene_name,
                    scene_id,
                    len(failures),
                )
                for f in failures:
                    logger.debug(
                        "  Failed target %s: %s",
                        "/".join(f["segments"]),
                        f["status"],
                    )

            results.append(
                {
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
                }
            )

    logger.info("Finished seasonal download: %d scene(s) processed.", len(results))
    return pd.DataFrame(results)
