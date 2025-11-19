# sentinel_py/s2/workflows.py

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Iterable
import calendar

import pandas as pd
from shapely.geometry.base import BaseGeometry

from sentinel_py.common.utils import seasonal_date_ranges
from sentinel_py.common.cdse_search import build_search_query, fetch_all_products
from sentinel_py.common.cdse_auth import AutoRefreshSession
from sentinel_py.s2.cdse_s2_nodes import select_s2_targets
from sentinel_py.s2.cdse_s2_download import download_s2_targets


CDSE_CATALOGUE = "https://cdse-catalogue.dataspace.copernicus.eu/odata/v1/Products"

def safe_date_with_adjust(
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


def download_s2_seasonal_scenes(
    aoi: BaseGeometry,
    output_root: Path,
    *,
    years: Iterable[int],
    period_start: tuple[int, int],  # (month, day)
    period_end: tuple[int, int],  # (month, day)
    collection_name: str,
    product_type: str,
    bands: Iterable[str],
    target_res_m: int,
    credentials: dict | None = None,
    max_workers_files: int = 4,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    """
    Download Sentinel-2 scenes for a seasonal window repeated over one or more years.

    Parameters
    ----------
    years : iterable of int
        Year(s) to download. For a single year, pass [2020]. Otherwise,
        supply a range: (2020, 2021).
    period_start : (int, int)
        (month, day) for the start of the seasonal window (e.g. (6, 1) for June 1).
    period_end : (int, int)
        (month, day) for the end of the seasonal window (e.g. (8, 31) for Aug 31).
        Assumed to be within the same calendar year as period_start.
    """
    # establish logger
    if logger is None:
        logger = logging.getLogger(__name__)
    if credentials is None:
        credentials = {}

    # ensure years is a list and has at least one year
    years = list(years)
    if not years:
        raise ValueError("years must contain at least one year")

    # build date windows for each year
    smonth, sday = period_start
    emonth, eday = period_end
    date_windows = []
    for year in years:
        # adjust month end day if necessary
        start = safe_date_with_adjust(year, smonth, sday, logger)
        end = safe_date_with_adjust(year, emonth, eday, logger)
        # ensure end is not before start
        if end < start:
            raise ValueError(
                f"Period_end {end} is before period_start {start} in year {year}."
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

    # convert to ISO strings for each date range
    iso_windows = (
        [(sdate.isoformat(), edate.isoformat()) for sdate, edate in date_windows]
    )

    # query and fetch products for each date window
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

        # request products for this date window
        df = fetch_all_products(query_url)
        df = df.assign(window_start=start_iso, window_end=end_iso)
        all_rows.append(df)

    # check if any products found
    if not all_rows:
        logger.warning("No products found for given AOI and date windows.")
        return pd.DataFrame()
    products = pd.concat(all_rows, ignore_index=True)

    token_cache: dict = {}
    with AutoRefreshSession(
        credentials=credentials,
        token_cache=token_cache,
        logger=logger,
    ) as sess:
        results: list[dict] = []
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
            )

            logger.info(
                "Scene %s (%s): selected %d targets",
                scene_name,
                scene_id,
                len(targets),
            )

            # download in parallel
            failures = download_s2_targets(
                session=sess,
                scene_id=str(scene_id),
                targets=targets,
                output_root=output_root,
                max_workers=max_workers_files,
            )

            # log any failures
            if failures:
                logger.warning(
                    "Scene %s (%s): %d target(s) failed to download",
                    scene_name,
                    scene_id,
                    len(failures),
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