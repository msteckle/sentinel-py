# sentinel_py/s2/workflows.py

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import logging
import pandas as pd
from shapely.geometry.base import BaseGeometry

from sentinel_py.common.utils import seasonal_date_ranges
from sentinel_py.common.cdse_search import build_search_query, fetch_all_products
from sentinel_py.common.cdse_auth import AutoRefreshSession
from sentinel_py.s2.cdse_s2_nodes import select_s2_targets
from sentinel_py.s2.cdse_s2_download import download_s2_targets


def download_s2_seasonal_scenes(
    aoi: BaseGeometry,
    output_root: Path,
    *,
    start_year: int,
    end_year: int,
    start_month: int,
    start_day: int,
    end_month: int,
    end_day: int,
    catalogue_odata: str,
    collection_name: str,
    product_type: str,
    bands: Iterable[str],
    target_res_m: int,
    credentials: dict | None = None,
    max_scenes: int | None = None,
    max_workers_files: int = 4,
    log_file: Path | None = None,
) -> pd.DataFrame:
    """
    High-level workflow:
      - For each year in [start_year, end_year], build a seasonal date window.
      - Query CDSE for Sentinel-2 scenes intersecting AOI in that window.
      - For each scene:
          - Choose appropriate band files (select_s2_targets).
          - Download them concurrently into output_root.
      - Return a DataFrame summarizing band resolutions and failures per scene.
    """
    if credentials is None:
        credentials = {}

    # create logger given log_file
    if log_file is not None:
        logging.basicConfig(filename=log_file, level=logging.INFO)
        logger = logging.getLogger(__name__)

    # build yearly date windows
    date_windows = seasonal_date_ranges(
        start_year=start_year,
        end_year=end_year,
        start_month=start_month,
        start_day=start_day,
        end_month=end_month,
        end_day=end_day,
    )

    # query CDSE for each date window and accumulate results
    all_rows: list[dict] = []
    for start_iso, end_iso in date_windows:
        query_url = build_search_query(
            aoi=aoi,
            catalogue_odata=catalogue_odata,
            collection_name=collection_name,
            product_type=product_type,
            start_iso=start_iso,
            end_iso=end_iso,
        )
        if logger:
            logger.info("Querying CDSE: %s", query_url)

        df = fetch_all_products(query_url)  # <- from cdse_search.py
        df = df.assign(
            window_start=start_iso,
            window_end=end_iso,
        )
        all_rows.append(df)

    if not all_rows:
        return pd.DataFrame()

    products = pd.concat(all_rows, ignore_index=True)

    # optionally limit the number of scenes
    if max_scenes is not None and len(products) > max_scenes:
        products = products.iloc[:max_scenes].copy()

    # initiate an authenticated session
    token_cache: dict = {}
    with AutoRefreshSession(credentials=credentials, token_cache=token_cache, logger=logger) as sess:
        # for each scene, select targets and download them
        results: list[dict] = []
        for _, row in products.iterrows():
            scene_id = row.get("Id")  # CDSE product ID
            scene_name = row.get("Name")  # human-readable scene name

            if not scene_id or not scene_name:
                continue

            # determine which files to download for this scene
            targets, safe_root, granule_dir, band_res_map = select_s2_targets(
                session=sess,
                scene_id=str(scene_id),
                scene_name=str(scene_name),
                bands=bands,
                target_res_m=target_res_m,
            )

            if logger:
                logger.info("Scene %s: selected %d targets", scene_name, len(targets))

            # download targets concurrently
            failures = download_s2_targets(
                session=sess,
                scene_id=str(scene_id),
                targets=targets,
                output_root=output_root,
                max_workers=max_workers_files,
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

    return pd.DataFrame(results)