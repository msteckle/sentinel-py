import calendar
import logging
import shutil
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Callable, Dict, Optional

import pandas as pd
import phidown.search as _phidown_search
from phidown.s5cmd_utils import run_s5cmd_with_config
from phidown.search import CopernicusDataSearcher
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from sentinel_py.cache import (
    cache_directory,
    deterministic_cache_key,
    find_latest_cache_file,
    mark_cache_used,
    merge_state_rows,
    write_json_atomic,
    write_parquet_atomic,
)

# fix some phidown limitations
_phidown_search.REQUEST_TIMEOUT_SECONDS = 120

S2_BAND_RESOLUTIONS: dict[str, list[int]] = {
    "B01": [60],
    "B02": [10, 20, 60],
    "B03": [10, 20, 60],
    "B04": [10, 20, 60],
    "B05": [20, 60],
    "B06": [20, 60],
    "B07": [20, 60],
    "B08": [10],
    "B8A": [20, 60],
    "B09": [60],
    "B11": [20, 60],
    "B12": [20, 60],
    "SCL": [20, 60],
    "TCI": [10, 20, 60],
    "AOT": [10, 20, 60],
    "WVP": [10, 20, 60],
}

# Canonical ordering finest -> coarsest
RESOLUTIONS = [10, 20, 60]

# Bands that allow coarser fallback (SCL is the main one)
COARSER_FALLBACK_BANDS = {"SCL"}

# Columns used as the unique key for an image row in all_downloaded_images.parquet
IMAGE_KEY_COLS = ["safedir", "band_name", "resolution_m"]

########################################################################################
# Variable helpers
########################################################################################


def _fix_date(year: int, month: int, day: int, logger: logging.Logger) -> date:
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


########################################################################################
# Cache helpers
########################################################################################

"""
Sentinel-py caches:
- Query results (the .SAFE scenes found for a particular CDSE query)
    - Each unique query gets its own cache directory named by a hash of the parameters
    that the user specified (AOI, date windows, collection/product, etc); this way, if
    you want to re-run a query with the same parameters, you get the cached results 
    immediately without hitting the CDSE API again.
- Download results
    - The downloaded images (jp2 files) for all queries are cached into a single parquet
    file with information about the .SAFE directory name, full S3 path, relative S3 
    path, band, resolution, expected size (from S3), and actual size (from disk after 
    download). This allows the download step to verify if the expected images already 
    exist on disk and are valid (actual size matches expected), and if so, skip the 
    download while still recording the actual size (if it doesn't already exist) in the 
    cache for future runs.
"""


def query_cache_key(
    aoi_wkt: str,
    collection_name: str,
    product_type: Optional[str],
    iso_windows: list[tuple[str, str]],
    orbit: str | None = None,
    cloud_thresh: float | None = None,
    burst_id: int | None = None,
    swath_id: str | None = None,
    rel_orbit_num: int | None = None,
    ops_mode: str | None = None,
    platform_serial_id: str | None = None,
) -> str:
    """
    Generate a hash key from query parameters. This hash key will be used as the name of
    the cache directory for the query results.
    """
    payload = {
        "collection": collection_name,
        "product": product_type,
        "windows": iso_windows,
        "aoi_wkt": aoi_wkt,
        "orbit": orbit,
        "cloud_thresh": cloud_thresh,
        "burst_id": burst_id,
        "swath_id": swath_id,
        "rel_orbit_num": rel_orbit_num,
        "ops_mode": ops_mode,
        "platform_serial_id": platform_serial_id,
    }
    return deterministic_cache_key(payload)


def query_cache_dir(cache_root: Path, cache_key: str) -> Path:
    """
    Get or create the cache directory for a query given a parent directory and cache
    key.
    """
    return cache_directory(cache_root, cache_key)


def save_query_as_json(query_dir: Path, **kwargs) -> None:
    """
    Save query parameters as a human-readable JSON inside the query cache directory.
    """
    info = {k: str(v) if isinstance(v, (Path, date)) else v for k, v in kwargs.items()}
    info["created"] = datetime.now().isoformat()
    write_json_atomic(query_dir / "query_info.json", info)


def find_latest_scenes_cache(cache_root: Path) -> Optional[Path]:
    """
    Find the most recently used scenes.parquet across all query cache directories.
    """
    return find_latest_cache_file(cache_root, "scenes.parquet")


def write_protected_parquet(df: pd.DataFrame, path: Path) -> None:
    """Write a parquet file and set it as read-only to prevent accidental deletion."""
    write_parquet_atomic(df, path, index=True, read_only=True)


def _merge_image_rows(existing: pd.DataFrame, new_rows: list[dict]) -> pd.DataFrame:
    """Merge new/updated image row to the downloaded images cache."""
    return merge_state_rows(existing, new_rows, key_columns=IMAGE_KEY_COLS)


########################################################################################
# 1) Query the CDSE Catalogue
########################################################################################


def query_cdse(
    collection: str,
    product: Optional[str],
    years: list[int],
    speriod: date,
    eperiod: date,
    aoi: Path,
    crs: str,
    cache_dir: Path,
    orbit: str | None = None,
    cloud_thresh: float | None = None,
    attrs: Dict[str, str | int | float] | None = None,
    # burst related filters (S1 relevant)
    burst_mode: bool = False,
    burst_id: int | None = None,
    abs_burst_id: int | None = None,
    swath_id: str | None = None,
    parent_product_name: str | None = None,
    parent_product_type: str | None = None,
    parent_product_id: str | None = None,
    datatake_id: int | None = None,
    rel_orbit_num: int | None = None,
    ops_mode: str | None = None,
    pol_channels: str | None = None,
    platform_serial_id: str | None = None,
    # pagination parameters
    top: int = 1000,
    count: bool = False,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    """
    Using the phidown CompernicusDataSearcher, query the CDSE Catalogue for scenes
    matching the given parameters. Results are cached to avoid redundant queries, and a
    progress bar is displayed during the query process. Reference phidown docs for
    details on the parameters: https://esa-philab.github.io/phidown/api/phidown/search/index.html#phidown.search.CopernicusDataSearcher
    """

    from sentinel_py.aoi import aoi_as_geom, batch_geometries

    # ----------------------------------------------------------------------------------
    # Clean up and validate parameters, and prepare query windows
    # ----------------------------------------------------------------------------------

    # define logger if none provided
    logger = logger or logging.getLogger(__name__)

    # ensure AOI is geometry object
    aoi_geom = aoi_as_geom(aoi, crs)

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

    # generate scene cache key based on the query parameters
    cache_key = query_cache_key(
        aoi_geom.union_all().wkt, collection, product, iso_windows
    )
    query_dir = query_cache_dir(cache_dir, cache_key)
    scenes_cache = query_dir / "scenes.parquet"

    # if cached products exist, don't bother querying
    if scenes_cache.exists():
        logger.info(f"Loading cached products from {scenes_cache}")
        mark_cache_used(scenes_cache)
        return pd.read_parquet(scenes_cache)

    # otherwise, proceed with querying
    # if AOI is too large/complex, the CDSE API may reject it. To mitigate this, we
    # split the AOI into batches of geometries and query each batch separately,
    # then combine the results.
    aoi_batches = batch_geometries(aoi_geom)
    logger.info(f"AOI split into {len(aoi_batches)} batch(es) for querying")

    def _run_query(
        start_iso: str, end_iso: str, batch_idx: int, batch_geom
    ) -> pd.DataFrame:
        searcher = CopernicusDataSearcher()
        searcher.query_by_filter(
            collection_name=collection,
            product_type=product,
            orbit_direction=orbit,
            cloud_cover_threshold=cloud_thresh,
            attributes=attrs,
            aoi_wkt=batch_geom.wkt,
            start_date=start_iso,
            end_date=end_iso,
            burst_mode=burst_mode,
            burst_id=burst_id,
            absolute_burst_id=abs_burst_id,
            swath_identifier=swath_id,
            parent_product_name=parent_product_name,
            parent_product_type=parent_product_type,
            parent_product_id=parent_product_id,
            datatake_id=datatake_id,
            relative_orbit_number=rel_orbit_num,
            operational_mode=ops_mode,
            polarisation_channels=pol_channels,
            platform_serial_identifier=platform_serial_id,
            top=top,
            count=count,
        )
        df = searcher.execute_query()
        num_rows = len(df) if df is not None else 0
        logger.info(
            f"Window {start_iso} -> {end_iso}, batch {batch_idx + 1}/"
            f"{len(aoi_batches)}: {num_rows} scene(s)"
        )
        return df if df is not None else pd.DataFrame()

    # build all (window × batch) tasks
    tasks = [
        (start_iso, end_iso, i, batch_geom)
        for start_iso, end_iso in iso_windows
        for i, batch_geom in enumerate(aoi_batches)
    ]

    all_rows: list[pd.DataFrame] = []
    max_workers = min(len(tasks), 8)

    # run queries in parallel with a progress bar, and collect results
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
    ) as progress:
        task_id = progress.add_task("Querying CDSE", total=len(tasks))
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(_run_query, *task): task for task in tasks}
            for future in as_completed(futures):
                try:
                    df = future.result()
                    if not df.empty:
                        all_rows.append(df)
                except Exception as e:
                    task = futures[future]
                    logger.error(
                        f"Query failed for window {task[0]} -> {task[1]}, "
                        f"batch {task[2] + 1}: {e}"
                    )
                progress.advance(task_id)

    # if no scenes found across all windows and batches, return empty scenes dataframe
    if not all_rows:
        logger.warning("No scenes found for given AOI and date windows.")
        return pd.DataFrame()

    # otherwise, combine results, drop duplicates, and cache to scenes.parquet
    scenes = pd.concat(all_rows, ignore_index=True).drop_duplicates(subset="Id")
    scenes = scenes[["Id", "Name", "S3Path", "ContentDate", "GeoFootprint"]]
    logger.info(
        f"Found {len(scenes)} unique scenes across {len(iso_windows)} "
        f"window(s) and {len(aoi_batches)} batch(es)"
    )

    # save scenes.parquet and query_info.json within query-specific cache directory
    try:
        scenes.to_parquet(scenes_cache)
        save_query_as_json(
            query_dir,
            collection=collection,
            product=product,
            years=years,
            period=f"{speriod.month:02d}-{speriod.day:02d} to {eperiod.month:02d}-{eperiod.day:02d}",
            aoi=str(aoi),
            cloud_cover=cloud_thresh,
            orbit=orbit,
            num_scenes=len(scenes),
        )
        logger.info(f"Cached scenes to {scenes_cache}")
    except Exception as e:
        logger.error(f"Failed to cache scenes to {scenes_cache}: {e}")

    return scenes


# after X number of days we could overwrite the cache


########################################################################################
# 2) Download based on query results
########################################################################################


# --------------------------------------------------------------------------------------
# Handle when a resolution is not available for a requested band
# --------------------------------------------------------------------------------------
@dataclass
class ResolvedBand:
    """
    An object that stores the band name, the resolved resolution, whether a fallback
    resolution was used, and the original requested resolution (if fallback was used).
    Also has a property to get the resolution directory name (e.g. "R20m") for
    S2 images.
    """

    band: str
    resolution: int
    fallback_used: bool = False
    original_resolution: Optional[int] = None

    @property
    def resolution_dir(self) -> str:
        return f"R{self.resolution}m"

    def __repr__(self) -> str:
        if self.fallback_used:
            return (
                f"{self.band}@{self.resolution}m "
                f"(requested {self.original_resolution}m)"
            )
        return f"{self.band}@{self.resolution}m"


def _resolve_s2_band(band: str, requested_res: int) -> ResolvedBand:
    """
    Return a ResolvedBand object containing information about the band name, the
    requested resolution, the resolved (fixed) resolution, and whether a fallback
    resolution was used. If someone requests a resolution that we know (without
    hitting S3) isn't available for that band, we can immediately resolve to the
    closest available resolution and log a warning to continue working and prevent
    hitting errors later.
    """
    available = S2_BAND_RESOLUTIONS[band]

    # exact match
    if requested_res in available:
        return ResolvedBand(band=band, resolution=requested_res)

    # try finer resolutions first
    finer = [r for r in RESOLUTIONS if r < requested_res and r in available]
    if finer:
        chosen = max(finer)  # closest to requested but finer
        return ResolvedBand(
            band=band,
            resolution=chosen,
            fallback_used=True,
            original_resolution=requested_res,
        )

    # for SCL, coarser fallback is acceptable
    coarser = [r for r in RESOLUTIONS if r > requested_res and r in available]
    if coarser:
        if band in COARSER_FALLBACK_BANDS:
            chosen = min(coarser)  # closest coarser
            return ResolvedBand(
                band=band,
                resolution=chosen,
                fallback_used=True,
                original_resolution=requested_res,
            )
        # for other bands, coarser is last resort
        chosen = min(coarser)
        return ResolvedBand(
            band=band,
            resolution=chosen,
            fallback_used=True,
            original_resolution=requested_res,
        )

    raise ValueError(
        f"No resolution available, are you sure {band} is a valid S2 band?"
    )


def _resolve_s2_bands(
    bands: list[str], requested_res: int, logger: logging.Logger
) -> list[ResolvedBand]:
    """
    Resolve a list of S2 bands for a requested resolution to known existing
    resolutions. Returns a list of ResolvedBand objects (see above).
    """
    bands = [b.upper() for b in bands]
    resolved = [_resolve_s2_band(b, requested_res) for b in bands]

    for r in resolved:
        if r.fallback_used:
            logger.warning(
                f"  -> {r.band}: requested {r.original_resolution}m, "
                f"resolved to {r.resolution}m"
            )

    return resolved


# --------------------------------------------------------------------------------------
# Determine the images we want to download for a scene, and cache the results
# --------------------------------------------------------------------------------------


def _parse_s5cmd_ls_line(line: str) -> Optional[tuple[int, str]]:
    """Get size, rel_path, or None from s5cmd `ls` output line."""
    parts = line.strip().split()
    if len(parts) >= 4:
        try:
            size = int(parts[2])
            rel_path = parts[-1]
            return size, rel_path
        except ValueError:
            pass
    return None


def _find_s2_scene_images(
    scene_name: str,
    s3_path: str,
    resolved: list[ResolvedBand],
    config_file: str,
    logger: logging.Logger,
) -> list[dict]:
    """
    Find S2 images in a scene (.SAFE directory) by querying S3 directly given resolved
    bands.
    """
    s3_path = s3_path.removeprefix("/eodata")
    is_l1c = "MSIL1C" in scene_name.upper()
    images = []

    # loop through each resolved band
    for rb in resolved:
        # get the S3 pattern to query based on whether it's L1C or L2A
        if is_l1c:
            pattern = f"s3://eodata{s3_path}/GRANULE/*/IMG_DATA/*_{rb.band}.jp2"
        else:
            pattern = (
                f"s3://eodata{s3_path}/GRANULE/*/IMG_DATA/"
                f"R{rb.resolution}m/*_{rb.band}_{rb.resolution}m.jp2"
            )

        # find images for a scene and band by querying S3 directly using `s5cmd ls`
        cmd = f'ls "{pattern}"'
        try:
            output = run_s5cmd_with_config(cmd, config_file=config_file)
        except FileNotFoundError as e:
            raise RuntimeError(
                f"s5cmd not found: ensure s5cmd is installed and in PATH: {e}"
            ) from e
        except Exception:
            logger.warning(
                f"  ERR {rb.band}@{rb.resolution}m: not found in {scene_name}",
                exc_info=True,
            )
            continue

        # parse the results of `s5cmd ls` and append information about the images
        found = False
        for line in output.strip().splitlines():
            parsed = _parse_s5cmd_ls_line(line)
            if parsed:
                expected_size, rel_path = parsed
                if not rel_path.startswith("GRANULE/"):
                    rel_path = f"GRANULE/{rel_path}"
                images.append(
                    {
                        "safedir": scene_name,
                        "s3_path": s3_path,
                        "band_name": rb.band,
                        "resolution_m": rb.resolution if not is_l1c else 0,
                        "img_path_in_safedir": rel_path,
                        "s3_expected_size": expected_size,
                        "local_actual_size": None,
                    }
                )
                found = True
                break

        # if no images found for this band, log a warning
        # (sometimes what CDSE says is available doesn't actually exist in S3)
        if not found:
            logger.warning(f"  ERR {rb.band}: not found in {scene_name}")

    return images


def _find_s1_scene_images(
    scene_name: str,
    s3_path: str,
    polarisations: list[str],
    config_file: str,
    logger: logging.Logger,
) -> list[dict]:
    """Find S1 images in a scene by querying S3 directly for each polarisation."""
    s3_path = s3_path.removeprefix("/eodata")
    images = []

    for pol in [p.upper() for p in polarisations]:
        pattern = f"s3://eodata{s3_path}/measurement/*-{pol.lower()}-*.tiff"
        cmd = f'ls "{pattern}"'
        try:
            output = run_s5cmd_with_config(cmd, config_file=config_file)
        except FileNotFoundError as e:
            raise RuntimeError(
                f"s5cmd not found: ensure s5cmd is installed and in PATH: {e}"
            ) from e
        except Exception:
            logger.warning(
                f"  ERR {pol}: not found in {scene_name}",
                exc_info=True,
            )
            continue

        found = False
        for line in output.strip().splitlines():
            parsed = _parse_s5cmd_ls_line(line)
            if parsed:
                expected_size, rel_path = parsed
                if not rel_path.startswith("measurement/"):
                    rel_path = f"measurement/{rel_path}"
                images.append(
                    {
                        "safedir": scene_name,
                        "s3_path": s3_path,
                        "band_name": pol,
                        "resolution_m": 0,
                        "img_path_in_safedir": rel_path,
                        "s3_expected_size": expected_size,
                        "local_actual_size": None,
                    }
                )
                found = True
                break

        if not found:
            logger.warning(f"  ERR {pol}: not found in {scene_name}")

    return images


# --------------------------------------------------------------------------------------
# Download one image file
# --------------------------------------------------------------------------------------


def download_s3_file(
    s3_uri: str,
    local_path: Path,
    logger: logging.Logger,
    config_file: str = ".s5cfg",
    endpoint_url: str = "https://eodata.dataspace.copernicus.eu",
) -> bool:
    """Download a single file from S3."""
    local_path.parent.mkdir(parents=True, exist_ok=True)

    cmd = f'cp "{s3_uri}" "{local_path}"'
    try:
        run_s5cmd_with_config(cmd, config_file=config_file, endpoint_url=endpoint_url)
        return True
    except Exception as e:
        logger.error(f"Download failed: {s3_uri} -> {local_path}: {e}")
        return False


# --------------------------------------------------------------------------------------
# Download multiple image files for one scene
# --------------------------------------------------------------------------------------


@dataclass
class DownloadResult:
    """
    Result of downloading images for a single scene, with lists of succeeded, failed,
    and skipped images (identified by band@resolution), plus updated image rows
    (with actual_size populated) ready to be merged into the images cache.
    """

    scene_name: str
    succeeded: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    updated_images: list[dict] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.succeeded) + len(self.failed) + len(self.skipped)

    @property
    def ok(self) -> bool:
        return len(self.failed) == 0


def _download_scene_from_images(
    scene_name: str,
    s3_path: str,
    images: list[dict],
    output_dir: Path,
    config_file: str,
    parallel_bands: int,
    logger: logging.Logger,
) -> DownloadResult:
    """
    Download pre-resolved images for a single scene, preserving SAFE structure.

    For each image we compare s3_expected_size (from S3) against local_actual_size (on
    disk). Rules:

      * Cache already knows local_actual_size == s3_expected_size; trust it, skip
        without stat-ing the file.
      * File on disk and its size matches s3_expected_size; skip. If the cache's
        local_actual_size is missing or stale, emit an updated row so it gets
        persisted.
      * File on disk but size != s3_expected_size, so warn and re-download.
      * File missing, download.

    After any successful download, local_actual_size is re-measured from disk and
    attached to an updated image row. Those rows are returned via
    DownloadResult.updated_images so the caller can merge them into the
    images cache (keyed on Name/band/resolution, so they overwrite the
    existing entry).
    """
    result = DownloadResult(scene_name=scene_name)
    scene_root = output_dir / scene_name

    # (label, s3_uri, local_path, image_dict)
    download_tasks: list[tuple[str, str, Path, dict]] = []

    for t in images:
        band = t["band_name"]
        res = t.get("resolution_m", 0)
        label = f"{band}@{res}m" if res else band
        rel_path = t["img_path_in_safedir"]
        expected = t.get("s3_expected_size", 0)
        cached_actual = t.get("local_actual_size")
        local_path = scene_root / rel_path

        # The cache records what was observed after an earlier download, but it is not
        # proof that the file still exists. Always check the current filesystem so a
        # deleted scene or band is restored on the next run.
        if local_path.exists():
            local_size = local_path.stat().st_size

            # if stat-ed file size matches s3_expected_size...
            if expected and local_size == expected:
                # if the cached local_actual_size isn't populated or is stale, update it
                if cached_actual != local_size:
                    result.updated_images.append({**t, "local_actual_size": local_size})
                result.skipped.append(label)
                continue

            # if stat-ed file size doesn't match s3_expected_size, warn & re-download
            if expected and local_size != expected:
                logger.warning(
                    f"  SIZE MISMATCH {scene_name} / {label}: "
                    f"local={local_size} expected={expected} -- re-downloading"
                )
                # fall through to the download path below
            # if S3 doesn't have an expected size, just trust what we have
            elif not expected and local_size > 0:
                if cached_actual != local_size:
                    result.updated_images.append({**t, "local_actual_size": local_size})
                result.skipped.append(label)
                continue

        # build the s3_uri and add to download tasks
        s3_uri = f"s3://eodata{s3_path}/{rel_path}"
        download_tasks.append((label, s3_uri, local_path, t))

    # if there's nothing to download, we're done
    if not download_tasks:
        logger.info(
            f"All images for {scene_name} already exist and are valid, skipping."
        )
        return result

    # otherwise, download in parallel
    def _dload(
        task: tuple[str, str, Path, dict],
    ) -> tuple[str, bool, Path, dict]:
        label, uri, local, image = task
        logger.debug(f"  DWNLD {scene_name} / {label} starting...")
        ok = download_s3_file(uri, local, logger=logger, config_file=config_file)
        return label, ok, local, image

    with ThreadPoolExecutor(max_workers=parallel_bands) as pool:
        futures = {pool.submit(_dload, t): t for t in download_tasks}
        for future in as_completed(futures):
            label, ok, local_path, image = future.result()
            if ok:
                result.succeeded.append(label)
                # measure local_actual_size from the freshly written file so it gets
                # persisted to the cache
                try:
                    actual = local_path.stat().st_size
                except OSError as e:
                    logger.warning(f"  could not stat {local_path} after download: {e}")
                    actual = None
                result.updated_images.append({**image, "local_actual_size": actual})
                logger.debug(f"  DWNLDED {scene_name} / {label}")
            else:
                result.failed.append(label)
                logger.debug(f"  ERRED {scene_name} / {label}")

    return result


# --------------------------------------------------------------------------------------
# Download multiple scenes given cached query results
# --------------------------------------------------------------------------------------


def resolve_and_download(
    scenes_cache: Path,
    mission: str,
    bands: list[str],
    resolution: int,
    output_dir: Path,
    config_file: str = ".s5cfg",
    parallel_scenes: int = 2,
    parallel_bands: int = 2,
    logger: Optional[logging.Logger] = None,
) -> list[DownloadResult]:
    """
    Resolve + download pipeline.

    For each scene:
      1. If already in images cache, skip band resolve, then verify each local file
         exists and matches s3_expected_size before skipping its download.
      2. Otherwise: resolve band via targeted S3 ls, download, & append to cache.

    Parameters
    ----------
    scenes_cache : Path
        Path to scenes.parquet (inside a query cache dir).
    mission : str
        "S1" or "S2".
    bands : list[str]
        Band names to download.
    resolution : int
        Requested resolution in metres (S2 only).
    output_dir : Path
        Root output directory.
    config_file : str
        Path to s5cmd config.
    parallel_scenes : int
        Concurrent scene downloads (keep ≤4 for free accounts).
    parallel_bands : int
        Concurrent band downloads per scene.
    logger : logging.Logger
        Optional logger instance.

    Returns
    -------
    list[DownloadResult]
        List of DownloadResult, one per scene.
    """

    # set up logger if none
    logger = logger or logging.getLogger(__name__)

    # load scenes found for this query
    scenes = pd.read_parquet(scenes_cache)
    required_cols = {"Name", "S3Path"}
    if not required_cols.issubset(scenes.columns):
        raise ValueError(
            f"Scenes parquet missing required columns: "
            f"{required_cols - set(scenes.columns)}"
        )
    logger.info(f"Loaded {len(scenes)} scenes from {scenes_cache}")

    # load images (results of all queries so far)
    images_file = Path(scenes_cache).parent.parent / "all_downloaded_images.parquet"
    logger.info(f"Looking for images cache at: {images_file}")
    if images_file.exists():
        cached_images = pd.read_parquet(images_file)
        # ensure local_actual_size column exists for back-compat with older caches
        if "local_actual_size" not in cached_images.columns:
            cached_images["local_actual_size"] = None
        cached_keys = set(
            zip(
                cached_images["safedir"],
                cached_images["band_name"],
                cached_images["resolution_m"],
            )
        )
        cached_scene_names = set(cached_images["safedir"].unique())
        logger.info(
            f"Found {len(cached_scene_names)} scenes ({len(cached_images)} rows) in {images_file}"
        )
        logger.info(f"First 3 cached scenes: {list(cached_scene_names)[:3]}")
        logger.info(f"First 3 query scenes: {scenes['Name'].head(3).tolist()}")
    else:
        logger.info(f"No images cache at {images_file}")
        cached_images = pd.DataFrame()
        cached_keys = set()
        cached_scene_names = set()

    # resolve bands once (logs fallbacks once) — must be before _scene_fully_cached
    if mission.upper() == "S2":
        resolved_bands = _resolve_s2_bands(bands, resolution, logger)
    else:
        resolved_bands = None

    # split scenes; a scene is only "fully cached" if ALL requested bands are cached
    def _scene_fully_cached(name: str) -> bool:
        """Check if all requested bands for this scene are in the cached keys."""
        if name not in cached_scene_names:
            return False
        bands_to_check = (
            resolved_bands
            if mission.upper() == "S2" and resolved_bands is not None
            else [ResolvedBand(band=b, resolution=0) for b in bands]
        )
        for rb in bands_to_check:
            if (name, rb.band, rb.resolution) not in cached_keys:
                return False
        return True

    fully_cached = scenes["Name"].apply(_scene_fully_cached)
    uncached_df = scenes[~fully_cached]
    cached_df = scenes[fully_cached]

    logger.info(
        f"Total: {len(scenes)} scenes — "
        f"{len(cached_keys)} cached, {len(uncached_df)} to resolve"
    )

    all_results: list[DownloadResult] = []
    # rows to append/overwrite in the images cache on next flush
    pending_image_rows: list[dict] = []
    lock = threading.Lock()
    cancel = threading.Event()
    s3_paths = dict(zip(scenes["Name"], scenes["S3Path"]))

    def _flush_images(force: bool = False) -> None:
        """Merge pending rows into the on-disk images cache."""
        with lock:
            if not pending_image_rows:
                return
            if not force and len(pending_image_rows) < 1000:
                return
            rows_to_write = list(pending_image_rows)
            pending_image_rows.clear()

        existing = (
            pd.read_parquet(images_file) if images_file.exists() else pd.DataFrame()
        )
        if not existing.empty and "local_actual_size" not in existing.columns:
            existing["local_actual_size"] = None
        combined = _merge_image_rows(existing, rows_to_write)
        write_protected_parquet(combined, images_file)
        logger.info(f"Flushed {len(rows_to_write)} image row(s) to cache")

    # Uncached scenes: resolve + download ----------------------------------------------
    def _resolve_and_dl(row: pd.Series) -> DownloadResult:

        # if fatal error, skip all this and return empty result
        if cancel.is_set():
            return DownloadResult(scene_name=row["Name"])

        name = row["Name"]
        raw_s3_path = row["S3Path"]
        s3_path = raw_s3_path.removeprefix("/eodata")

        # always resolve via S3 so s3_expected_size is authoritative. If the
        # files already exist locally, the download step will stat them and,
        # on a size match, skip + record local_actual_size into the cache
        if mission.upper() == "S2":
            if resolved_bands is None:
                logger.error(f"resolved_bands is None for S2 scene: {name}")
                return DownloadResult(scene_name=name)
            images = _find_s2_scene_images(
                name, raw_s3_path, resolved_bands, config_file, logger=logger
            )
            logger.info(
                f"Scene {name}: resolved images: "
                f"{', '.join(f'{t["band_name"]}@{t["resolution_m"]}m' for t in images)}"
            )
        elif mission.upper() == "S1":
            images = _find_s1_scene_images(
                name, raw_s3_path, bands, config_file, logger=logger
            )
            logger.info(
                f"Scene {name}: resolved images: "
                f"{', '.join(f'{t["band_name"]}' for t in images)}"
            )
        else:
            logger.error(f"Unsupported mission: {mission}")
            return DownloadResult(scene_name=name)

        if not images:
            logger.error(f"No images found for scene: {name}")
            return DownloadResult(scene_name=name)

        # seed the cache with the resolved rows (local_actual_size still None). The
        # download step will produce updated rows with local_actual_size populated,
        # which will overwrite these seeds via the merge-by-key logic
        with lock:
            pending_image_rows.extend(images)

        logger.info(f"Scene {name}: downloading {len(images)} image(s)")
        result = _download_scene_from_images(
            scene_name=name,
            s3_path=s3_path,
            images=images,
            output_dir=output_dir,
            config_file=config_file,
            parallel_bands=parallel_bands,
            logger=logger,
        )
        if result.updated_images:
            with lock:
                pending_image_rows.extend(result.updated_images)
        return result

    if not uncached_df.empty:
        uncached_rows = [row for _, row in uncached_df.iterrows()]

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
        ) as progress:
            task_id = progress.add_task(
                "Resolving & downloading new scenes", total=len(uncached_rows)
            )

            with ThreadPoolExecutor(max_workers=parallel_scenes) as pool:
                futures = {
                    pool.submit(_resolve_and_dl, r): r["Name"] for r in uncached_rows
                }
                fatal_error = None
                for future in as_completed(futures):
                    name = futures[future]
                    try:
                        result = future.result()
                        all_results.append(result)
                    except RuntimeError as exc:
                        cancel.set()
                        fatal_error = exc
                        for f in futures:
                            f.cancel()
                        break
                    except Exception as exc:
                        logger.error(f"Scene {name} failed: {exc}")
                        all_results.append(
                            DownloadResult(scene_name=str(name), failed=["SCENE_ERROR"])
                        )

                    _flush_images(force=False)
                    progress.advance(task_id)

                if fatal_error:
                    raise fatal_error

    # final flush for any remaining images from the uncached pass
    _flush_images(force=True)

    # Cached scenes: verify + download only --------------------------------------------
    if not cached_images.empty and not cached_df.empty:
        cached_names = set(cached_df["Name"])
        grouped = [
            (name, group.to_dict("records"))
            for name, group in cached_images.groupby("safedir")
            if name in cached_names
        ]

        if grouped:

            def _dl_cached(name_images: tuple[str, list[dict]]) -> DownloadResult:
                if cancel.is_set():
                    return DownloadResult(scene_name=name_images[0])
                name, images = name_images
                s3_path = s3_paths.get(name, "").removeprefix("/eodata")
                result = _download_scene_from_images(
                    scene_name=name,
                    s3_path=s3_path,
                    images=images,
                    output_dir=output_dir,
                    config_file=config_file,
                    parallel_bands=parallel_bands,
                    logger=logger,
                )
                if result.updated_images:
                    with lock:
                        pending_image_rows.extend(result.updated_images)
                return result

            with Progress(
                SpinnerColumn(),
                TextColumn("[bold blue]{task.description}"),
                BarColumn(),
                MofNCompleteColumn(),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
                TextColumn("[cyan]{task.fields[repair_status]}"),
            ) as progress:
                task_id = progress.add_task(
                    "Checking cached scenes & restoring files",
                    total=len(grouped),
                    repair_status="checking local files...",
                )

                with ThreadPoolExecutor(max_workers=parallel_scenes) as pool:
                    futures = {
                        pool.submit(_dl_cached, (str(ng[0]), ng[1])): str(ng[0])
                        for ng in grouped
                    }
                    fatal_error = None
                    for future in as_completed(futures):
                        name = futures[future]
                        try:
                            result = future.result()
                            all_results.append(result)
                        except RuntimeError as exc:
                            cancel.set()
                            fatal_error = exc
                            for f in futures:
                                f.cancel()
                            break
                        except Exception as exc:
                            logger.error(f"Scene {name} failed: {exc}")
                            all_results.append(
                                DownloadResult(
                                    scene_name=str(name), failed=["SCENE_ERROR"]
                                )
                            )
                        _flush_images(force=False)
                        progress.advance(task_id)

                    if fatal_error:
                        raise fatal_error

            # flush any remaining updates from the cached-scenes pass
            _flush_images(force=True)

    # Summary --------------------------------------------------------------------------
    total_ok = sum(len(r.succeeded) for r in all_results)
    total_fail = sum(len(r.failed) for r in all_results)
    total_skip = sum(len(r.skipped) for r in all_results)

    logger.info("━" * 60)
    logger.info("DOWNLOAD SUMMARY")
    logger.info("━" * 60)
    logger.info(
        f"  {total_ok} succeeded, {total_fail} failed, "
        f"{total_skip} skipped across {len(all_results)} scenes"
    )

    if total_fail > 0:
        failed_scenes = [r for r in all_results if not r.ok]
        for r in failed_scenes[:10]:
            logger.info(f"  ERR {r.scene_name}: {r.failed}")
        if len(failed_scenes) > 10:
            logger.info(f"  ... and {len(failed_scenes) - 10} more")

    return all_results
