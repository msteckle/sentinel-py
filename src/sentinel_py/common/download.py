import calendar
import hashlib
import json
import logging
import stat
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Optional

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

# fix some phidown limitations
_phidown_search.CopernicusDataSearcher._validate_aoi_wkt = lambda self: None
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


def query_cache_key(
    aoi_wkt: str,
    collection_name: str,
    product_type: str,
    iso_windows: list[tuple[str, str]],
    orbit: str | None = None,
    cloud_thresh: float | None = None,
    burst_id: int | None = None,
    swath_id: str | None = None,
    rel_orbit_num: int | None = None,
    ops_mode: str | None = None,
    platform_serial_id: str | None = None,
) -> str:
    """Generate a hash key from query parameters."""
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
    return hashlib.md5(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def query_cache_dir(cache_root: Path, cache_key: str) -> Path:
    """Get or create the cache directory for a query."""
    d = cache_root / cache_key
    d.mkdir(parents=True, exist_ok=True)
    return d


def save_query_info(query_dir: Path, **kwargs) -> None:
    """Save query parameters as human-readable JSON."""
    info = {k: str(v) if isinstance(v, (Path, date)) else v for k, v in kwargs.items()}
    info["created"] = datetime.now().isoformat()
    (query_dir / "query_info.json").write_text(
        json.dumps(info, indent=2, default=str) + "\n"
    )


def find_latest_scenes_cache(cache_root: Path) -> Optional[Path]:
    """Find the most recently modified scenes.parquet across all query dirs."""
    candidates = sorted(
        cache_root.glob("*/scenes.parquet"),
        key=lambda p: p.stat().st_mtime,
    )
    return candidates[-1] if candidates else None


def write_protected_parquet(df: pd.DataFrame, path: Path) -> None:
    """Write parquet and set read-only to prevent accidental deletion."""
    # temporarily make writable if it already exists as read-only
    if path.exists():
        path.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)
    df.to_parquet(path)
    path.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)


########################################################################################
# 1) Query the CDSE Catalogue
########################################################################################


def query_cdse(
    collection: str,
    product: str,
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

    from sentinel_py.common.aoi import aoi_as_geom, batch_geometries

    # ----------------------------------------------------------------------------------
    # Prepare for querying CDSE Catalogue (search/discovery API)
    # ----------------------------------------------------------------------------------

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

    # generate cache key based on the query parameters
    # if a parquet with the cache key exists, it means all the products for that
    # query have already been found and cached, so we can skip the querying step
    cache_key = query_cache_key(
        aoi_geom.union_all().wkt, collection, product, iso_windows
    )
    query_dir = query_cache_dir(cache_dir, cache_key)
    scenes_cache = query_dir / "scenes.parquet"

    # if cached products exist, don't bother querying
    if scenes_cache.exists():
        logger.info(f"Loading cached products from {scenes_cache}")
        scenes = pd.read_parquet(scenes_cache)

    # otherwise, proceed with querying
    else:
        from concurrent.futures import ThreadPoolExecutor, as_completed

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
            logger.info(
                f"Window {start_iso} -> {end_iso}, batch {batch_idx + 1}/"
                f"{len(aoi_batches)}: {len(df)} scene(s)"
            )
            return df

        # build all (window × batch) tasks
        tasks = [
            (start_iso, end_iso, i, batch_geom)
            for start_iso, end_iso in iso_windows
            for i, batch_geom in enumerate(aoi_batches)
        ]

        all_rows: list[pd.DataFrame] = []
        max_workers = min(len(tasks), 8)

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

            if not all_rows:
                logger.warning("No scenes found for given AOI and date windows.")
                return pd.DataFrame()

            scenes = pd.concat(all_rows, ignore_index=True).drop_duplicates(subset="Id")
            scenes = scenes[["Id", "Name", "S3Path", "ContentDate", "GeoFootprint"]]
            logger.info(
                f"Found {len(scenes)} unique scenes across {len(iso_windows)} "
                f"window(s) and {len(aoi_batches)} batch(es)"
            )

            try:
                scenes.to_parquet(scenes_cache)
                save_query_info(
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

            progress.advance(task_id)

    return scenes


########################################################################################
# 2) Download based on query results
########################################################################################


# --------------------------------------------------------------------------------------
# Handle when a resolution is not available for a requested band
# --------------------------------------------------------------------------------------
@dataclass
class ResolvedBand:
    """A band resolved to an existing resolution."""

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
    Resolve a single S2 band to an existing resolution.
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
    """Resolve a list of S2 bands to existing resolutions."""
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
# Determine the targets we want to download for a scene, and cache the results
# --------------------------------------------------------------------------------------


def _parse_s5cmd_ls_line(line: str) -> Optional[tuple[int, str]]:
    """Parse an s5cmd ls output line → (size, rel_path) or None."""
    parts = line.strip().split()
    if len(parts) >= 4:
        try:
            size = int(parts[2])
            rel_path = parts[-1]
            return size, rel_path
        except ValueError:
            pass
    return None


def _find_s2_scene_targets(
    scene_name: str,
    s3_path: str,
    resolved: list[ResolvedBand],
    config_file: str,
    logger: logging.Logger,
) -> list[dict]:
    """
    Find S2 scene targets by querying S3 directly for each resolved band.
    """
    s3_path = s3_path.removeprefix("/eodata")
    is_l1c = "MSIL1C" in scene_name.upper()
    targets = []

    for rb in resolved:
        if is_l1c:
            pattern = f"s3://eodata{s3_path}/GRANULE/*/IMG_DATA/*_{rb.band}.jp2"
        else:
            pattern = (
                f"s3://eodata{s3_path}/GRANULE/*/IMG_DATA/"
                f"R{rb.resolution}m/*_{rb.band}_{rb.resolution}m.jp2"
            )

        cmd = f'ls "{pattern}"'
        try:
            output = run_s5cmd_with_config(cmd, config_file=config_file)
        except Exception:
            logger.warning(
                f"  ERR {rb.band}@{rb.resolution}m: not found in {scene_name}"
            )
            continue

        found = False
        for line in output.strip().splitlines():
            parsed = _parse_s5cmd_ls_line(line)
            if parsed:
                expected_size, rel_path = parsed
                if not rel_path.startswith("GRANULE/"):
                    rel_path = f"GRANULE/{rel_path}"
                targets.append(
                    {
                        "Name": scene_name,
                        "S3Path": s3_path,
                        "band": rb.band,
                        "resolution": rb.resolution if not is_l1c else 0,
                        "rel_path": rel_path,
                        "expected_size": expected_size,
                    }
                )
                found = True
                break

        if not found:
            logger.warning(f"  ERR {rb.band}: not found in {scene_name}")

    return targets


def _find_s1_scene_targets(
    scene_name: str,
    s3_path: str,
    polarisations: list[str],
    config_file: str,
    logger: logging.Logger,
) -> list[dict]:
    """Resolve S1 polarisation targets by querying S3 directly for each pol."""
    s3_path = s3_path.removeprefix("/eodata")
    targets = []

    for pol in [p.upper() for p in polarisations]:
        pattern = f"s3://eodata{s3_path}/measurement/*-{pol.lower()}-*.tiff"
        cmd = f'ls "{pattern}"'
        try:
            output = run_s5cmd_with_config(cmd, config_file=config_file)
        except Exception:
            logger.warning(f"  ERR {pol}: not found in {scene_name}")
            continue

        found = False
        for line in output.strip().splitlines():
            parsed = _parse_s5cmd_ls_line(line)
            if parsed:
                expected_size, rel_path = parsed
                if not rel_path.startswith("measurement/"):
                    rel_path = f"measurement/{rel_path}"
                targets.append(
                    {
                        "Name": scene_name,
                        "S3Path": s3_path,
                        "band": pol,
                        "resolution": 0,
                        "rel_path": rel_path,
                        "expected_size": expected_size,
                    }
                )
                found = True
                break

        if not found:
            logger.warning(f"  ERR {pol}: not found in {scene_name}")

    return targets


# --------------------------------------------------------------------------------------
# Download one target file
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
# Download multiple target files for one scene
# --------------------------------------------------------------------------------------


@dataclass
class DownloadResult:
    """
    Result of downloading targets for a single scene, with lists of succeeded, failed,
    and skipped targets (identified by band@resolution).
    """

    scene_name: str
    succeeded: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.succeeded) + len(self.failed) + len(self.skipped)

    @property
    def ok(self) -> bool:
        return len(self.failed) == 0


def _download_scene_from_targets(
    scene_name: str,
    s3_path: str,
    targets: list[dict],
    output_dir: Path,
    config_file: str,
    parallel_bands: int,
    logger: logging.Logger,
) -> DownloadResult:
    """Download pre-resolved targets for a single scene, preserving SAFE structure."""
    result = DownloadResult(scene_name=scene_name)
    scene_root = output_dir / scene_name

    download_tasks: list[tuple[str, str, Path]] = []

    for t in targets:
        band = t["band"]
        res = t.get("resolution", 0)
        label = f"{band}@{res}m" if res else band
        rel_path = t["rel_path"]
        expected = t.get("expected_size", 0)
        local_path = scene_root / rel_path

        # check existing file with size verification
        if local_path.exists():
            local_size = local_path.stat().st_size
            if expected and local_size == expected:
                result.skipped.append(label)
                continue
            elif not expected and local_size > 0:
                result.skipped.append(label)
                continue
            # else: incomplete, re-download

        s3_uri = f"s3://eodata{s3_path}/{rel_path}"
        download_tasks.append((label, s3_uri, local_path))

    if not download_tasks:
        logger.info(
            f"All targets for {scene_name} already exist and are valid, skipping."
        )
        return result

    def _dload(task: tuple[str, str, Path]) -> tuple[str, bool]:
        label, uri, local = task
        logger.debug(f"  DWNLD {scene_name} / {label} starting...")
        ok = download_s3_file(uri, local, logger=logger, config_file=config_file)
        return label, ok

    with ThreadPoolExecutor(max_workers=parallel_bands) as pool:
        futures = {pool.submit(_dload, t): t for t in download_tasks}
        for future in as_completed(futures):
            label, ok = future.result()
            if ok:
                result.succeeded.append(label)
                logger.debug(f"  DWNLDED {scene_name} / {label}")
            else:
                result.failed.append(label)
                logger.debug(f"  ERRED {scene_name} / {label}")

    return result


# --------------------------------------------------------------------------------------
# Download multiple scenes given cached query results
# --------------------------------------------------------------------------------------


def _band_from_filename(filename: str, mission: str) -> str:
    """Extract band name from a JP2/TIFF filename."""
    # L2A: T33TUM_20240615T100559_B02_20m.jp2 -> B02
    # L1C: T33TUM_20240615T100559_B02.jp2 -> B02
    parts = filename.replace(".jp2", "").replace(".tiff", "").split("_")
    for p in reversed(parts):
        if p in S2_BAND_RESOLUTIONS or p in {"VV", "VH", "HH", "HV"}:
            return p
        # catch "B02" in "B02_20m" style
        if (
            p.startswith("B")
            or p == "SCL"
            or p == "TCI"
            or p == "AOT"
            or p == "WVP"
            or p == "B8A"
        ):
            return p
    return parts[-1]


def _res_from_filename(filename: str) -> int:
    """Extract resolution from a JP2 filename, or 0 if not present."""
    import re

    m = re.search(r"_(\d+)m\.jp2$", filename)
    return int(m.group(1)) if m else 0


def resolve_and_download(
    scenes_cache: Path,
    mission: str,
    bands: list[str],
    resolution: int,
    output_dir: Path,
    config_file: str = ".s5cfg",
    parallel_scenes: int = 2,
    parallel_bands: int = 2,
    logger: logging.Logger = None,
) -> list[DownloadResult]:
    """
    Resolve + download pipeline.

    For each scene:
      1. If already in targets cache, skip resolve, just download
      2. Otherwise: resolve via targeted S3 ls, download, append to cache

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

    # load scenes
    scenes = pd.read_parquet(scenes_cache)
    required_cols = {"Name", "S3Path"}
    if not required_cols.issubset(scenes.columns):
        raise ValueError(
            f"Scenes parquet missing required columns: "
            f"{required_cols - set(scenes.columns)}"
        )
    logger.info(f"Loaded {len(scenes)} scenes from {scenes_cache}")

    # targets cache lives alongside scenes.parquet
    targets_file = Path(scenes_cache).parent.parent / "downloaded_targets.parquet"
    logger.info(f"Looking for targets cache at: {targets_file}")  # ← add this
    if targets_file.exists():
        cached_targets = pd.read_parquet(targets_file)
        cached_keys = set(
            zip(
                cached_targets["Name"],
                cached_targets["band"],
                cached_targets["resolution"],
            )
        )
        cached_scene_names = set(cached_targets["Name"].unique())
        logger.info(
            f"Found {len(cached_scene_names)} scenes ({len(cached_targets)} rows) in {targets_file}"
        )
        logger.info(f"First 3 cached scenes: {list(cached_scene_names)[:3]}")
        logger.info(f"First 3 query scenes: {scenes['Name'].head(3).tolist()}")
    else:
        logger.info(f"No target cache at {targets_file}")  # ← add this
        cached_targets = pd.DataFrame()
        cached_keys = set()
        cached_scene_names = set()

    # resolve bands once (logs fallbacks once) — must be before _scene_fully_cached
    if mission.upper() == "S2":
        resolved_bands = _resolve_s2_bands(bands, resolution, logger)
    else:
        resolved_bands = None

    # split scenes; a scene is only "fully cached" if ALL requested bands are cached
    def _scene_fully_cached(name: str) -> bool:
        if name not in cached_scene_names:
            return False
        for rb in (
            resolved_bands
            if mission.upper() == "S2"
            else [type("", (), {"band": b, "resolution": 0})() for b in bands]
        ):
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
    new_target_rows: list[dict] = []
    lock = threading.Lock()
    s3_paths = dict(zip(scenes["Name"], scenes["S3Path"]))

    # Uncached scenes: resolve + download ----------------------------------------------
    def _resolve_and_dl(row: pd.Series) -> DownloadResult:
        name = row["Name"]
        raw_s3_path = row["S3Path"]
        s3_path = raw_s3_path.removeprefix("/eodata")
        scene_root = output_dir / name

        # quick local check before hitting S3
        if scene_root.exists():
            existing_jp2 = list(scene_root.rglob("*.jp2"))
            expected_bands = resolved_bands if mission.upper() == "S2" else bands
            if len(existing_jp2) >= len(expected_bands):
                # build targets from local files so the cache gets populated
                local_targets = []
                for f in existing_jp2:
                    rel = str(f.relative_to(scene_root))
                    local_targets.append(
                        {
                            "Name": name,
                            "S3Path": s3_path,
                            "band": _band_from_filename(f.name, mission),
                            "resolution": _res_from_filename(f.name),
                            "rel_path": rel,
                            "expected_size": f.stat().st_size,
                        }
                    )
                with lock:
                    new_target_rows.extend(local_targets)
                return DownloadResult(
                    scene_name=name,
                    skipped=[f.name for f in existing_jp2],
                )

        if mission.upper() == "S2":
            targets = _find_s2_scene_targets(
                name, raw_s3_path, resolved_bands, config_file, logger=logger
            )
            logger.info(
                f"Scene {name}: resolved targets: "
                f"{', '.join(f'{t["band"]}@{t["resolution"]}m' for t in targets)}"
            )
        elif mission.upper() == "S1":
            targets = _find_s1_scene_targets(
                name, raw_s3_path, bands, config_file, logger=logger
            )
            logger.info(
                f"Scene {name}: resolved targets: "
                f"{', '.join(f'{t["band"]}' for t in targets)}"
            )
        else:
            logger.error(f"Unsupported mission: {mission}")
            return DownloadResult(scene_name=name)

        if not targets:
            logger.error(f"No targets found for scene: {name}")
            return DownloadResult(scene_name=name)

        with lock:
            new_target_rows.extend(targets)

        logger.info(f"Scene {name}: downloading {len(targets)} target(s)")
        return _download_scene_from_targets(
            scene_name=name,
            s3_path=s3_path,
            targets=targets,
            output_dir=output_dir,
            config_file=config_file,
            parallel_bands=parallel_bands,
            logger=logger,
        )

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
                "Resolving + downloading", total=len(uncached_rows)
            )

            with ThreadPoolExecutor(max_workers=parallel_scenes) as pool:
                futures = {
                    pool.submit(_resolve_and_dl, r): r["Name"] for r in uncached_rows
                }
                for future in as_completed(futures):
                    name = futures[future]
                    try:
                        result = future.result()
                        all_results.append(result)
                    except Exception as exc:
                        logger.error(f"Scene {name} failed: {exc}")
                        all_results.append(
                            DownloadResult(scene_name=name, failed=["SCENE_ERROR"])
                        )

                    # Add to targets cache as they come in -----------------------------
                    with lock:
                        if len(new_target_rows) >= 1000:  # ~100 scenes × 10 bands
                            new_df = pd.DataFrame(new_target_rows)
                            if targets_file.exists():
                                existing = pd.read_parquet(targets_file)
                                combined = pd.concat(
                                    [existing, new_df], ignore_index=True
                                )
                            else:
                                combined = new_df
                            write_protected_parquet(combined, targets_file)
                            new_target_rows.clear()
                            logger.info(f"Flushed {len(new_df)} targets to cache")

                    progress.advance(task_id)

    # final flush for any remaining targets that didn't hit the 1000 threshold
    with lock:
        if new_target_rows:
            new_df = pd.DataFrame(new_target_rows)
            if targets_file.exists():
                existing = pd.read_parquet(targets_file)
                combined = pd.concat([existing, new_df], ignore_index=True)
            else:
                combined = new_df
            write_protected_parquet(combined, targets_file)
            logger.info(f"Flushed final {len(new_df)} targets to cache")
            new_target_rows.clear()

    # Cached scenes: download only -----------------------------------------------------
    if not cached_targets.empty and not cached_df.empty:
        cached_names = set(cached_df["Name"])
        grouped = [
            (name, group.to_dict("records"))
            for name, group in cached_targets.groupby("Name")
            if name in cached_names
        ]

        if grouped:

            def _dl_cached(name_targets: tuple[str, list[dict]]) -> DownloadResult:
                name, targets = name_targets
                s3_path = s3_paths.get(name, "").removeprefix("/eodata")
                return _download_scene_from_targets(
                    scene_name=name,
                    s3_path=s3_path,
                    targets=targets,
                    output_dir=output_dir,
                    config_file=config_file,
                    parallel_bands=parallel_bands,
                    logger=logger,
                )

            with Progress(
                SpinnerColumn(),
                TextColumn("[bold blue]{task.description}"),
                BarColumn(),
                MofNCompleteColumn(),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
            ) as progress:
                task_id = progress.add_task(
                    "Downloading cached scenes", total=len(grouped)
                )

                with ThreadPoolExecutor(max_workers=parallel_scenes) as pool:
                    futures = {pool.submit(_dl_cached, ng): ng[0] for ng in grouped}
                    for future in as_completed(futures):
                        name = futures[future]
                        try:
                            result = future.result()
                            all_results.append(result)
                        except Exception as exc:
                            logger.error(f"Cached scene {name} failed: {exc}")
                            all_results.append(
                                DownloadResult(scene_name=name, failed=["SCENE_ERROR"])
                            )
                        progress.advance(task_id)

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
