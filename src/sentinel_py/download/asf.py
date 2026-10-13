"""Sentinel-1 query and download from ASF with persistent, size-verified state."""

import hashlib
import logging
import netrc
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import asf_search as asf
import pandas as pd
import requests
from asf_search.download import download_url as asf_download_url
from asf_search.exceptions import ASFAuthenticationError
from shapely import wkt
from shapely.geometry.polygon import orient

from sentinel_py.cache import REGISTRY, RowStore
from sentinel_py.enums import ASFBeamMode, ASFPolarization, ASFProductLevel
from sentinel_py.download.base import (
    DownloadSummary,
    part_path,
    retry_call,
    run_download_pool,
    url_filename,
)

# Sentinel-1-specific values documented by ASF rather than the full generic
# asf_search constant catalog, which also includes values for other missions:
# https://docs.asf.alaska.edu/api/keywords/. The data-product, beam-mode, and
# polarization vocabularies live in ``sentinel_py.enums``; ASF additionally exposes
# metadata-only product levels.
SENTINEL1_METADATA_LEVELS = (
    "METADATA_GRD_HD",
    "METADATA_GRD_HS",
    "METADATA_GRD_MD",
    "METADATA_GRD_MS",
    "METADATA_SLC",
    "METADATA_RAW",
    "METADATA_OCN",
)
SENTINEL1_PRODUCT_LEVELS = frozenset(
    (*(level.value for level in ASFProductLevel), *SENTINEL1_METADATA_LEVELS)
)
SENTINEL1_BEAM_MODES = frozenset(mode.value for mode in ASFBeamMode)
SENTINEL1_POLARIZATIONS = frozenset(pol.value for pol in ASFPolarization)

ASF_MANIFEST_COLUMNS = [
    "granule",
    "url",
    "beamMode",
    "flightDirection",
    "polarization",
    "relativeOrbit",
    "startTime",
    "stopTime",
    "expected_size",
    "sizeMB",
    "md5sum",
]
ASF_DOWNLOAD_STATE_COLUMNS = [
    "url",
    "filename",
    "expected_size",
    "local_actual_size",
    "local_mtime_ns",
    "fingerprint_version",
    "status",
    "last_action",
    "checked_at",
    "md5sum",
    "error",
]
ASF_FINGERPRINT_VERSION = 1
# CMR rejects queries with more than 4,100 total conditions, but spatial OR
# performance degrades well before that limit. A 100-point global query stays
# comfortably within both the condition limit and the normal request timeout.
CMR_POINT_BATCH_SIZE = 100
CMR_POINT_QUERY_WORKERS = 8


def _is_retryable_download_error(error: Exception) -> bool:
    """Return whether another full-product download attempt may succeed."""
    if isinstance(error, ASFAuthenticationError):
        return False
    if isinstance(error, requests.exceptions.HTTPError):
        response = error.response
        return (
            response is None
            or response.status_code == 429
            or response.status_code >= 500
        )
    if isinstance(
        error,
        (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        ),
    ):
        return True
    # A short/corrupt response is detected by the post-download size check.
    return isinstance(error, IOError) and str(error).startswith(
        ("size mismatch", "MD5 mismatch")
    )


def get_predominant_flightdir(manifest: pd.DataFrame) -> str | None:
    """Return the most common direction; select ASCENDING if counts are tied."""
    if "flightDirection" not in manifest.columns:
        return None
    directions = manifest["flightDirection"].dropna().astype(str).str.upper()
    if directions.empty:
        return None
    counts = directions.value_counts()
    winners = sorted(counts[counts == counts.max()].index.tolist())
    return winners[0]


def query_asf(
    aoi_wkt: str,
    date_start: str,
    date_end: str,
    product_levels: list[str],
    season: list[int] | None = None,
    beam_mode: str = "IW",
    flight_direction: str | None = None,
    polarization: str | None = None,
    relative_orbit: int | None = None,
    max_results: int | None = None,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    """Return manifest pandas dataframe with unique URLs."""
    logger = logger or logging.getLogger(__name__)

    # Validate date range
    if pd.to_datetime(date_end) < pd.to_datetime(date_start):
        raise ValueError("date_end must be on or after date_start")

    # Set a longer timeout for CMR queries to avoid timeouts on large queries
    asf.constants.INTERNAL.CMR_TIMEOUT = 60
    normalized_levels = [product.upper() for product in product_levels]
    invalid = [
        product
        for product, normalized in zip(product_levels, normalized_levels)
        if normalized not in SENTINEL1_PRODUCT_LEVELS
    ]
    if invalid:
        raise ValueError(
            "Unsupported Sentinel-1 ASF product level(s): "
            + ", ".join(invalid)
            + ". Supported values: "
            + ", ".join(sorted(SENTINEL1_PRODUCT_LEVELS))
        )
    lvls = [getattr(asf.PRODUCT_TYPE, product) for product in normalized_levels]

    # Validate beam mode
    normalized_beam_mode = beam_mode.upper()
    if normalized_beam_mode not in SENTINEL1_BEAM_MODES:
        raise ValueError(
            f"Unsupported Sentinel-1 ASF beam mode: {beam_mode}. Supported values: "
            + ", ".join(sorted(SENTINEL1_BEAM_MODES))
        )

    normalized_polarization = polarization.upper() if polarization else None
    if (
        normalized_polarization is not None
        and normalized_polarization not in SENTINEL1_POLARIZATIONS
    ):
        raise ValueError(
            f"Unsupported Sentinel-1 ASF polarization: {polarization}. "
            "Supported values: " + ", ".join(sorted(SENTINEL1_POLARIZATIONS))
        )

    normalized_direction = flight_direction.upper() if flight_direction else None
    logger.info(
        "Querying ASF: start=%s end=%s season=%s product_levels=%s beam_mode=%s "
        "flight_direction=%s polarization=%s relative_orbit=%s max_results=%s",
        date_start,
        date_end,
        season,
        normalized_levels,
        normalized_beam_mode,
        normalized_direction,
        normalized_polarization,
        relative_orbit,
        max_results,
    )
    logger.debug("ASF query AOI WKT: %s", aoi_wkt)

    spatial_option_batches: list[dict[str, object]]
    aoi_geometry = wkt.loads(aoi_wkt)
    if aoi_geometry.geom_type == "MultiPoint":
        points = list(aoi_geometry.geoms)
        if not points:
            raise ValueError("MultiPoint AOI must contain at least one point")
        # CMR supports repeated point parameters with OR semantics. Using
        # asf_search's raw CMR keyword escape hatch avoids its intersectsWith
        # validator, which otherwise convex-hulls disconnected geometries.
        spatial_option_batches = []
        for offset in range(0, len(points), CMR_POINT_BATCH_SIZE):
            point_batch = points[offset : offset + CMR_POINT_BATCH_SIZE]
            cmr_keywords = [("point", f"{point.x},{point.y}") for point in point_batch]
            cmr_keywords.append(("options[point][or]", "true"))
            spatial_option_batches.append({"cmr_keywords": cmr_keywords})
        logger.info(
            "Querying ASF MultiPoint AOI as %d OR-ed CMR point filters in %d batch(es)",
            len(points),
            len(spatial_option_batches),
        )
    elif aoi_geometry.geom_type == "MultiPolygon":
        # ASF's geometry repair can reverse rings while attempting to merge
        # disconnected polygons, yielding an invalid clockwise CMR polygon.
        # Query each independently oriented component instead; this also avoids
        # an accidental world-spanning envelope for distant AOI features.
        spatial_option_batches = [
            {"intersectsWith": orient(polygon, sign=1.0).wkt}
            for polygon in aoi_geometry.geoms
            if not polygon.is_empty
        ]
        if not spatial_option_batches:
            raise ValueError("MultiPolygon AOI contains no usable polygons")
        logger.info(
            "Querying ASF MultiPolygon AOI as %d independent polygon filters",
            len(spatial_option_batches),
        )
    else:
        spatial_option_batches = [
            {"intersectsWith": orient(aoi_geometry, sign=1.0).wkt}
        ]

    # Search ASF for products matching the criteria using the asf_search library
    try:

        def _search(spatial_options: dict[str, object]):
            """Run one ASF search for a spatial-option batch."""
            return asf.search(
                platform=asf.PLATFORM.SENTINEL1,
                processingLevel=lvls,
                start=pd.to_datetime(date_start).date(),
                end=pd.to_datetime(date_end).date(),
                season=season,
                beamMode=getattr(asf.BEAMMODE, normalized_beam_mode),
                flightDirection=normalized_direction,
                polarization=normalized_polarization,
                relativeOrbit=relative_orbit,
                maxResults=max_results,
                **spatial_options,
            )

        if len(spatial_option_batches) == 1:
            result_batches = [_search(spatial_option_batches[0])]
        else:
            max_workers = min(CMR_POINT_QUERY_WORKERS, len(spatial_option_batches))
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                result_batches = list(pool.map(_search, spatial_option_batches))
    except Exception:
        logger.exception("ASF query failed for %s to %s", date_start, date_end)
        raise

    # Convert the results to a GeoJSON and then to a pandas DataFrame
    rows = []
    features = [
        feature
        for results in result_batches
        for feature in results.geojson()["features"]
    ]
    for feat in features:
        prop = feat["properties"]
        expected_size = prop.get("bytes")
        expected_size = int(expected_size) if expected_size is not None else None
        size_mb = (
            expected_size / (1024 * 1024)
            if expected_size is not None
            else prop.get("sizeMB")
        )
        rows.append(
            {
                "granule": prop.get("fileName"),
                "url": prop.get("url"),
                "beamMode": prop.get("beamMode"),
                "flightDirection": prop.get("flightDirection"),
                "polarization": prop.get("polarization"),
                "relativeOrbit": prop.get("pathNumber") or prop.get("relativeOrbit"),
                "startTime": prop.get("startTime"),
                "stopTime": prop.get("stopTime"),
                "expected_size": expected_size,
                "sizeMB": size_mb,
                "md5sum": prop.get("md5sum"),
            }
        )
    df = pd.DataFrame(rows, columns=ASF_MANIFEST_COLUMNS)

    # Remove duplicates, sort by granule name, and reset the index
    if df.empty:
        logger.info("ASF query returned 0 products for %s to %s", date_start, date_end)
        return df
    df = df.dropna(subset=["url"]).drop_duplicates(subset=["url"])
    if max_results is not None and len(df) > max_results:
        df = df.sort_values(
            ["stopTime", "granule"], ascending=[False, True], na_position="last"
        ).head(max_results)
    df = df.sort_values("granule").reset_index(drop=True)
    logger.info(
        "ASF query returned %d unique products for %s to %s",
        len(df),
        date_start,
        date_end,
    )
    return df


def earthdata_netrc_credentials(config_file: Path) -> tuple[str, str] | None:
    """Read Earthdata credentials from a specified netrc-format file."""
    try:
        auth = netrc.netrc(str(config_file)).authenticators("urs.earthdata.nasa.gov")
    except (FileNotFoundError, netrc.NetrcParseError, OSError):
        return None
    if auth is None:
        return None
    username, _, password = auth
    if not username or not password:
        return None
    return username, password


def _build_asf_session(
    config_file: Path,
):
    """Build an authenticated ASF session from an Earthdata config file."""
    credentials = earthdata_netrc_credentials(config_file)
    if credentials is None:
        raise ValueError(
            f"No Earthdata credentials for urs.earthdata.nasa.gov in {config_file}"
        )
    return asf.ASFSession().auth_with_creds(*credentials)


def _optional_int(value) -> int | None:
    """Return ``int(value)``, or ``None`` for missing or NaN values."""
    if value is None or pd.isna(value):
        return None
    return int(value)


def _expected_product_size(product: dict) -> int | None:
    """Return the manifest's expected byte size (``expected_size`` or ``sizeMB``)."""
    expected = _optional_int(product.get("expected_size"))
    if expected is not None:
        return expected
    size_mb = product.get("sizeMB")
    if size_mb is None or pd.isna(size_mb):
        return None
    return round(float(size_mb) * 1024 * 1024)


def _expected_product_md5(product: dict) -> str | None:
    """Return the normalized manifest MD5, or ``None`` when absent."""
    checksum = product.get("md5sum")
    if checksum is None or pd.isna(checksum):
        return None
    normalized = str(checksum).strip().lower()
    return normalized or None


def _file_md5(path: Path) -> str:
    """Return the hex MD5 digest of a file, streaming in bounded memory."""
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "md5").hexdigest()


def _validate_product_file(
    path: Path,
    *,
    filename: str,
    expected_size: int | None,
    expected_md5: str | None,
) -> int:
    """Check a downloaded file's size and MD5; return its size or raise ``OSError``."""
    actual_size = path.stat().st_size
    if expected_size is not None and actual_size != expected_size:
        raise OSError(
            f"size mismatch for {filename}: expected {expected_size}, got {actual_size}"
        )
    if expected_md5 is not None:
        actual_md5 = _file_md5(path)
        if actual_md5 != expected_md5:
            raise OSError(
                f"MD5 mismatch for {filename}: expected {expected_md5}, "
                f"got {actual_md5}"
            )
    return actual_size


def product_filename(product: dict) -> str:
    """Return the safe local file name for a manifest row.

    The URL's final path component is used, falling back to the ``granule`` column
    when the URL has none.

    Raises
    ------
    ValueError
        If no safe file name can be determined.
    """
    granule = product.get("granule")
    return url_filename(
        str(product["url"]),
        None if granule is None or pd.isna(granule) else str(granule),
    )


def _state_row(
    product: dict,
    *,
    filename: str,
    expected_size: int | None,
    actual_size: int | None,
    local_mtime_ns: int | None,
    status: str,
    last_action: str,
    error: str | None = None,
) -> dict:
    """Build one download-state row for a manifest product."""
    checksum = product.get("md5sum")
    if checksum is not None and pd.isna(checksum):
        checksum = None
    return {
        "url": str(product["url"]),
        "filename": filename,
        "expected_size": expected_size,
        "local_actual_size": actual_size,
        "local_mtime_ns": local_mtime_ns,
        "fingerprint_version": ASF_FINGERPRINT_VERSION,
        "status": status,
        "last_action": last_action,
        "checked_at": datetime.now().isoformat(),
        "md5sum": checksum,
        "error": error,
    }


def _verified_fingerprint_matches(
    cached: dict,
    *,
    filename: str,
    actual_size: int,
    local_mtime_ns: int,
    expected_md5: str,
) -> bool:
    """Return whether an unchanged local file was previously MD5-verified."""
    cached_md5 = cached.get("md5sum")
    if cached_md5 is None or pd.isna(cached_md5):
        return False
    cached_mtime_ns = _optional_int(cached.get("local_mtime_ns"))
    fingerprint_version = _optional_int(cached.get("fingerprint_version"))
    if fingerprint_version == ASF_FINGERPRINT_VERSION:
        mtime_matches = cached_mtime_ns == local_mtime_ns
    else:
        # Version 0 state briefly stored nanosecond mtimes as float64. At current
        # epoch values, IEEE-754 rounding can shift the timestamp by up to 128 ns.
        # Accept that narrow migration tolerance once, then rewrite an exact v1 row.
        mtime_matches = (
            cached_mtime_ns is not None and abs(cached_mtime_ns - local_mtime_ns) <= 256
        )
    return (
        cached.get("status") == "complete"
        and str(cached.get("filename")) == filename
        and _optional_int(cached.get("local_actual_size")) == actual_size
        and mtime_matches
        and str(cached_md5).strip().lower() == expected_md5
    )


def download_asf(
    products: pd.DataFrame | list[str],
    out_dir: Path,
    config_file: Path,
    processes: int = 4,
    retries: int = 3,
    retry_backoff: float = 2.0,
    state_file: Path | None = None,
    logger: logging.Logger | None = None,
) -> DownloadSummary:
    """
    Download an ASF manifest with persistent, size-verified state.

    Files are written to ``.part`` siblings and atomically renamed only after their
    expected byte size matches. Existing files are checked against the manifest or a
    prior completed state row before being skipped. Transient failures are retried
    with exponential backoff; ``retries`` counts attempts after the initial one.
    """
    if retries < 0:
        raise ValueError("retries must be zero or greater")
    if retry_backoff < 0:
        raise ValueError("retry_backoff must be zero or greater")
    logger = logger or logging.getLogger(__name__)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    if isinstance(products, list):
        products = pd.DataFrame({"url": products})
    if "url" not in products.columns:
        raise ValueError("ASF manifest is missing the required 'url' column")
    products = (
        products.dropna(subset=["url"])
        .drop_duplicates(subset=["url"])
        .reset_index(drop=True)
    )
    if products.empty:
        logger.info("No ASF URLs to download")
        return DownloadSummary(0, 0, 0, ["No ASF URLs to download."])

    state_file = state_file or REGISTRY["asf_downloads"].path(out_dir)
    state_store = RowStore(state_file, ["url"])
    state = state_store.read()
    if state.empty:
        state = pd.DataFrame(columns=ASF_DOWNLOAD_STATE_COLUMNS)
        logger.info("No existing ASF download state at %s", state_file)
    else:
        logger.info("Loaded ASF download state from %s", state_file)
    for column in ("local_mtime_ns", "fingerprint_version"):
        if column not in state.columns:
            state[column] = pd.Series(pd.NA, index=state.index, dtype="Int64")
        else:
            state[column] = pd.array(
                [pd.NA if pd.isna(value) else int(value) for value in state[column]],
                dtype="Int64",
            )
    state_by_url = (
        state.drop_duplicates(subset=["url"], keep="last")
        .set_index("url")
        .to_dict("index")
        if not state.empty and "url" in state.columns
        else {}
    )
    session_lock = threading.Lock()
    session_holder = []

    def _session():
        """Lazily create one ASF session shared by all worker threads."""
        with session_lock:
            if not session_holder:
                session_holder.append(_build_asf_session(config_file))
            return session_holder[0]

    start = time.time()
    start_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start))
    logger.info(
        "Starting ASF download: products=%d output=%s processes=%d retries=%d",
        len(products),
        out_dir,
        processes,
        retries,
    )

    def _download_one(product: dict) -> dict:
        """Verify or download one product and return its state row."""
        url = str(product["url"])
        filename = product_filename(product)
        target = out_dir / filename
        partial = part_path(out_dir, filename)
        expected = _expected_product_size(product)
        expected_md5 = _expected_product_md5(product)
        cached = state_by_url.get(url, {})
        logger.debug("Checking ASF product %s", filename)

        try:
            if target.is_file():
                target_stat = target.stat()
                actual = target_stat.st_size
                local_mtime_ns = target_stat.st_mtime_ns
                expected_matches = expected is not None and actual == expected
                fingerprint_matches = (
                    expected_md5 is not None
                    and _verified_fingerprint_matches(
                        cached,
                        filename=filename,
                        actual_size=actual,
                        local_mtime_ns=local_mtime_ns,
                        expected_md5=expected_md5,
                    )
                )
                checksum_matches = expected_md5 is None or fingerprint_matches
                if (
                    expected_md5 is not None
                    and not fingerprint_matches
                    and (expected is None or actual == expected)
                ):
                    checksum_matches = _file_md5(target) == expected_md5
                cached_matches = (
                    expected is None
                    and expected_md5 is None
                    and cached.get("status") == "complete"
                    and _optional_int(cached.get("local_actual_size")) == actual
                )
                checksum_only_matches = expected is None and expected_md5 is not None
                if (
                    (expected_matches and checksum_matches)
                    or (checksum_only_matches and checksum_matches)
                    or cached_matches
                ):
                    logger.debug("SKIPPED %s: existing file verified", filename)
                    return _state_row(
                        product,
                        filename=filename,
                        expected_size=expected,
                        actual_size=actual,
                        local_mtime_ns=local_mtime_ns,
                        status="complete",
                        last_action="skipped",
                    )


            def fetch() -> int:
                """Download to the ``.part`` file and verify size and checksum."""
                partial.unlink(missing_ok=True)
                asf_download_url(
                    url=url,
                    path=str(out_dir),
                    filename=partial.name,
                    session=_session(),
                )
                return _validate_product_file(
                    partial,
                    filename=filename,
                    expected_size=expected,
                    expected_md5=expected_md5,
                )

            def log_retry(error: Exception, attempt: int, delay: float) -> None:
                """Log a retryable ASF download failure."""
                logger.warning(
                    "Retrying ASF product %s after attempt %d/%d failed: %s; "
                    "waiting %.1f seconds",
                    filename,
                    attempt + 1,
                    retries + 1,
                    error,
                    delay,
                )

            actual = retry_call(
                fetch,
                retries=retries,
                backoff=retry_backoff,
                is_retryable=_is_retryable_download_error,
                on_retry=log_retry,
                cleanup=lambda: partial.unlink(missing_ok=True),
            )
            os.replace(partial, target)
            local_mtime_ns = target.stat().st_mtime_ns
            logger.debug("DOWNLOADED %s", filename)
            return _state_row(
                product,
                filename=filename,
                expected_size=expected,
                actual_size=actual,
                local_mtime_ns=local_mtime_ns,
                status="complete",
                last_action="downloaded",
            )
        except Exception as error:
            actual = partial.stat().st_size if partial.exists() else None
            logger.error("FAILED %s: %s", filename, error)
            return _state_row(
                product,
                filename=filename,
                expected_size=expected,
                actual_size=actual,
                local_mtime_ns=None,
                status="failed",
                last_action="failed",
                error=str(error),
            )

    product_records = products.to_dict("records")
    partial_paths = [
        part_path(out_dir, product_filename(product)) for product in product_records
    ]

    try:
        # Rows are checkpointed in batches as downloads finish; the writer compacts
        # the parts into the state file after a clean run.
        with state_store.writer(batch_rows=100, flush_seconds=15.0) as state_writer:
            completed, skipped, failed = run_download_pool(
                product_records,
                _download_one,
                processes=processes,
                description="Checking & downloading ASF products",
                sink=state_writer,
                show_cache_updated=True,
            )
    finally:
        # Partial archives are never resumable: the next attempt starts from zero.
        # Remove only this invocation's temporary files, including when
        # KeyboardInterrupt unwinds the concurrent download loop.
        for partial_path in partial_paths:
            try:
                partial_path.unlink(missing_ok=True)
            except OSError:
                pass

    end = time.time()
    end_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))
    elapsed = end - start
    logger.info("━" * 60)
    logger.info("ASF DOWNLOAD SUMMARY")
    logger.info("━" * 60)
    logger.info(
        "%d downloaded, %d skipped, %d failed in %.1f seconds; state=%s",
        completed,
        skipped,
        failed,
        elapsed,
        state_file,
    )

    return DownloadSummary(
        downloaded=completed,
        skipped=skipped,
        failed=failed,
        messages=[
            f"Download started: {start_str}",
            f"Download ended:   {end_str}",
            f"Elapsed time:     {elapsed:.1f} seconds for {len(products)} files",
            (
                f"Results:          {completed} downloaded, {skipped} skipped, "
                f"{failed} failed"
            ),
            f"Download status:  {state_file}",
        ],
    )
