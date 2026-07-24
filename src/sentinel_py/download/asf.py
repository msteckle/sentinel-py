import hashlib
import logging
import netrc
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote, urlparse

import asf_search as asf
import pandas as pd
import requests
from asf_search.download import download_url as asf_download_url
from asf_search.exceptions import ASFAuthenticationError
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from sentinel_py.cache import merge_state_rows, write_parquet_atomic

# Sentinel-1-specific values documented by ASF rather than the full generic
# asf_search constant catalog, which also includes values for other missions:
# https://docs.asf.alaska.edu/api/keywords/
SENTINEL1_DATA_PRODUCT_LEVELS = (
    "GRD_HD",
    "GRD_HS",
    "GRD_MD",
    "GRD_MS",
    "GRD_FD",
    "SLC",
    "RAW",
    "OCN",
)
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
    (*SENTINEL1_DATA_PRODUCT_LEVELS, *SENTINEL1_METADATA_LEVELS)
)

# Sentinel-1 acquisition modes are IW, EW, SM, and WV. ASF exposes the six
# Stripmap beams individually as S1-S6:
# https://sentiwiki.copernicus.eu/web/s1-mission
SENTINEL1_BEAM_MODES = frozenset(("IW", "EW", "WV", "S1", "S2", "S3", "S4", "S5", "S6"))

# Standard Sentinel-1 product configurations plus ASF's documented additional
# catalog labels:
# https://docs.asf.alaska.edu/api/keywords/
SENTINEL1_POLARIZATIONS = frozenset(
    ("VV+VH", "HH+HV", "VV", "HH", "DUAL VV", "DUAL VH", "DUAL HH", "DUAL HV")
)

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


@dataclass(frozen=True)
class ASFDownloadSummary:
    """Counts and user-facing messages from an ASF download invocation."""

    downloaded: int
    skipped: int
    failed: int
    messages: list[str]

    def __iter__(self):
        """Keep callers that iterate over result messages working."""
        return iter(self.messages)

    def __getitem__(self, index):
        """Keep callers that index result messages working."""
        return self.messages[index]


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
        "Querying ASF: start=%s end=%s product_levels=%s beam_mode=%s "
        "flight_direction=%s polarization=%s relative_orbit=%s max_results=%s",
        date_start,
        date_end,
        normalized_levels,
        normalized_beam_mode,
        normalized_direction,
        normalized_polarization,
        relative_orbit,
        max_results,
    )
    logger.debug("ASF query AOI WKT: %s", aoi_wkt)

    # Search ASF for products matching the criteria using the asf_search library
    try:
        results = asf.search(
            platform=asf.PLATFORM.SENTINEL1,
            processingLevel=lvls,
            start=pd.to_datetime(date_start).date(),
            end=pd.to_datetime(date_end).date(),
            beamMode=getattr(asf.BEAMMODE, normalized_beam_mode),
            intersectsWith=aoi_wkt,
            flightDirection=normalized_direction,
            polarization=normalized_polarization,
            relativeOrbit=relative_orbit,
            maxResults=max_results,
        )
    except Exception:
        logger.exception("ASF query failed for %s to %s", date_start, date_end)
        raise

    # Convert the results to a GeoJSON and then to a pandas DataFrame
    geoj = results.geojson()
    rows = []
    for feat in geoj["features"]:
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
    df = (
        df.dropna(subset=["url"])
        .drop_duplicates(subset=["url"])
        .sort_values("granule")
        .reset_index(drop=True)
    )
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
    credentials = earthdata_netrc_credentials(config_file)
    if credentials is None:
        raise ValueError(
            f"No Earthdata credentials for urs.earthdata.nasa.gov in {config_file}"
        )
    return asf.ASFSession().auth_with_creds(*credentials)


def _optional_int(value) -> int | None:
    if value is None or pd.isna(value):
        return None
    return int(value)


def _expected_product_size(product: dict) -> int | None:
    expected = _optional_int(product.get("expected_size"))
    if expected is not None:
        return expected
    size_mb = product.get("sizeMB")
    if size_mb is None or pd.isna(size_mb):
        return None
    return round(float(size_mb) * 1024 * 1024)


def _expected_product_md5(product: dict) -> str | None:
    checksum = product.get("md5sum")
    if checksum is None or pd.isna(checksum):
        return None
    normalized = str(checksum).strip().lower()
    return normalized or None


def _file_md5(path: Path) -> str:
    digest = hashlib.md5()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_product_file(
    path: Path,
    *,
    filename: str,
    expected_size: int | None,
    expected_md5: str | None,
) -> int:
    actual_size = path.stat().st_size
    if expected_size is not None and actual_size != expected_size:
        raise IOError(
            f"size mismatch for {filename}: expected {expected_size}, got {actual_size}"
        )
    if expected_md5 is not None:
        actual_md5 = _file_md5(path)
        if actual_md5 != expected_md5:
            raise IOError(
                f"MD5 mismatch for {filename}: expected {expected_md5}, "
                f"got {actual_md5}"
            )
    return actual_size


def _product_filename(product: dict) -> str:
    url = str(product["url"])
    filename = Path(unquote(urlparse(url).path)).name
    if not filename:
        granule = product.get("granule")
        if granule is None or pd.isna(granule):
            raise ValueError(f"Could not determine filename for ASF URL: {url}")
        filename = Path(str(granule)).name
    if filename in {"", ".", ".."}:
        raise ValueError(f"Unsafe filename for ASF URL: {url}")
    return filename


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
) -> ASFDownloadSummary:
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
        return ASFDownloadSummary(
            downloaded=0,
            skipped=0,
            failed=0,
            messages=["No ASF URLs to download."],
        )

    state_file = state_file or out_dir / ".sentinel-py" / "asf_downloads.parquet"
    if state_file.exists():
        state = pd.read_parquet(state_file)
        logger.info("Loaded ASF download state from %s", state_file)
    else:
        state = pd.DataFrame(columns=ASF_DOWNLOAD_STATE_COLUMNS)
        logger.info("No existing ASF download state at %s", state_file)
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
        url = str(product["url"])
        filename = _product_filename(product)
        target = out_dir / filename
        partial = out_dir / f".{filename}.part"
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

            for attempt in range(retries + 1):
                partial.unlink(missing_ok=True)
                try:
                    asf_download_url(
                        url=url,
                        path=str(out_dir),
                        filename=partial.name,
                        session=_session(),
                    )
                    actual = _validate_product_file(
                        partial,
                        filename=filename,
                        expected_size=expected,
                        expected_md5=expected_md5,
                    )
                    break
                except Exception as error:
                    partial.unlink(missing_ok=True)
                    if attempt >= retries or not _is_retryable_download_error(error):
                        raise
                    delay = retry_backoff * (2**attempt)
                    logger.warning(
                        "Retrying ASF product %s after attempt %d/%d failed: %s; "
                        "waiting %.1f seconds",
                        filename,
                        attempt + 1,
                        retries + 1,
                        error,
                        delay,
                    )
                    time.sleep(delay)
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

    completed = 0
    skipped = 0
    failed = 0
    cache_updated = 0
    product_records = products.to_dict("records")
    partial_paths = [
        out_dir / f".{_product_filename(product)}.part" for product in product_records
    ]
    pending_state_rows: list[dict] = []

    def _flush_state() -> None:
        nonlocal state
        if not pending_state_rows:
            return
        state = merge_state_rows(state, pending_state_rows, key_columns=["url"])
        write_parquet_atomic(state, state_file, index=False)
        pending_state_rows.clear()

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            TextColumn("[cyan]{task.fields[status]}"),
        ) as progress:
            task_id = progress.add_task(
                "Checking & downloading ASF products",
                total=len(products),
                status=("downloaded 0 · skipped 0 · failed 0 · cache updated 0"),
            )

            with ThreadPoolExecutor(max_workers=processes) as pool:
                futures = {
                    pool.submit(_download_one, product): str(product["url"])
                    for product in product_records
                }
                for future in as_completed(futures):
                    row = future.result()
                    cache_updated += 1
                    if row["last_action"] == "downloaded":
                        completed += 1
                    elif row["last_action"] == "skipped":
                        skipped += 1
                    else:
                        failed += 1
                    pending_state_rows.append(row)
                    if len(pending_state_rows) >= 100:
                        _flush_state()
                    progress.update(
                        task_id,
                        advance=1,
                        status=(
                            f"downloaded {completed} · skipped {skipped} · "
                            f"failed {failed} · cache updated {cache_updated}"
                        ),
                    )
    finally:
        try:
            _flush_state()
        finally:
            # Partial archives are never resumable: the next attempt starts from zero.
            # Remove only the temporary files belonging to this invocation, including
            # when KeyboardInterrupt unwinds the concurrent download loop.
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

    return ASFDownloadSummary(
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
