import netrc
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    "status",
    "last_action",
    "checked_at",
    "md5sum",
    "error",
]


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
    return isinstance(error, IOError) and str(error).startswith("size mismatch")


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
) -> pd.DataFrame:
    """Return manifest pandas dataframe with unique URLs."""

    # Validate date range
    if pd.to_datetime(date_end) < pd.to_datetime(date_start):
        raise ValueError("date_end must be on or after date_start")

    # Set a longer timeout for CMR queries to avoid timeouts on large queries
    asf.constants.INTERNAL.CMR_TIMEOUT = 60
    try:
        lvls = [getattr(asf.PRODUCT_TYPE, p.upper()) for p in product_levels]
    except AttributeError as error:
        invalid = [
            product
            for product in product_levels
            if not hasattr(asf.PRODUCT_TYPE, product.upper())
        ]
        raise ValueError(
            "Unsupported ASF product level(s): " + ", ".join(invalid)
        ) from error

    # Validate beam mode
    normalized_beam_mode = beam_mode.upper()
    if not hasattr(asf.BEAMMODE, normalized_beam_mode):
        raise ValueError(f"Unsupported ASF beam mode: {beam_mode}")

    # Search ASF for products matching the criteria using the asf_search library
    results = asf.search(
        platform=asf.PLATFORM.SENTINEL1,
        processingLevel=lvls,
        start=pd.to_datetime(date_start).date(),
        end=pd.to_datetime(date_end).date(),
        beamMode=getattr(asf.BEAMMODE, normalized_beam_mode),
        intersectsWith=aoi_wkt,
        flightDirection=flight_direction.upper() if flight_direction else None,
        polarization=polarization,
        relativeOrbit=relative_orbit,
        maxResults=max_results,
    )

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
        return df
    df = (
        df.dropna(subset=["url"])
        .drop_duplicates(subset=["url"])
        .sort_values("granule")
        .reset_index(drop=True)
    )
    return df


def earthdata_netrc_credentials(config_file: Path) -> tuple[str, str] | None:
    """Read Earthdata credentials from a specified netrc-format file."""
    try:
        auth = netrc.netrc(str(config_file)).authenticators(
            "urs.earthdata.nasa.gov"
        )
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
    return int(round(float(value)))


def _expected_product_size(product: dict) -> int | None:
    expected = _optional_int(product.get("expected_size"))
    if expected is not None:
        return expected
    size_mb = product.get("sizeMB")
    if size_mb is None or pd.isna(size_mb):
        return None
    return round(float(size_mb) * 1024 * 1024)


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
        "status": status,
        "last_action": last_action,
        "checked_at": datetime.now().isoformat(),
        "md5sum": checksum,
        "error": error,
    }


def download_asf(
    products: pd.DataFrame | list[str],
    out_dir: Path,
    config_file: Path,
    processes: int = 4,
    retries: int = 3,
    retry_backoff: float = 2.0,
    state_file: Path | None = None,
) -> list[str]:
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
        return ["No ASF URLs to download."]

    state_file = state_file or out_dir / ".sentinel-py" / "asf_downloads.parquet"
    if state_file.exists():
        state = pd.read_parquet(state_file)
    else:
        state = pd.DataFrame(columns=ASF_DOWNLOAD_STATE_COLUMNS)
    state_by_url = (
        state.drop_duplicates(subset=["url"], keep="last").set_index("url").to_dict(
            "index"
        )
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

    def _download_one(product: dict) -> dict:
        url = str(product["url"])
        filename = _product_filename(product)
        target = out_dir / filename
        partial = out_dir / f".{filename}.part"
        expected = _expected_product_size(product)
        cached = state_by_url.get(url, {})

        try:
            if target.is_file():
                actual = target.stat().st_size
                expected_matches = expected is not None and actual == expected
                cached_matches = (
                    expected is None
                    and cached.get("status") == "complete"
                    and _optional_int(cached.get("local_actual_size")) == actual
                )
                if expected_matches or cached_matches:
                    return _state_row(
                        product,
                        filename=filename,
                        expected_size=expected,
                        actual_size=actual,
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
                    actual = partial.stat().st_size
                    if expected is not None and actual != expected:
                        raise IOError(
                            f"size mismatch for {filename}: expected {expected}, "
                            f"got {actual}"
                        )
                    break
                except Exception as error:
                    partial.unlink(missing_ok=True)
                    if (
                        attempt >= retries
                        or not _is_retryable_download_error(error)
                    ):
                        raise
                    time.sleep(retry_backoff * (2**attempt))
            os.replace(partial, target)
            return _state_row(
                product,
                filename=filename,
                expected_size=expected,
                actual_size=actual,
                status="complete",
                last_action="downloaded",
            )
        except Exception as error:
            actual = partial.stat().st_size if partial.exists() else None
            return _state_row(
                product,
                filename=filename,
                expected_size=expected,
                actual_size=actual,
                status="failed",
                last_action="failed",
                error=str(error),
            )

    completed = 0
    skipped = 0
    failed = 0
    product_records = products.to_dict("records")
    partial_paths = [
        out_dir / f".{_product_filename(product)}.part"
        for product in product_records
    ]
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
                status="downloaded 0 · skipped 0 · failed 0",
            )

            with ThreadPoolExecutor(max_workers=processes) as pool:
                futures = {
                    pool.submit(_download_one, product): str(product["url"])
                    for product in product_records
                }
                for future in as_completed(futures):
                    row = future.result()
                    if row["last_action"] == "downloaded":
                        completed += 1
                    elif row["last_action"] == "skipped":
                        skipped += 1
                    else:
                        failed += 1
                    state = merge_state_rows(state, [row], key_columns=["url"])
                    write_parquet_atomic(state, state_file, index=False)
                    progress.update(
                        task_id,
                        advance=1,
                        status=(
                            f"downloaded {completed} · skipped {skipped} · "
                            f"failed {failed}"
                        ),
                    )
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

    return [
        f"Download started: {start_str}",
        f"Download ended:   {end_str}",
        f"Elapsed time:     {elapsed:.1f} seconds for {len(products)} files",
        f"Results:          {completed} downloaded, {skipped} skipped, {failed} failed",
        f"Download state:   {state_file}",
    ]
