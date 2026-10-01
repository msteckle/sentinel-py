"""ArcticDEM 10m discovery, cache selection, and download helpers."""

from __future__ import annotations

import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable
from urllib.parse import urlparse

import pandas as pd
import requests
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from shapely.geometry import base as shapely_base
from shapely.geometry import shape
from shapely import wkt

from sentinel_py.cache import merge_state_rows, write_parquet_atomic

PGC_CATALOG_URL = "https://stac.pgc.umn.edu/api/v1/"
PGC_COLLECTION = "arcticdem-mosaics-v4.1-10m"
PGC_ASSET_KEY = "dem"
PGC_MANIFEST_COLUMNS = [
    "item_id",
    "tile_id",
    "collection",
    "url",
    "filename",
    "bbox",
    "geometry_wkt",
    "expected_size",
    "etag",
    "asset_key",
    "asset_type",
]
PGC_DOWNLOAD_STATE_COLUMNS = [
    "item_id",
    "tile_id",
    "url",
    "filename",
    "path",
    "expected_size",
    "local_actual_size",
    "local_mtime_ns",
    "etag",
    "status",
    "last_action",
    "checked_at",
    "error",
]


@dataclass(frozen=True)
class PGCDownloadSummary:
    """Counts and user-facing messages from an ArcticDEM download."""

    downloaded: int
    skipped: int
    failed: int
    messages: list[str]

    def __iter__(self):
        """Iterate over user-facing summary messages."""
        return iter(self.messages)


def _asset_href(asset: Any) -> str:
    """Return an HTTPS URL for a public PGC STAC asset."""
    href = str(asset.href)
    if href.startswith("https://") or href.startswith("http://"):
        return href
    if href.startswith("s3://"):
        bucket, _, key = href[5:].partition("/")
        return f"https://{bucket}.s3.us-west-2.amazonaws.com/{key}"
    raise ValueError(f"Unsupported PGC asset URL: {href}")


def _asset_extra(asset: Any, key: str) -> Any:
    """Read an optional STAC asset field from a pystac asset."""
    extra_fields = getattr(asset, "extra_fields", {}) or {}
    return extra_fields.get(key)


def _item_row(item: Any) -> dict[str, Any]:
    """Convert one PGC STAC item into a cached DEM manifest row."""
    assets = getattr(item, "assets", {})
    if PGC_ASSET_KEY not in assets:
        raise ValueError(f"PGC item {item.id} has no '{PGC_ASSET_KEY}' asset")
    asset = assets[PGC_ASSET_KEY]
    url = _asset_href(asset)
    filename = Path(urlparse(url).path).name
    if not filename:
        raise ValueError(f"PGC item {item.id} DEM asset has no filename")
    properties = getattr(item, "properties", {}) or {}
    tile_id = properties.get("pgc:tile") or item.id
    geometry = getattr(item, "geometry", None)
    if geometry is None:
        raise ValueError(f"PGC item {item.id} has no geometry")
    return {
        "item_id": str(item.id),
        "tile_id": str(tile_id),
        "collection": PGC_COLLECTION,
        "url": url,
        "filename": filename,
        "bbox": list(getattr(item, "bbox", None) or []),
        "geometry_wkt": shape(geometry).wkt,
        "expected_size": _asset_extra(asset, "file:size"),
        "etag": _asset_extra(asset, "eTag") or _asset_extra(asset, "etag"),
        "asset_key": PGC_ASSET_KEY,
        "asset_type": getattr(asset, "media_type", None),
    }


def atomic_aoi_geometries(
    geometries: Iterable[shapely_base.BaseGeometry],
) -> list[shapely_base.BaseGeometry]:
    """Flatten and validate AOI geometries for independent PGC searches.

    Multi-part geometries are split into their component geometries so globally
    separated points or polygons never become one enclosing bounding box.

    Parameters
    ----------
    geometries : iterable of shapely geometries
        AOI feature geometries in WGS84.

    Returns
    -------
    list of shapely geometries
        Non-empty atomic geometries in deterministic WKT order.
    """
    components: list[shapely_base.BaseGeometry] = []

    def add(geometry: shapely_base.BaseGeometry) -> None:
        if geometry.is_empty:
            return
        if geometry.geom_type == "GeometryCollection" or geometry.geom_type.startswith(
            "Multi"
        ):
            for part in geometry.geoms:
                add(part)
            return
        components.append(geometry)

    for geometry in geometries:
        add(geometry)
    unique = {geometry.wkt: geometry for geometry in components}
    return [unique[key] for key in sorted(unique)]


def query_arcticdem(
    aoi_geometries: Iterable[shapely_base.BaseGeometry]
    | tuple[float, float, float, float],
    *,
    max_results: int | None = None,
    logger: logging.Logger | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
) -> pd.DataFrame:
    """Query PGC for ArcticDEM assets intersecting independent AOI geometries.

    Parameters
    ----------
    aoi_geometries : iterable of shapely geometries or tuple
        WGS84 AOI geometries. A tuple is accepted as a backwards-compatible
        single bounding box.
    max_results : int or None, optional
        Maximum number of returned items.
    logger : logging.Logger or None, optional
        Logger used for query diagnostics.
    """
    logger = logger or logging.getLogger(__name__)
    if isinstance(aoi_geometries, tuple) and len(aoi_geometries) == 4:
        west, south, east, north = (float(value) for value in aoi_geometries)
        if west > east or south > north:
            raise ValueError("bbox must be (west, south, east, north)")
        geometries = [shape({
            "type": "Polygon",
            "coordinates": [[
                [west, south], [east, south], [east, north], [west, north],
                [west, south],
            ]],
        })]
        legacy_bbox_mode = True
    else:
        geometries = atomic_aoi_geometries(aoi_geometries)
        legacy_bbox_mode = False
    if not geometries:
        raise ValueError("AOI must contain at least one non-empty geometry")
    try:
        import pystac_client
    except ImportError as error:  # pragma: no cover - dependency is declared
        raise RuntimeError("pystac-client is required for PGC queries") from error

    logger.info(
        "Querying PGC with %d independent AOI geometry component(s)", len(geometries)
    )

    def search_component(
        component: shapely_base.BaseGeometry,
    ) -> list[dict[str, Any]]:
        catalog = pystac_client.Client.open(PGC_CATALOG_URL)
        bbox = tuple(float(value) for value in component.bounds)
        search = catalog.search(
            collections=[PGC_COLLECTION],
            bbox=bbox,
            max_items=None,
        )
        rows = []
        for item in search.items():
            row = _item_row(item)
            if legacy_bbox_mode or component.intersects(wkt.loads(row["geometry_wkt"])):
                rows.append(row)
        return rows

    rows: list[dict[str, Any]] = []
    max_workers = min(len(geometries), 8)
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(search_component, geometry) for geometry in geometries]
        for completed, future in enumerate(as_completed(futures), start=1):
            rows.extend(future.result())
            if progress_callback is not None:
                progress_callback(completed, len(geometries))
    manifest = pd.DataFrame(rows, columns=PGC_MANIFEST_COLUMNS)
    if manifest.empty:
        return manifest
    manifest = manifest.drop_duplicates(subset=["url"]).sort_values("tile_id")
    if max_results is not None:
        manifest = manifest.head(max_results)
    return manifest.reset_index(drop=True)


def select_cached_tiles(
    manifest: pd.DataFrame,
    aoi_geometry: shapely_base.BaseGeometry,
    *,
    state_file: Path | None = None,
) -> pd.DataFrame:
    """Select cached ArcticDEM manifest rows intersecting an AOI.

    The function reads only the supplied manifest and optional download state;
    it never walks the output directory.

    Parameters
    ----------
    manifest : pandas.DataFrame
        Cached PGC query manifest.
    aoi_geometry : shapely geometry
        AOI geometry in WGS84.
    state_file : pathlib.Path or None, optional
        Persistent download-state parquet file.
    """
    if "geometry_wkt" not in manifest.columns:
        raise ValueError("PGC manifest is missing geometry_wkt")
    selected = manifest.copy()
    from shapely import wkt

    selected["_geometry"] = selected["geometry_wkt"].map(wkt.loads)
    selected = selected[selected["_geometry"].map(aoi_geometry.intersects)].copy()
    selected = selected.drop(columns=["_geometry"])
    if state_file is not None and Path(state_file).exists():
        state = pd.read_parquet(state_file)
        if not state.empty and "url" in state.columns:
            state = state.drop_duplicates("url", keep="last")
            selected = selected.merge(
                state[["url", "path", "status", "last_action", "error"]],
                on="url",
                how="left",
                suffixes=("", "_download"),
            )
    if "status" not in selected.columns:
        selected["status"] = pd.NA
    if "path" not in selected.columns:
        selected["path"] = pd.NA

    def local_status(row: pd.Series) -> str:
        """Classify one cached row using only its recorded local path."""
        status = row.get("status")
        if status == "failed":
            return "failed"
        path_value = row.get("path")
        if path_value is None or pd.isna(path_value):
            return "missing"
        path = Path(str(path_value))
        if not path.is_file():
            return "missing"
        expected = _expected_size(row.to_dict())
        if expected is not None and path.stat().st_size != expected:
            return "partial"
        return "downloaded" if status == "complete" else "partial"

    selected["download_status"] = selected.apply(local_status, axis=1)
    return selected.reset_index(drop=True)


def _expected_size(product: dict[str, Any]) -> int | None:
    value = product.get("expected_size")
    return None if value is None or pd.isna(value) else int(value)


def _state_row(product: dict[str, Any], **values: Any) -> dict[str, Any]:
    """Build one persistent PGC download-state row."""
    return {
        "item_id": product.get("item_id"),
        "tile_id": product.get("tile_id"),
        "url": str(product["url"]),
        "filename": product.get("filename"),
        "path": values.get("path"),
        "expected_size": _expected_size(product),
        "local_actual_size": values.get("local_actual_size"),
        "local_mtime_ns": values.get("local_mtime_ns"),
        "etag": product.get("etag"),
        "status": values["status"],
        "last_action": values["last_action"],
        "checked_at": datetime.now().isoformat(),
        "error": values.get("error"),
    }


def download_arcticdem(
    products: pd.DataFrame,
    out_dir: Path,
    *,
    processes: int = 4,
    retries: int = 3,
    retry_backoff: float = 2.0,
    state_file: Path | None = None,
    logger: logging.Logger | None = None,
) -> PGCDownloadSummary:
    """Download ArcticDEM DEM assets with persistent parallel state."""
    if processes < 1 or retries < 0 or retry_backoff < 0:
        raise ValueError("processes must be positive; retries and backoff non-negative")
    logger = logger or logging.getLogger(__name__)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    state_file = state_file or out_dir / ".sentinel-py" / "pgc_downloads.parquet"
    state = (
        pd.read_parquet(state_file)
        if state_file.exists()
        else pd.DataFrame(columns=PGC_DOWNLOAD_STATE_COLUMNS)
    )
    state_by_url = (
        state.drop_duplicates("url", keep="last").set_index("url").to_dict("index")
        if not state.empty and "url" in state.columns
        else {}
    )
    products = products.dropna(subset=["url"]).drop_duplicates("url").reset_index(drop=True)
    if products.empty:
        return PGCDownloadSummary(0, 0, 0, ["No ArcticDEM URLs to download."])

    def download_one(product: dict[str, Any]) -> dict[str, Any]:
        url = str(product["url"])
        filename = str(product.get("filename") or Path(urlparse(url).path).name)
        target = out_dir / filename
        partial = out_dir / f".{filename}.part"
        expected = _expected_size(product)
        cached = state_by_url.get(url, {})
        try:
            if target.is_file():
                actual = target.stat().st_size
                cached_complete = (
                    cached.get("status") == "complete"
                    and cached.get("local_actual_size") == actual
                )
                if (expected is not None and actual == expected) or (
                    expected is None and cached_complete
                ):
                    return _state_row(
                        product,
                        path=str(target),
                        local_actual_size=actual,
                        local_mtime_ns=target.stat().st_mtime_ns,
                        status="complete",
                        last_action="skipped",
                    )
            for attempt in range(retries + 1):
                partial.unlink(missing_ok=True)
                try:
                    with requests.get(url, stream=True, timeout=(30, 300)) as response:
                        response.raise_for_status()
                        with partial.open("wb") as stream:
                            for chunk in response.iter_content(1024 * 1024):
                                if chunk:
                                    stream.write(chunk)
                    actual = partial.stat().st_size
                    if expected is not None and actual != expected:
                        raise IOError(f"size mismatch: expected {expected}, got {actual}")
                    break
                except (requests.RequestException, OSError, IOError) as error:
                    partial.unlink(missing_ok=True)
                    if attempt >= retries:
                        raise
                    time.sleep(retry_backoff * (2**attempt))
            os.replace(partial, target)
            stat = target.stat()
            return _state_row(
                product,
                path=str(target),
                local_actual_size=actual,
                local_mtime_ns=stat.st_mtime_ns,
                status="complete",
                last_action="downloaded",
            )
        except Exception as error:
            partial.unlink(missing_ok=True)
            logger.error("FAILED %s: %s", filename, error)
            return _state_row(
                product,
                path=str(target),
                status="failed",
                last_action="failed",
                error=str(error),
            )

    completed = skipped = failed = 0
    records = products.to_dict("records")
    partial_paths = [
        out_dir / f".{record.get('filename') or Path(urlparse(str(record['url'])).path).name}.part"
        for record in records
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
            task = progress.add_task(
                "Checking & downloading ArcticDEM tiles",
                total=len(records),
                status="downloaded 0 · skipped 0 · failed 0",
            )
            with ThreadPoolExecutor(max_workers=processes) as pool:
                futures = [pool.submit(download_one, record) for record in records]
                for future in as_completed(futures):
                    row = future.result()
                    state = merge_state_rows(state, [row], key_columns=["url"])
                    write_parquet_atomic(state, state_file, index=False)
                    if row["last_action"] == "downloaded":
                        completed += 1
                    elif row["last_action"] == "skipped":
                        skipped += 1
                    else:
                        failed += 1
                    progress.update(
                        task,
                        advance=1,
                        status=(
                            f"downloaded {completed} · skipped {skipped} · failed {failed}"
                        ),
                    )
    finally:
        for partial_path in partial_paths:
            partial_path.unlink(missing_ok=True)
    return PGCDownloadSummary(
        completed,
        skipped,
        failed,
        [
            f"Results:          {completed} downloaded, {skipped} skipped, {failed} failed",
            f"Download status:  {state_file}",
        ],
    )
