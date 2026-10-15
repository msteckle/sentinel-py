"""Sentinel-1 GRD source reader: local scene catalog and SNAP-facing discovery.

Sentinel-1 GRD archives are not read lazily; each scene is preprocessed by SNAP.
The catalog is built from each archive's ``manifest.safe`` (footprint, pass
direction, relative orbit, polarizations) and the orbit assignments staged by
``sentinel-py asf download``. Per-archive results are cached by file size and
modification time, so repeated runs over many terabytes read only new archives.
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path
from typing import Any

import pandas as pd
from shapely.geometry import Polygon

from sentinel_py.cache import REGISTRY, write_parquet_atomic
from sentinel_py.download.s1_orbits import parse_scene_name
from sentinel_py.enums import ASFOrbitDirection, S1OrbitPolicy, SourceType
from sentinel_py.pipeline.readers.base import BaseReader


########################################################################################
# Scene manifest parsing
########################################################################################

CATALOG_RELPATH = Path(REGISTRY["s1_catalog"].relpath)
CATALOG_VERSION = 1
CATALOG_COLUMNS = [
    "scene",
    "zip_name",
    "size",
    "mtime_ns",
    "platform",
    "product_type",
    "polarizations",
    "flight_direction",
    "relative_orbit",
    "start_time",
    "stop_time",
    "footprint_wkt",
    "catalog_version",
]
_MAX_MANIFEST_BYTES = 8 * 1024 * 1024
_NAMESPACES = {
    "safe": "http://www.esa.int/safe/sentinel-1.0",
    "s1": "http://www.esa.int/safe/sentinel-1.0/sentinel-1",
    "s1sarl1": "http://www.esa.int/safe/sentinel-1.0/sentinel-1/sar/level-1",
    "gml": "http://www.opengis.net/gml",
}


def _text(root: ET.Element, path: str) -> str | None:
    element = root.find(path, _NAMESPACES)
    return element.text.strip() if element is not None and element.text else None


def read_scene_manifest(zip_path: Path) -> dict[str, Any]:
    """Read catalog fields from one Sentinel-1 ZIP without extracting it.

    Parameters
    ----------
    zip_path : pathlib.Path
        Path to a Sentinel-1 ``.zip`` archive containing ``<scene>.SAFE``.

    Returns
    -------
    dict[str, Any]
        Catalog row values (without file size or modification time).

    Raises
    ------
    ValueError
        If the archive name or its ``manifest.safe`` is not a valid Sentinel-1
        manifest.
    """
    times = parse_scene_name(zip_path.name)
    if times is None:
        raise ValueError(f"not a Sentinel-1 product name: {zip_path.name}")
    manifest_name = f"{times.scene}.SAFE/manifest.safe"
    with zipfile.ZipFile(zip_path) as archive:
        try:
            info = archive.getinfo(manifest_name)
        except KeyError as error:
            raise ValueError(f"{zip_path.name} has no {manifest_name}") from error
        if info.file_size > _MAX_MANIFEST_BYTES:
            raise ValueError(f"{zip_path.name} manifest.safe is unexpectedly large")
        root = ET.fromstring(archive.read(info))

    # GML coordinates are space-separated "lat,lon" pairs.
    coordinates = _text(root, ".//safe:footPrint/gml:coordinates")
    if coordinates is None:
        raise ValueError(f"{zip_path.name} manifest.safe has no footprint")
    points = [tuple(map(float, pair.split(",")))[::-1] for pair in coordinates.split()]
    polarizations = [
        element.text.strip()
        for element in root.findall(".//s1sarl1:transmitterReceiverPolarisation", _NAMESPACES)
        if element.text
    ]
    relative = root.find(
        './/safe:relativeOrbitNumber[@type="start"]', _NAMESPACES
    )
    return {
        "scene": times.scene,
        "platform": times.platform,
        "product_type": _text(root, ".//s1sarl1:productType") or times.product,
        "polarizations": ",".join(polarizations),
        "flight_direction": (_text(root, ".//s1:pass") or "").upper() or None,
        "relative_orbit": int(relative.text) if relative is not None else None,
        "start_time": times.start.isoformat(),
        "stop_time": times.stop.isoformat(),
        "footprint_wkt": Polygon(points).wkt,
    }


########################################################################################
# Scene catalog and selection
########################################################################################


def build_scene_catalog(
    data_dir: Path,
    *,
    cache_path: Path | None = None,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    """Return one catalog row per Sentinel-1 ZIP in a directory.

    Archives whose size and modification time match a cached row are not reopened.
    Unreadable archives are skipped with a warning. The cache is written atomically
    and a read-only data directory only disables caching.

    Parameters
    ----------
    data_dir : pathlib.Path
        Directory containing downloaded Sentinel-1 ZIP archives.
    cache_path : pathlib.Path, optional
        Catalog cache; defaults to ``<data_dir>/.sentinel-py/s1_catalog.parquet``.
    logger : logging.Logger, optional
        Logger for cache decisions and skipped archives.

    Returns
    -------
    pandas.DataFrame
        Catalog rows sorted by acquisition start time then scene name.
    """
    logger = logger or logging.getLogger(__name__)
    data_dir = Path(data_dir)
    cache_path = cache_path or data_dir / CATALOG_RELPATH
    cached: dict[str, dict[str, Any]] = {}
    if cache_path.is_file():
        previous = pd.read_parquet(cache_path)
        previous = previous[previous["catalog_version"] == CATALOG_VERSION]
        cached = {row["zip_name"]: row for row in previous.to_dict("records")}

    rows: list[dict[str, Any]] = []
    reread = 0
    for zip_path in sorted(data_dir.glob("*.zip")):
        stat = zip_path.stat()
        row = cached.get(zip_path.name)
        if row is not None and row["size"] == stat.st_size and (
            row["mtime_ns"] == stat.st_mtime_ns
        ):
            rows.append(row)
            continue
        try:
            fields = read_scene_manifest(zip_path)
        except (ValueError, zipfile.BadZipFile, ET.ParseError, OSError) as error:
            logger.warning("Skipping unreadable Sentinel-1 archive %s: %s", zip_path, error)
            continue
        reread += 1
        rows.append(
            {
                **fields,
                "zip_name": zip_path.name,
                "size": stat.st_size,
                "mtime_ns": stat.st_mtime_ns,
                "catalog_version": CATALOG_VERSION,
            }
        )

    catalog = pd.DataFrame(rows, columns=CATALOG_COLUMNS)
    if not catalog.empty:
        catalog = catalog.sort_values(["start_time", "scene"]).reset_index(drop=True)
    logger.info(
        "Sentinel-1 catalog: %d scene(s) in %s (%d archive(s) read, %d from cache)",
        len(catalog),
        data_dir,
        reread,
        len(catalog) - reread,
    )
    if reread:
        try:
            write_parquet_atomic(catalog, cache_path, index=False)
        except OSError as error:
            logger.warning("Could not write Sentinel-1 catalog cache: %s", error)
    return catalog


def attach_orbits(catalog: pd.DataFrame, data_dir: Path) -> pd.DataFrame:
    """Join staged orbit assignments onto a scene catalog.

    Adds ``orbit_type``, ``orbit_file`` and an absolute ``orbit_path``. Scenes
    with no staged orbit, or whose staged file is missing on disk, have null
    values so later stages can fail fast with a precise list.
    """
    data_dir = Path(data_dir)
    result = catalog.copy()
    for column in ("orbit_type", "orbit_file", "orbit_path"):
        result[column] = pd.Series(pd.NA, index=result.index, dtype="object")
    # Reading through the store includes checkpoint parts of an interrupted run.
    state = REGISTRY["s1_orbits"].store(data_dir).read()
    if result.empty or state.empty:
        return result
    state = state[state["status"] == "staged"].drop_duplicates("scene", keep="last")
    by_scene = state.set_index("scene").to_dict("index")
    for index, scene in result["scene"].items():
        row = by_scene.get(scene)
        if row is None:
            continue
        path = data_dir / str(row["orbit_path"])
        if path.is_file():
            result.at[index, "orbit_type"] = row["orbit_type"]
            result.at[index, "orbit_file"] = row["orbit_file"]
            result.at[index, "orbit_path"] = str(path)
    return result


def select_scenes(
    catalog: pd.DataFrame,
    *,
    start: str | pd.Timestamp | None = None,
    end: str | pd.Timestamp | None = None,
    geometry_wgs84: Any | None = None,
    flight_direction: str = "BOTH",
    years: tuple[int, ...] | None = None,
    speriod: tuple[int, int] = (1, 1),
    eperiod: tuple[int, int] = (12, 31),
) -> pd.DataFrame:
    """Filter a catalog by time, footprint intersection, and pass direction.

    Parameters
    ----------
    catalog : pandas.DataFrame
        Output of :func:`build_scene_catalog`.
    start, end : str or pandas.Timestamp, optional
        Inclusive UTC acquisition window; a scene is kept when it overlaps it.
    geometry_wgs84 : shapely geometry, optional
        Area of interest in EPSG:4326; kept scenes' footprints must intersect it.
    flight_direction : str
        ``BOTH``, ``ASCENDING``, ``DESCENDING`` or ``PREDOMINANT``. Predominant is
        evaluated after the time and spatial filters; ties select ``ASCENDING``,
        matching the ASF query command.
    years : tuple[int, ...], optional
        Keep scenes acquired in these years.
    speriod, eperiod : tuple[int, int]
        Inclusive ``(month, day)`` season applied to the acquisition start, in every
        selected year (the same seasonal slot the Sentinel-2 reader uses).
    """
    from shapely import wkt

    selected = catalog
    if not selected.empty and (years is not None or (speriod, eperiod) != ((1, 1), (12, 31))):
        acquired = pd.to_datetime(selected["start_time"], utc=True)
        keep = pd.Series(True, index=selected.index)
        if years is not None:
            keep &= acquired.dt.year.isin(years)
        month_day = list(zip(acquired.dt.month, acquired.dt.day, strict=True))
        keep &= pd.Series(
            [speriod <= pair <= eperiod for pair in month_day], index=selected.index
        )
        selected = selected[keep]
    if start is not None:
        selected = selected[
            pd.to_datetime(selected["stop_time"], utc=True) >= pd.Timestamp(start, tz="UTC")
        ]
    if end is not None:
        selected = selected[
            pd.to_datetime(selected["start_time"], utc=True) <= pd.Timestamp(end, tz="UTC")
        ]
    if geometry_wgs84 is not None and not selected.empty:
        keep = [
            wkt.loads(footprint).intersects(geometry_wgs84)
            for footprint in selected["footprint_wkt"]
        ]
        selected = selected[keep]

    direction = flight_direction.upper()
    if direction == "PREDOMINANT" and not selected.empty:
        from sentinel_py.download.asf import get_predominant_flightdir

        direction = (
            get_predominant_flightdir(
                selected.rename(columns={"flight_direction": "flightDirection"})
            )
            or "BOTH"
        )
    if direction in {"ASCENDING", "DESCENDING"}:
        selected = selected[selected["flight_direction"] == direction]
    elif direction not in {"BOTH", "PREDOMINANT"}:
        raise ValueError(f"unsupported flight_direction: {flight_direction}")
    return selected.reset_index(drop=True)


########################################################################################
# Pipeline reader adapter
########################################################################################


class S1Reader(BaseReader):
    """Reader adapter for ASF-downloaded Sentinel-1 GRD ZIP archives.

    Sentinel-1 GRD archives are not read lazily. They are preprocessed scene by
    scene by SNAP (an external ``gpt`` step), so this reader only validates the
    source configuration and provides the scene catalog.
    """

    source_type = SourceType.s1_grd_local

    def parse_config(self, source_id: str, raw: dict[str, Any], base_dir: Any) -> Any:
        """Parse and validate one ``s1.grd.local`` source mapping."""
        from sentinel_py.pipeline.config import (
            PipelineConfigError,
            S1SourceConfig,
            only_keys,
            required,
            resolve_path,
        )

        location = f"sources.{source_id}"
        only_keys(raw, {"type", "data_dir", "flight_direction", "orbit"}, location)
        data_dir = resolve_path(
            required(raw, "data_dir", location), base_dir, f"{location}.data_dir"
        )
        if not data_dir.is_dir():
            raise PipelineConfigError(f"S1 source directory does not exist: {data_dir}")

        direction = raw.get("flight_direction", "BOTH")
        allowed_directions = {item.value for item in ASFOrbitDirection}
        if not isinstance(direction, str) or direction.upper() not in allowed_directions:
            raise PipelineConfigError(
                f"{location}.flight_direction must be one of: "
                + ", ".join(sorted(allowed_directions))
            )
        orbit = raw.get("orbit", S1OrbitPolicy.precise_or_restituted.value)
        allowed_orbits = {
            item.value for item in S1OrbitPolicy if item is not S1OrbitPolicy.none
        }
        if not isinstance(orbit, str) or orbit not in allowed_orbits:
            raise PipelineConfigError(
                f"{location}.orbit must be one of: " + ", ".join(sorted(allowed_orbits))
            )
        return S1SourceConfig(source_id, data_dir, direction.upper(), orbit)

    def scene_catalog(self, source: Any, **filters: Any) -> pd.DataFrame:
        """Return catalog rows, with staged orbit files, selected for one source.

        Parameters
        ----------
        source : S1SourceConfig
            Parsed source configuration.
        **filters
            Optional ``start``, ``end`` and ``geometry_wgs84`` arguments forwarded
            to :func:`select_scenes`.
        """

        catalog = build_scene_catalog(Path(source.data_dir))
        selected = select_scenes(
            catalog, flight_direction=source.flight_direction, **filters
        )
        return attach_orbits(selected, Path(source.data_dir))

    def read_raster(self, *args: Any, **kwargs: Any) -> Any:
        """S1 raster execution is owned by the SNAP processor."""
        raise NotImplementedError(
            "Sentinel-1 GRD data is preprocessed by a SNAP node, not read directly"
        )

    def sample_points(self, *args: Any, **kwargs: Any) -> Any:
        """S1 point execution is owned by the SNAP processor."""
        raise NotImplementedError(
            "Sentinel-1 GRD data is preprocessed by a SNAP node, not read directly"
        )
