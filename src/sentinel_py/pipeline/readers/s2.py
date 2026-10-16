"""Sentinel-2 L2A source reader: discovery, lazy scene reads, and point sampling.

The module is organized in sections: the granule record, product metadata parsing,
local granule discovery, lazy scene/chunk reads on the canonical grid, lazy dataset
assembly, bulk point sampling, and the pipeline reader adapter.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import geopandas as gpd
import numpy as np
import pandas as pd
from affine import Affine
from lxml import etree
from shapely.geometry import box
from shapely.geometry.base import BaseGeometry

from sentinel_py.cache import REGISTRY
from sentinel_py.aoi import GeometryLike, aoi_as_geom
from sentinel_py.enums import S2_BAND_IDS, EarthSearchCollection, SourceType
import sentinel_py.pipeline.grid as pipeline_grid
from sentinel_py.pipeline.grid import (
    AOIUnit,
    ChunkShape,
    GridSpec,
    GridWindow,
    native_pixel_window,
    point_grid,
    select_resampling,
    warp_to_window,
)
from sentinel_py.pipeline.readers.base import BaseReader, apply_polygon_mask, point_row

S2_ACQUISITION_PATTERN = re.compile(r"_MSIL2A_(\d{8}T\d{6})_")
S2_TILE_PATTERN = re.compile(r"_(T\d{2}[A-Z]{3})_")

# Largest number of points one point-sampling task reads from a granule.
POINTS_PER_TASK = 500


########################################################################################
# Granule record
########################################################################################


@dataclass(frozen=True)
class S2Radiometry:
    """Radiometric metadata of one granule, for sources that do not ship SAFE XML.

    Attributes
    ----------
    offsets : dict[str, int]
        Signed DN offset (``BOA_ADD_OFFSET``) per spectral band.
    special_values : tuple[int, ...]
        Raw DNs that mean nodata or saturation.
    quantification_value : int
        DN-to-reflectance divisor (``BOA_QUANTIFICATION_VALUE``).
    """

    offsets: dict[str, int]
    special_values: tuple[int, ...]
    quantification_value: int


@dataclass(frozen=True)
class S2Granule:
    """
    The inputs needed to preprocess one Sentinel-2 granule, local or remote.

    Attributes
    ----------
    product_id : str
        The product identifier of the Sentinel-2 granule.
    granule_id : str
        The granule identifier of the Sentinel-2 granule.
    tile_id : str
        The tile identifier of the Sentinel-2 granule.
    acquisition_time : str
        The acquisition time of the Sentinel-2 granule.
    safe_dir : Path | None
        The path to the SAFE directory of the Sentinel-2 granule (``None`` for remote).
    product_metadata : Path | None
        The path to the product metadata file (XML) of the Sentinel-2 granule
        (``None`` for remote).
    band_paths : dict[str, str | Path]
        A dictionary mapping band names to their file paths or URLs.
    scl_path : str | Path | None
        The path or URL of the scene classification layer file, if available.
    footprint : Any | None
        WGS84 footprint of the granule.
    radiometry : S2Radiometry | None
        Radiometric metadata carried on the record; ``None`` means it is read from
        ``product_metadata``.
    fingerprint : str | None
        Identity of the granule inputs; ``None`` means it is derived from the files'
        size and modification time.
    """

    product_id: str
    granule_id: str
    tile_id: str
    acquisition_time: str
    safe_dir: Path | None
    product_metadata: Path | None
    band_paths: dict[str, str | Path]
    scl_path: str | Path | None
    footprint: Any | None = None
    radiometry: S2Radiometry | None = None
    fingerprint: str | None = None


########################################################################################
# Product metadata parsing
########################################################################################


def read_l2a_radiometry(metadata_path: Path) -> tuple[dict[str, int], int]:
    """Read per-band BOA offsets and the quantification value from L2A XML.

    Parameters
    ----------
    metadata_path
        Path to a product-level ``MTD_MSIL2A.xml`` file.

    Returns
    -------
    tuple[dict[str, int], int]
        A mapping from spectral band name to its signed ``BOA_ADD_OFFSET`` and
        the product ``BOA_QUANTIFICATION_VALUE``.

    Examples
    --------
    ``offsets, scale = read_l2a_radiometry(safe_dir / "MTD_MSIL2A.xml")``
    """
    metadata_path = Path(metadata_path)
    if not metadata_path.is_file():
        raise FileNotFoundError(
            f"Sentinel-2 product metadata not found: {metadata_path}"
        )

    # Parse by local element name so all supported product namespace versions work.
    tree = etree.parse(str(metadata_path))
    quantification_nodes = tree.xpath('//*[local-name()="BOA_QUANTIFICATION_VALUE"]')
    if len(quantification_nodes) != 1 or not quantification_nodes[0].text:
        raise ValueError(f"Expected one BOA_QUANTIFICATION_VALUE in {metadata_path}")
    quantification_value = int(float(quantification_nodes[0].text))

    # Convert ESA numeric band identifiers to the familiar Sentinel-2 band names.
    offsets_by_id: dict[str, int] = {}
    for node in tree.xpath('//*[local-name()="BOA_ADD_OFFSET"]'):
        band_id = node.get("band_id")
        if band_id is not None and node.text:
            offsets_by_id[band_id] = int(float(node.text))
    expected_ids = set(S2_BAND_IDS.values())
    if offsets_by_id and set(offsets_by_id) != expected_ids:
        missing = sorted(expected_ids - set(offsets_by_id), key=int)
        unexpected = sorted(set(offsets_by_id) - expected_ids)
        details = []
        if missing:
            details.append(f"missing band_id values {', '.join(missing)}")
        if unexpected:
            details.append(f"unexpected band_id values {', '.join(unexpected)}")
        raise ValueError(
            f"Incomplete BOA_ADD_OFFSET metadata in {metadata_path}: "
            + "; ".join(details)
        )
    offsets = {
        band: offsets_by_id.get(band_id, 0) for band, band_id in S2_BAND_IDS.items()
    }

    return offsets, quantification_value


def read_l2a_special_values(metadata_path: Path) -> dict[str, int]:
    """Read the radiometric NODATA and SATURATED DN codes from L2A XML."""
    metadata_path = Path(metadata_path)
    if not metadata_path.is_file():
        raise FileNotFoundError(
            f"Sentinel-2 product metadata not found: {metadata_path}"
        )

    tree = etree.parse(str(metadata_path))
    values: dict[str, int] = {}
    for group in tree.xpath('//*[local-name()="Special_Values"]'):
        names = group.xpath('./*[local-name()="SPECIAL_VALUE_TEXT"]/text()')
        indexes = group.xpath('./*[local-name()="SPECIAL_VALUE_INDEX"]/text()')
        if len(names) != 1 or len(indexes) != 1:
            raise ValueError(f"Malformed Special_Values metadata in {metadata_path}")
        values[str(names[0]).strip().upper()] = int(float(indexes[0]))

    required = {"NODATA", "SATURATED"}
    missing = sorted(required - set(values))
    if missing:
        raise ValueError(
            f"Missing Sentinel-2 special value(s) in {metadata_path}: "
            + ", ".join(missing)
        )
    return {name: values[name] for name in sorted(required)}


def read_granule_footprint(
    metadata_path: Path,
    output_crs: str = "EPSG:4326",
) -> BaseGeometry:
    """Read a Sentinel-2 granule footprint from its tile metadata.

    Parameters
    ----------
    metadata_path
        Path to a granule-level ``MTD_TL.xml`` file.
    output_crs
        CRS in which to return the footprint.

    Returns
    -------
    shapely.geometry.base.BaseGeometry
        Polygon covering the granule's raster grid in ``output_crs``.

    Examples
    --------
    ``footprint = read_granule_footprint(granule_dir / "MTD_TL.xml")``
    """
    metadata_path = Path(metadata_path)
    if not metadata_path.is_file():
        raise FileNotFoundError(f"Sentinel-2 tile metadata not found: {metadata_path}")

    # Read the CRS and the finest grid; every L2A resolution has the same extent.
    tree = etree.parse(str(metadata_path))
    crs_values = tree.xpath('//*[local-name()="HORIZONTAL_CS_CODE"]/text()')
    sizes = tree.xpath('//*[local-name()="Size"]')
    positions = tree.xpath('//*[local-name()="Geoposition"]')
    if len(crs_values) != 1:
        raise ValueError(f"Expected one HORIZONTAL_CS_CODE in {metadata_path}")
    size = min(
        sizes, key=lambda node: int(node.get("resolution", "10000")), default=None
    )
    position = min(
        positions,
        key=lambda node: int(node.get("resolution", "10000")),
        default=None,
    )
    if size is None or position is None:
        raise ValueError(f"Missing raster grid geometry in {metadata_path}")

    # Convert the upper-left grid origin and dimensions into a projected polygon.
    resolution = int(size.get("resolution"))
    nrows = int(size.xpath('./*[local-name()="NROWS"]/text()')[0])
    ncols = int(size.xpath('./*[local-name()="NCOLS"]/text()')[0])
    ulx = float(position.xpath('./*[local-name()="ULX"]/text()')[0])
    uly = float(position.xpath('./*[local-name()="ULY"]/text()')[0])
    footprint = box(
        ulx,
        uly - nrows * resolution,
        ulx + ncols * resolution,
        uly,
    )
    return gpd.GeoSeries([footprint], crs=str(crs_values[0])).to_crs(output_crs).iloc[0]


########################################################################################
# Local granule discovery
########################################################################################


def _product_identity(safe_dir: Path) -> tuple[str, str, str]:
    """Extract product, acquisition, and tile identifiers from a SAFE name."""
    product_id = safe_dir.name.removesuffix(".SAFE")
    acquisition_match = S2_ACQUISITION_PATTERN.search(safe_dir.name)
    tile_match = S2_TILE_PATTERN.search(safe_dir.name)
    if acquisition_match is None or tile_match is None:
        raise ValueError(f"Unrecognized Sentinel-2 L2A SAFE name: {safe_dir.name}")
    return product_id, acquisition_match.group(1), tile_match.group(1)


def _asset_resolution(path: Path) -> int:
    """Return the pixel resolution encoded in an L2A image filename."""
    match = re.search(r"_(10|20|60)m\.jp2$", path.name, flags=re.IGNORECASE)
    return int(match.group(1)) if match else 10_000


def _choose_asset(paths: list[Path], target_resolution_m: int) -> Path | None:
    """Choose the available asset closest to the requested output resolution."""
    existing = [path for path in paths if path.is_file()]
    return min(
        existing,
        key=lambda path: (
            abs(_asset_resolution(path) - target_resolution_m),
            str(path),
        ),
        default=None,
    )


def _acquisition_is_selected(
    acquisition_time: str,
    years: tuple[int, ...] | None,
    speriod: tuple[int, int],
    eperiod: tuple[int, int],
) -> bool:
    """Return whether an acquisition falls within the requested years and season."""
    acquired = datetime.strptime(acquisition_time, "%Y%m%dT%H%M%S").replace(tzinfo=UTC)
    if years is not None and acquired.year not in years:
        return False
    return speriod <= (acquired.month, acquired.day) <= eperiod


def discover_s2_granules(
    input_dir: Path,
    bands: tuple[str, ...],
    resolution_m: int,
    *,
    aoi: GeometryLike | None = None,
    years: tuple[int, ...] | None = None,
    speriod: tuple[int, int] = (1, 1),
    eperiod: tuple[int, int] = (12, 31),
) -> list[S2Granule]:
    """Discover one preprocessing task for every downloaded L2A granule.

    The CDSE download state is used when available so discovery does not walk a
    large archive. A direct SAFE scan is retained for products copied from
    elsewhere or downloaded before persistent state was introduced.

    Parameters
    ----------
    input_dir
        Directory containing ``*.SAFE`` products.
    bands
        Spectral bands required in each lazy preprocessed scene.
    resolution_m
        Target grid resolution used to select the closest local asset.
    aoi
        Optional area of interest. Only intersecting local granules are returned.
    years
        Optional acquisition years to retain.
    speriod, eperiod
        Inclusive ``(month, day)`` seasonal bounds applied within each year.

    Returns
    -------
    list[S2Granule]
        Deterministically ordered granule descriptions. Missing assets remain
        visible to the preprocessing worker and are reported as task failures.

    Examples
    --------
    ``granules = discover_s2_granules(raw_dir, ("B02", "B03"), 20, years=(2024,))``
    """
    input_dir = Path(input_dir).resolve()

    # Normalize the AOI once instead of reopening and reprojecting it per granule.
    aoi_geometry = None
    if aoi is not None:
        geometries = aoi_as_geom(aoi, "EPSG:4326")
        aoi_geometry = geometries.union_all()
        if aoi_geometry.is_empty:
            raise ValueError("The AOI contains no usable geometry")

    # Prefer the output-scoped download state to avoid recursively scanning the archive.
    safe_names: list[str] = []
    # RowStore includes checkpoint parts left by an interrupted download.
    state = REGISTRY["cdse_downloads"].store(input_dir).read(
        columns=["safedir", "download_status"]
    )
    if not state.empty:
        state = state[state["download_status"] == "complete"]
        safe_names = sorted(state["safedir"].dropna().astype(str).unique())
    if not safe_names:
        safe_names = sorted(path.name for path in input_dir.glob("*.SAFE"))

    # Discover granules within each product and select one source per requested asset.
    granules: list[S2Granule] = []
    for safe_name in safe_names:
        safe_dir = input_dir / safe_name
        if not safe_dir.is_dir():
            continue
        product_id, acquisition_time, tile_id = _product_identity(safe_dir)
        if not _acquisition_is_selected(
            acquisition_time,
            years,
            speriod,
            eperiod,
        ):
            continue
        product_metadata = safe_dir / "MTD_MSIL2A.xml"
        for granule_dir in sorted((safe_dir / "GRANULE").glob("*")):
            image_dir = granule_dir / "IMG_DATA"
            if not image_dir.is_dir():
                continue
            footprint = read_granule_footprint(granule_dir / "MTD_TL.xml")
            if aoi_geometry is not None and not footprint.intersects(aoi_geometry):
                continue
            band_paths = {
                band: selected
                for band in bands
                if (
                    selected := _choose_asset(
                        list(image_dir.glob(f"R*m/*_{band}_*m.jp2")), resolution_m
                    )
                )
                is not None
            }
            scl_path = _choose_asset(
                list(image_dir.glob("R*m/*_SCL_*m.jp2")), resolution_m
            )
            granules.append(
                S2Granule(
                    product_id=product_id,
                    granule_id=granule_dir.name,
                    tile_id=tile_id,
                    acquisition_time=acquisition_time,
                    safe_dir=safe_dir,
                    product_metadata=product_metadata,
                    band_paths=band_paths,
                    scl_path=scl_path,
                    footprint=footprint,
                )
            )

    return granules


# Callable that lists the granules usable for one AOI unit (``None``: all granules).
GranuleFinder = Callable[[AOIUnit | None], list[S2Granule]]


def local_granule_finder(
    data_dir: Any,
    bands: tuple[str, ...],
    resolution_m: int,
    *,
    years: tuple[int, ...] | None,
    speriod: tuple[int, int],
    eperiod: tuple[int, int],
) -> GranuleFinder:
    """Return a finder that scans a local SAFE archive, filtered to the unit's AOI."""

    def find(unit: AOIUnit | None = None) -> list[S2Granule]:
        """List local granules, restricted to those intersecting ``unit``."""
        return discover_s2_granules(
            data_dir, bands, resolution_m,
            aoi=None if unit is None else unit.geometry_wgs84,
            years=years, speriod=speriod, eperiod=eperiod,
        )

    return find


########################################################################################
# Lazy scene reads on the canonical grid
########################################################################################


@dataclass(frozen=True)
class S2SceneSource:
    """Serializable source metadata shared by every chunk of one scene."""

    granule: S2Granule
    bands: tuple[str, ...]
    offsets: dict[str, int]
    special_values: tuple[int, ...]
    quantification_value: int
    mask_classes: tuple[int, ...]
    nodata: int


def _correct_dn_values(
    values: np.ndarray,
    *,
    offset: int,
    nodata: int,
    invalid_values: tuple[int, ...],
) -> np.ndarray:
    """Mask XML special DNs before applying the signed BOA offset."""
    invalid = np.isin(values, invalid_values)
    corrected = np.maximum(values.astype(np.int32) + offset, 0)
    if corrected.max(initial=0) >= np.iinfo(np.uint16).max:
        raise ValueError("Corrected DN exceeds the UInt16 valid range")
    return np.where(invalid, nodata, corrected).astype(np.uint16)


def _window_dataset(
    path: str | Path,
    grid: GridSpec,
    window: GridWindow,
    *,
    nodata: int | None,
    offset: int | None,
    invalid_values: tuple[int, ...] = (),
) -> tuple[np.ndarray | None, Affine | None, object | None, float]:
    """Decode one native source window and retain its exact spatial metadata."""
    try:
        import rasterio
        from rasterio.windows import Window
    except ImportError as error:
        raise RuntimeError(
            "Rasterio is required when lazy Sentinel-2 chunks are computed. "
            "Install the processing dependencies."
        ) from error
    with pipeline_grid.raster_env([path]), rasterio.open(path) as source:
        source_resolution = abs(float(source.transform.a))
        source_window = native_pixel_window(source, window.bounds, grid.crs)
        if source_window is None:
            return None, None, None, source_resolution
        x_offset, y_offset, width, height = source_window
        raster_window = Window(x_offset, y_offset, width, height)
        values = source.read(1, window=raster_window)
        source_transform = source.window_transform(raster_window)
        source_crs = source.crs
    if offset is not None:
        if nodata is None:
            raise ValueError("A nodata value is required when applying a BOA offset")
        values = _correct_dn_values(
            values,
            offset=offset,
            nodata=nodata,
            invalid_values=invalid_values,
        )
    else:
        values = values.astype(np.uint16, copy=False)
    return values, source_transform, source_crs, source_resolution


def _read_scene_chunk(
    scene: S2SceneSource,
    grid: GridSpec,
    window: GridWindow,
) -> np.ndarray:
    """Read every band and SCL for one scene/window task."""
    with pipeline_grid.RASTERIO_LOCK:  # module access keeps the lock unpickled
        return _read_scene_chunk_locked(scene, grid, window)


def _read_scene_chunk_locked(
    scene: S2SceneSource,
    grid: GridSpec,
    window: GridWindow,
) -> np.ndarray:
    """Run one scene/window task while holding the process-local raster lock."""
    if scene.granule.scl_path is None:
        raise FileNotFoundError(f"Missing SCL asset for {scene.granule.granule_id}")
    scl_source, scl_transform, scl_crs, _ = _window_dataset(
        scene.granule.scl_path,
        grid,
        window,
        nodata=None,
        offset=None,
    )
    scl = warp_to_window(
        scl_source,
        scl_transform,
        scl_crs,
        grid,
        window,
        resampling="nearest",
        src_nodata=None,
        dst_nodata=None,
    )
    mask = (scl == 0) | (scl > 11)
    for value in scene.mask_classes:
        mask |= scl == value

    # Resolve every band asset before reading any, so a missing asset is reported
    # the same way whether or not the window turns out to be masked.
    band_paths = []
    for band_name in scene.bands:
        try:
            band_paths.append(scene.granule.band_paths[band_name])
        except KeyError as error:
            raise FileNotFoundError(
                f"Missing {band_name} asset for {scene.granule.granule_id}"
            ) from error

    output = np.empty(
        (len(scene.bands) + 1, window.height, window.width), dtype=np.uint16
    )
    if mask.all():
        # The SCL masks every pixel, so each band would be set to nodata below no
        # matter what it holds: skip decoding the band files (about 70% of point
        # windows over cloudy Arctic summers are fully masked).
        output[:-1] = scene.nodata
    else:
        for index, (band_name, path) in enumerate(
            zip(scene.bands, band_paths, strict=True)
        ):
            source, source_transform, source_crs, source_resolution = _window_dataset(
                path,
                grid,
                window,
                nodata=scene.nodata,
                offset=scene.offsets[band_name],
                invalid_values=scene.special_values,
            )
            values = warp_to_window(
                source,
                source_transform,
                source_crs,
                grid,
                window,
                resampling=select_resampling(source_resolution, grid),
                src_nodata=scene.nodata,
                dst_nodata=scene.nodata,
            )
            output[index] = np.where(
                # Raw no-data and saturation codes were already normalized to the
                # canonical nodata value before offset correction. A corrected DN
                # of zero can be valid and must remain distinguishable from nodata.
                mask | (values == scene.nodata), scene.nodata, values
            ).astype(np.uint16)
    output[-1] = scl
    return output


def _scene_source(
    granule: S2Granule,
    bands: tuple[str, ...],
    mask_classes: tuple[int, ...],
    nodata: int,
) -> S2SceneSource:
    """Read the radiometric metadata shared by every chunk of one granule."""
    if granule.radiometry is not None:
        offsets = granule.radiometry.offsets
        special_values = granule.radiometry.special_values
        quantification_value = granule.radiometry.quantification_value
    else:
        offsets, quantification_value = read_l2a_radiometry(granule.product_metadata)
        special_values = tuple(
            read_l2a_special_values(granule.product_metadata).values()
        )
    return S2SceneSource(
        granule,
        bands,
        offsets,
        special_values,
        quantification_value,
        mask_classes,
        nodata,
    )


def _source_fingerprint(scene: S2SceneSource) -> str:
    """Return the granule's identity, else a hash of every input file's path, size, mtime."""
    if scene.granule.fingerprint is not None:
        return scene.granule.fingerprint
    paths = [
        scene.granule.product_metadata,
        *[scene.granule.band_paths[band] for band in scene.bands],
    ]
    if scene.granule.scl_path is not None:
        paths.append(scene.granule.scl_path)
    values = []
    for path in sorted(paths, key=str):
        stat = path.stat()
        values.append((str(path.resolve()), stat.st_size, stat.st_mtime_ns))
    serialized = json.dumps(values, separators=(",", ":"))
    return hashlib.sha256(serialized.encode()).hexdigest()


########################################################################################
# Lazy dataset assembly
########################################################################################


def build_lazy_s2_dataset(
    granules: list[S2Granule],
    grid: GridSpec,
    *,
    bands: tuple[str, ...],
    mask_classes: tuple[int, ...],
    nodata: int,
):
    """Build an xarray Dataset without reading raster pixels or writing files."""
    try:
        import dask.array as da
        import xarray as xr
        from dask import delayed
    except ImportError as error:
        raise RuntimeError(
            "Lazy S2 processing requires xarray and Dask. Install the processing "
            "dependencies."
        ) from error

    windows = tuple(grid.iter_windows())
    scene_arrays = []
    scene_sources = [
        _scene_source(granule, bands, mask_classes, nodata) for granule in granules
    ]
    for scene in scene_sources:
        rows = []
        for row_index in range(grid.chunk_grid_shape[0]):
            blocks = []
            for window in windows:
                if window.row_index != row_index:
                    continue
                task = delayed(_read_scene_chunk, pure=True)(scene, grid, window)
                blocks.append(
                    da.from_delayed(
                        task,
                        shape=(len(bands) + 1, window.height, window.width),
                        dtype=np.uint16,
                    )
                )
            rows.append(da.concatenate(blocks, axis=2))
        scene_arrays.append(da.concatenate(rows, axis=1))
    stacked = da.stack(scene_arrays, axis=0)

    x = grid.bounds[0] + (np.arange(grid.width) + 0.5) * grid.resolution[0]
    y = grid.bounds[3] - (np.arange(grid.height) + 0.5) * grid.resolution[1]
    times = np.array(
        [
            np.datetime64(
                datetime.strptime(
                    source.granule.acquisition_time, "%Y%m%dT%H%M%S"
                ).isoformat(),
                "ns",
            )
            for source in scene_sources
        ]
    )
    dataset = xr.Dataset(
        data_vars={
            "reflectance": (
                ("time", "band", "y", "x"),
                stacked[:, : len(bands)],
                {
                    "long_name": "offset-corrected SCL-masked BOA reflectance DN",
                    "nodata": nodata,
                },
            ),
            "scl": (
                ("time", "y", "x"),
                stacked[:, -1].astype(np.uint8),
                {"long_name": "Sentinel-2 scene classification", "nodata": 0},
            ),
        },
        coords={
            "time": times,
            "band": list(bands),
            "y": y,
            "x": x,
            "product_id": (
                "time",
                [source.granule.product_id for source in scene_sources],
            ),
            "granule_id": (
                "time",
                [source.granule.granule_id for source in scene_sources],
            ),
            "quantification_value": (
                "time",
                [source.quantification_value for source in scene_sources],
            ),
            "source_fingerprint": (
                "time",
                [_source_fingerprint(source) for source in scene_sources],
            ),
        },
        attrs={
            "crs": grid.crs,
            "transform": tuple(grid.affine[:6]),
            "bounds": grid.bounds,
            "nodata": nodata,
        },
    )
    # Keep all spectral bands together so downstream band-wise operations and
    # multi-band COG writes do not need to rechunk the reflectance cube.
    dataset["reflectance"] = dataset.reflectance.chunk({"band": -1})
    return dataset


def read_s2_unit(
    unit: AOIUnit,
    *,
    data_dir: Any = None,
    bands: tuple[str, ...],
    resolution_m: int,
    years: tuple[int, ...] | None,
    speriod: tuple[int, int],
    eperiod: tuple[int, int],
    mask_classes: tuple[int, ...],
    nodata: int,
    grid: Any | None = None,
    find_granules: GranuleFinder | None = None,
) -> Any:
    """Build a lazy Sentinel-2 dataset for one planned polygon AOI unit.

    Granules come from ``find_granules`` (supplied by the source's reader) or, when it
    is omitted, from a scan of the local archive in ``data_dir``.
    """

    grid = grid or unit.grid
    if grid is None:
        raise ValueError("Sentinel-2 raster reads require a polygon AOI unit")
    if find_granules is None:
        find_granules = local_granule_finder(
            data_dir, bands, resolution_m, years=years, speriod=speriod, eperiod=eperiod
        )
    granules = find_granules(unit)
    if not granules:
        raise RuntimeError(f"Found 0 Sentinel-2 granules for AOI {unit.aoi_id}")
    dataset = build_lazy_s2_dataset(
        granules, grid, bands=bands, mask_classes=mask_classes, nodata=nodata,
    )
    return apply_polygon_mask(dataset, unit)


########################################################################################
# Bulk point sampling
########################################################################################


def _sample_granule_points(
    granule: Any,
    units: tuple[AOIUnit, ...],
    *,
    crs: str,
    resolution: tuple[float, float],
    anchor: tuple[float, float],
    bands: tuple[str, ...],
    mask_classes: tuple[int, ...],
    nodata: int,
) -> list[dict[str, Any]]:
    """Read one granule's canonical pixel for a batch of points (one worker task).

    The granule metadata is resolved once and every point reuses it, so a remote
    scene's files are opened once per batch rather than once per point.
    """

    scene = _scene_source(granule, bands, mask_classes, nodata)
    fingerprint = _source_fingerprint(scene)
    rows = []
    for unit in units:
        grid = point_grid(unit, crs=crs, resolution=resolution, anchor=anchor,
                          chunks=ChunkShape(y=1, x=1))
        values = _read_scene_chunk(scene, grid, next(grid.iter_windows()))[:, 0, 0]
        row = point_row(
            unit,
            time=granule.acquisition_time,
            product_id=granule.product_id,
            granule_id=granule.granule_id,
            time_representation="acquisition",
            source_fingerprint=fingerprint,
        )
        row["quantification_value"] = scene.quantification_value
        row.update({band: value.item() for band, value in zip(bands, values)})
        rows.append(row)
    return rows


def sample_s2_points(
    units: tuple[AOIUnit, ...],
    *,
    data_dir: Any = None,
    bands: tuple[str, ...],
    resolution_m: int,
    years: tuple[int, ...] | None,
    speriod: tuple[int, int],
    eperiod: tuple[int, int],
    mask_classes: tuple[int, ...],
    nodata: int,
    crs: str,
    resolution: tuple[float, float],
    anchor: tuple[float, float],
    execution: Any,
    progress: Callable[[int, int], None] | None = None,
    find_granules: GranuleFinder | None = None,
) -> pd.DataFrame:
    """Discover once and sample all intersecting scene/point pairs on the Dask backend.

    Work is split into one task per granule and batch of at most ``POINTS_PER_TASK``
    points and runs through ``execution`` (local, external Dask, or Slurm), exactly
    like raster outputs. ``progress`` counts scene/point pairs.
    """
    from sentinel_py.pipeline.execution import compute_dask_tasks

    if find_granules is None:
        find_granules = local_granule_finder(
            data_dir, bands, resolution_m, years=years, speriod=speriod, eperiod=eperiod
        )
    granules = find_granules(None)
    batches = []
    for granule in granules:
        matching = tuple(
            unit
            for unit in units
            if granule.footprint is not None
            and granule.footprint.intersects(unit.geometry_wgs84)
        )
        for start in range(0, len(matching), POINTS_PER_TASK):
            batches.append((granule, matching[start : start + POINTS_PER_TASK]))
    total = sum(len(batch) for _, batch in batches)
    if not total:
        raise RuntimeError("Found 0 Sentinel-2 granules intersecting the point AOIs")
    if progress is not None:
        progress(0, total)

    from dask import delayed

    tasks = [
        delayed(_sample_granule_points)(
            granule, batch, crs=crs, resolution=resolution, anchor=anchor,
            bands=bands, mask_classes=mask_classes, nodata=nodata,
        )
        for granule, batch in batches
    ]
    completed = 0

    def on_result(rows: list[dict[str, Any]]) -> None:
        """Advance the progress count by the points a finished task sampled."""
        nonlocal completed
        completed += len(rows)
        if progress is not None:
            progress(completed, total)

    results = compute_dask_tasks(tasks, execution, progress_callback=on_result)
    return pd.DataFrame.from_records([row for rows in results for row in rows])


########################################################################################
# Pipeline reader adapter
########################################################################################


class S2Reader(BaseReader):
    """Reader adapter for Sentinel-2 L2A: local SAFE archives or remote Earth Search COGs."""

    def __init__(self, source_type: SourceType = SourceType.s2_l2a_local) -> None:
        self.source_type = source_type

    def parse_config(self, source_id: str, raw: dict[str, Any], base_dir: Any) -> Any:
        """Parse the source configuration without coupling YAML parsing to readers."""
        from sentinel_py.pipeline.config import (
            PipelineConfigError,
            S2SourceConfig,
            only_keys,
            required,
            resolve_path,
        )

        if self.source_type == SourceType.s2_l2a_remote:
            only_keys(raw, {"type", "collection"}, f"sources.{source_id}")
            collection = raw.get("collection", EarthSearchCollection.c1_l2a.value)
            if collection not in {item.value for item in EarthSearchCollection}:
                choices = ", ".join(item.value for item in EarthSearchCollection)
                raise PipelineConfigError(
                    f"sources.{source_id}.collection must be one of: {choices}"
                )
            return S2SourceConfig(
                source_id, None, SourceType.s2_l2a_remote, str(collection)
            )
        only_keys(raw, {"type", "data_dir"}, f"sources.{source_id}")
        data_dir = resolve_path(
            required(raw, "data_dir", f"sources.{source_id}"),
            base_dir,
            f"sources.{source_id}.data_dir",
        )
        if not data_dir.is_dir():
            raise PipelineConfigError(f"S2 source directory does not exist: {data_dir}")
        return S2SourceConfig(source_id, data_dir)

    def granule_finder(
        self,
        source: Any,
        plan: Any,
        *,
        bands: tuple[str, ...],
        resolution_m: int,
        years: tuple[int, ...] | None,
        speriod: tuple[int, int],
        eperiod: tuple[int, int],
        logger: logging.Logger | None = None,
    ) -> GranuleFinder:
        """Return the function that lists the source's granules for an AOI unit."""
        if self.source_type == SourceType.s2_l2a_remote:
            from sentinel_py.pipeline.readers.s2_remote import RemoteGranuleFinder

            return RemoteGranuleFinder(
                plan, bands=bands, collection=source.collection, years=years,
                speriod=speriod, eperiod=eperiod, logger=logger,
            )
        return local_granule_finder(
            source.data_dir, bands, resolution_m,
            years=years, speriod=speriod, eperiod=eperiod,
        )

    def point_cache_fingerprint(self, source: Any, find_granules: GranuleFinder) -> Any:
        """Return the source identity folded into point-sample cache keys."""
        if self.source_type == SourceType.s2_l2a_remote:
            return find_granules.fingerprint
        # The state fingerprint covers checkpoint parts as well as the base file.
        fingerprint = REGISTRY["cdse_downloads"].store(source.data_dir).fingerprint()
        if fingerprint is None:
            fingerprint = (str(source.data_dir), source.data_dir.stat().st_mtime_ns)
        return fingerprint

    def read_raster(self, unit: AOIUnit, **kwargs: Any) -> Any:
        """Build one lazy canonical-grid raster read."""
        return read_s2_unit(unit, **kwargs)

    def sample_points(self, units: tuple[AOIUnit, ...], **kwargs: Any) -> pd.DataFrame:
        """Read all requested S2 point observations through the bulk sampler."""
        return sample_s2_points(units, **kwargs)
