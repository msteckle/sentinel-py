"""Discover downloaded Sentinel-2 granules, assets, and product metadata."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from pathlib import Path

import geopandas as gpd
import pandas as pd
from lxml import etree
from shapely.geometry import box
from shapely.geometry.base import BaseGeometry

from sentinel_py.aoi import GeometryLike, aoi_as_geom
from sentinel_py.enums import S2_BAND_IDS
from sentinel_py.s2.base import S2Granule

S2_ACQUISITION_PATTERN = re.compile(r"_MSIL2A_(\d{8}T\d{6})_")
S2_TILE_PATTERN = re.compile(r"_(T\d{2}[A-Z]{3})_")


########################################################################################
# Product metadata discovery
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


########################################################################################
# SAFE product and raster asset discovery
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
    state_path = input_dir / ".sentinel-py" / "cdse_downloads.parquet"

    # Normalize the AOI once instead of reopening and reprojecting it per granule.
    aoi_geometry = None
    if aoi is not None:
        geometries = aoi_as_geom(aoi, "EPSG:4326")
        aoi_geometry = geometries.union_all()
        if aoi_geometry.is_empty:
            raise ValueError("The AOI contains no usable geometry")

    # Prefer the output-scoped download state to avoid recursively scanning the archive.
    safe_names: list[str] = []
    if state_path.is_file():
        state = pd.read_parquet(state_path, columns=["safedir", "download_status"])
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
            if aoi_geometry is not None:
                footprint = read_granule_footprint(granule_dir / "MTD_TL.xml")
                if not footprint.intersects(aoi_geometry):
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
                )
            )

    return granules
