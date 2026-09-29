"""Discover downloaded Sentinel-2 granules, assets, and product metadata."""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
from lxml import etree

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
        raise FileNotFoundError(f"Sentinel-2 product metadata not found: {metadata_path}")

    # Parse by local element name so all supported product namespace versions work.
    tree = etree.parse(str(metadata_path))
    quantification_nodes = tree.xpath(
        '//*[local-name()="BOA_QUANTIFICATION_VALUE"]'
    )
    if len(quantification_nodes) != 1 or not quantification_nodes[0].text:
        raise ValueError(
            f"Expected one BOA_QUANTIFICATION_VALUE in {metadata_path}"
        )
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
        band: offsets_by_id.get(band_id, 0)
        for band, band_id in S2_BAND_IDS.items()
    }

    return offsets, quantification_value


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
        key=lambda path: (abs(_asset_resolution(path) - target_resolution_m), str(path)),
        default=None,
    )


def discover_s2_granules(
    input_dir: Path,
    bands: tuple[str, ...],
    resolution_m: int,
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
        Spectral bands required in each preprocessed VRT.
    resolution_m
        Target grid resolution used to select the closest local asset.

    Returns
    -------
    list[S2Granule]
        Deterministically ordered granule descriptions. Missing assets remain
        visible to the preprocessing worker and are reported as task failures.

    Examples
    --------
    ``granules = discover_s2_granules(raw_dir, ("B02", "B03"), 20)``
    """
    input_dir = Path(input_dir).resolve()
    state_path = input_dir / ".sentinel-py" / "cdse_downloads.parquet"

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
        product_metadata = safe_dir / "MTD_MSIL2A.xml"
        for granule_dir in sorted((safe_dir / "GRANULE").glob("*")):
            image_dir = granule_dir / "IMG_DATA"
            if not image_dir.is_dir():
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
