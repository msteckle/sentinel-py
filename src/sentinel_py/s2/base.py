"""Base data classes shared by Sentinel-2 processing modules."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class S2Granule:
    """
    The local inputs needed to preprocess one Sentinel-2 granule.

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
    safe_dir : Path
        The path to the SAFE directory of the Sentinel-2 granule.
    product_metadata : Path
        The path to the product metadata file (XML) of the Sentinel-2 granule.
    band_paths : dict[str, Path]
        A dictionary mapping band names to their corresponding file paths.
    scl_path : Path | None
        The path to the scene classification layer file, if available.
    """

    product_id: str
    granule_id: str
    tile_id: str
    acquisition_time: str
    safe_dir: Path
    product_metadata: Path
    band_paths: dict[str, Path]
    scl_path: Path | None
