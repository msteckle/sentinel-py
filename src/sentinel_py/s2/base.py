"""Base data classes shared by Sentinel-2 processing modules."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class S2Granule:
    """Describe the local inputs needed to preprocess one Sentinel-2 granule."""

    product_id: str
    granule_id: str
    tile_id: str
    acquisition_time: str
    safe_dir: Path
    product_metadata: Path
    band_paths: dict[str, Path]
    scl_path: Path | None


@dataclass(frozen=True)
class S2PreprocessConfig:
    """Configuration that determines one reproducible preprocessing recipe."""

    bands: tuple[str, ...]
    resolution_m: int = 20
    mask_classes: tuple[int, ...] = (0, 1, 3, 8, 9, 10, 11)
    nodata: int = 65535


@dataclass(frozen=True)
class S2PreprocessResult:
    """Result returned by a preprocessing task worker."""

    task_id: str
    product_id: str
    granule_id: str
    output_path: Path
    status: str
    error: str | None
    state_row: dict[str, object]
