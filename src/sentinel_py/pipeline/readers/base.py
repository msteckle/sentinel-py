"""Shared raster and point-sampling services for pipeline source readers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar, Protocol, runtime_checkable

import numpy as np
import pandas as pd
from affine import Affine

from sentinel_py.cache import (
    DEFAULT_PIPELINE_CACHE_DIR,
    deterministic_cache_key,
    mark_cache_used,
    write_parquet_atomic,
)
from sentinel_py.enums import AOIKind, SourceType
from sentinel_py.pipeline.base import Registry
from sentinel_py.pipeline.grid import AOIUnit, point_pixel


########################################################################################
# Polygon masking
########################################################################################


def _mask_block(
    values: np.ndarray, *, unit: AOIUnit, nodata: float, block_info: Any = None
) -> np.ndarray:
    """Apply one polygon's all-touched mask to a canonical-grid array block."""
    import rasterio.features

    location = block_info[None]["array-location"]
    row_start = location[-2][0]
    column_start = location[-1][0]
    grid = unit.grid
    if grid is None:
        return values
    transform = grid.affine @ Affine.translation(column_start, row_start)
    mask = rasterio.features.geometry_mask(
        [unit.geometry.__geo_interface__],
        out_shape=values.shape[-2:],
        transform=transform,
        invert=True,
        all_touched=True,
    )
    return np.where(mask, values, nodata).astype(values.dtype, copy=False)


def apply_polygon_mask(dataset: Any, unit: AOIUnit) -> Any:
    """Lazily mask every spatial variable in a polygon AOI dataset."""
    if unit.kind != AOIKind.polygon:
        return dataset
    import dask.array as da

    result = dataset.copy()
    for name, variable in dataset.data_vars.items():
        if not {"y", "x"}.issubset(variable.dims) or not hasattr(
            variable.data, "chunks"
        ):
            continue
        nodata = variable.attrs.get("nodata", dataset.attrs.get("nodata", np.nan))
        result[name] = variable.copy(
            data=da.map_blocks(
                _mask_block,
                variable.data,
                dtype=variable.dtype,
                unit=unit,
                nodata=nodata,
            )
        )
        result[name].attrs.update(variable.attrs)
    return result


########################################################################################
# Reader contract and registry
########################################################################################


POINT_SAMPLE_CACHE_VERSION = 1

# Sentinel-1 point rows carry polarization value columns plus per-scene attributes;
# consumers must not treat the attributes as values, nor apply Sentinel-2 integer
# sentinels (0 / 65535) to floating-point backscatter.
S1_POINT_POLARIZATIONS = frozenset({"VV", "VH", "HH", "HV"})
S1_POINT_ATTRIBUTE_COLUMNS = frozenset(
    {"flight_direction", "relative_orbit", "orbit_type", "units"}
)


class BaseReader(ABC):
    """Formulaic source adapter for raster and point pipeline access."""

    source_type: ClassVar[str]

    @abstractmethod
    def parse_config(self, source_id: str, raw: dict[str, Any], base_dir: Path) -> Any:
        """Validate one YAML source mapping and return its typed configuration."""

    @abstractmethod
    def read_raster(self, *args: Any, **kwargs: Any) -> Any:
        """Build the source's lazy raster artifact for planned polygon AOIs."""

    @abstractmethod
    def sample_points(self, *args: Any, **kwargs: Any) -> pd.DataFrame:
        """Return normalized observation rows for all requested point AOIs."""


class ReaderRegistry(Registry[BaseReader]):
    """Explicit mapping from pipeline source types to registered readers."""

    kind = "source reader type"

    @staticmethod
    def key_of(item: BaseReader) -> str:
        """Return the YAML source ``type`` handled by a reader."""
        return item.source_type

    @property
    def source_types(self) -> tuple[str, ...]:
        """Return registered source types in deterministic order."""
        return self.names


_DEFAULT_READERS: ReaderRegistry | None = None


def get_default_reader_registry() -> ReaderRegistry:
    """Return the built-in reader registry used by pipeline configuration parsing."""
    global _DEFAULT_READERS
    if _DEFAULT_READERS is None:
        from sentinel_py.pipeline.readers.arcticdem import ArcticDEMReader
        from sentinel_py.pipeline.readers.s1 import S1Reader
        from sentinel_py.pipeline.readers.s2 import S2Reader

        registry = ReaderRegistry()
        registry.register(S1Reader())
        registry.register(S2Reader(SourceType.s2_l2a_local))
        registry.register(S2Reader(SourceType.s2_l2a_remote))
        registry.register(ArcticDEMReader(SourceType.dem_local))
        registry.register(ArcticDEMReader(SourceType.dem_remote))
        _DEFAULT_READERS = registry
    return _DEFAULT_READERS


########################################################################################
# Point sampling requests, rows, and cache
########################################################################################


@dataclass(frozen=True)
class PointSampleRequest:
    """One canonical-grid point requested from a source-specific reader."""

    aoi_id: str
    component_id: str
    longitude: float
    latitude: float
    x: float
    y: float


@runtime_checkable
class PointSampler(Protocol):
    """Source adapter contract for cached, scene-aware point sampling."""

    reader_name: str

    def sample_points(self, requests: tuple[PointSampleRequest, ...]) -> pd.DataFrame:
        """Return normalized point rows for every relevant source observation."""


def point_sample_requests(
    units: tuple[AOIUnit, ...],
    *,
    crs: str,
    resolution: tuple[float, float],
    anchor: tuple[float, float],
) -> tuple[PointSampleRequest, ...]:
    """Convert planned point units into reusable source-reader requests."""

    requests = []
    for unit in units:
        if unit.kind != AOIKind.point:
            raise ValueError("Point sample requests require point AOI units")
        x, y = point_pixel(unit, crs=crs, resolution=resolution, anchor=anchor)
        point = unit.geometry_wgs84
        requests.append(
            PointSampleRequest(
                unit.aoi_id,
                unit.component_id,
                float(point.x),
                float(point.y),
                float(x),
                float(y),
            )
        )
    return tuple(requests)


def point_row(
    unit: AOIUnit,
    *,
    time: Any,
    product_id: str,
    granule_id: str,
    time_representation: str,
    source_fingerprint: str,
) -> dict[str, Any]:
    """Build the identity and provenance columns shared by every point row.

    Parameters
    ----------
    unit : AOIUnit
        Point AOI unit; its ``component_id`` must end in ``-<point index>``.
    time : Any
        Observation time (or a placeholder for static sources).
    product_id, granule_id : str
        Source product and granule identifiers.
    time_representation : str
        ``"acquisition"``, ``"static"``, or ``"composite"``.
    source_fingerprint : str
        Fingerprint of the source data that produced the row.

    Returns
    -------
    dict[str, Any]
        Row with identity, geometry, and provenance columns; callers add values.
    """
    return {
        "aoi_id": unit.aoi_id,
        "component_id": unit.component_id,
        "point_index": int(unit.component_id.rsplit("-", 1)[-1]),
        "longitude": float(unit.geometry_wgs84.x),
        "latitude": float(unit.geometry_wgs84.y),
        "time": time,
        "product_id": product_id,
        "granule_id": granule_id,
        "time_representation": time_representation,
        "source_fingerprint": source_fingerprint,
    }


def point_sample_cache_path(reader_name: str, cache_key: str) -> Path:
    """Return the shared, reader-namespaced cache location for point rows."""
    return (
        DEFAULT_PIPELINE_CACHE_DIR
        / "point-samples"
        / reader_name
        / f"{cache_key}.parquet"
    )


def point_sample_cache_key(
    *,
    reader_name: str,
    source_fingerprint: Any,
    requests: tuple[PointSampleRequest, ...],
    settings: dict[str, Any],
) -> str:
    """Build a key containing every input that changes sampled point values."""
    return deterministic_cache_key(
        {
            "version": POINT_SAMPLE_CACHE_VERSION,
            "reader": reader_name,
            "source": source_fingerprint,
            "requests": [request.__dict__ for request in requests],
            "settings": settings,
        }
    )


def load_point_sample_cache(reader_name: str, cache_key: str) -> pd.DataFrame | None:
    """Return a valid cached point table, or ``None`` when sampling is required."""
    path = point_sample_cache_path(reader_name, cache_key)
    if not path.is_file():
        return None
    mark_cache_used(path)
    return pd.read_parquet(path)


def save_point_sample_cache(
    reader_name: str,
    cache_key: str,
    rows: pd.DataFrame,
) -> Path:
    """Atomically persist source-normalized point samples for reuse."""
    path = point_sample_cache_path(reader_name, cache_key)
    write_parquet_atomic(rows, path, index=False)
    return path
