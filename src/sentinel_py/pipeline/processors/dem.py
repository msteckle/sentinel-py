"""GDAL-compatible terrain features from local DEM GeoTIFFs."""

from __future__ import annotations

import math
from collections.abc import Mapping
from pathlib import Path
from typing import Any, ClassVar

import numpy as np
from pydantic import BaseModel, ConfigDict, Field, field_validator

from sentinel_py.cache import deterministic_cache_key
from sentinel_py.pipeline.lazy import read_reprojected_window
from sentinel_py.pipeline.processors.base import Processor, ProcessorConfig, ProcessorContext


class DEMFeature:
    """Base class and registry for extensible DEM-derived bands."""

    registry: ClassVar[dict[str, type[DEMFeature]]] = {}
    name: ClassVar[str]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        name = cls.__dict__.get("name")
        if not isinstance(name, str) or not name.strip():
            raise TypeError("DEM features must define a non-empty name")
        normalized = name.strip().lower()
        if normalized in DEMFeature.registry:
            raise ValueError(f"duplicate DEM feature name: {normalized}")
        if "calculate" not in cls.__dict__:
            raise TypeError(f"DEM feature {normalized} must define calculate")
        cls.name = normalized
        DEMFeature.registry[normalized] = cls

    @classmethod
    def calculate(cls, elevation: np.ndarray, **kwargs: Any) -> np.ndarray:
        """Calculate a feature from a native-grid elevation window."""
        raise NotImplementedError


def _gradients(elevation: np.ndarray, x: float, y: float, algorithm: str) -> tuple[np.ndarray, np.ndarray]:
    """Return GDAL Horn or Zevenbergen-Thorne gradients for a 3x3 window."""
    z = elevation
    if algorithm == "Horn":
        dx = ((z[:-2, 2:] + 2 * z[1:-1, 2:] + z[2:, 2:]) -
              (z[:-2, :-2] + 2 * z[1:-1, :-2] + z[2:, :-2])) / (8 * x)
        dy = ((z[2:, :-2] + 2 * z[2:, 1:-1] + z[2:, 2:]) -
              (z[:-2, :-2] + 2 * z[:-2, 1:-1] + z[:-2, 2:])) / (8 * y)
    else:
        dx = (z[1:-1, 2:] - z[1:-1, :-2]) / (2 * x)
        dy = (z[2:, 1:-1] - z[:-2, 1:-1]) / (2 * y)
    return dx, dy


def _valid_derivative_inputs(elevation: np.ndarray) -> np.ndarray:
    """Return validity for complete 3x3 neighborhoods."""
    valid = np.isfinite(elevation)
    return np.logical_and.reduce([valid[:-2, :-2], valid[:-2, 1:-1], valid[:-2, 2:],
                                  valid[1:-1, :-2], valid[1:-1, 1:-1], valid[1:-1, 2:],
                                  valid[2:, :-2], valid[2:, 1:-1], valid[2:, 2:]])


class ElevationFeature(DEMFeature):
    """Pass through native elevation values."""

    name = "elevation"

    @classmethod
    def calculate(cls, elevation: np.ndarray, **kwargs: Any) -> np.ndarray:
        del kwargs
        return elevation[1:-1, 1:-1].astype(np.float32, copy=False)


class SlopeFeature(DEMFeature):
    """GDAL-compatible slope in degrees."""

    name = "slope"

    @classmethod
    def calculate(cls, elevation: np.ndarray, **kwargs: Any) -> np.ndarray:
        dx, dy = _gradients(elevation, kwargs["x_resolution"], kwargs["y_resolution"], kwargs["algorithm"])
        result = np.degrees(np.arctan(np.hypot(dx, dy))).astype(np.float32)
        return np.where(_valid_derivative_inputs(elevation), result, np.nan)


class AspectFeature(DEMFeature):
    """GDAL-compatible aspect in degrees clockwise from north."""

    name = "aspect"

    @classmethod
    def calculate(cls, elevation: np.ndarray, **kwargs: Any) -> np.ndarray:
        dx, dy = _gradients(elevation, kwargs["x_resolution"], kwargs["y_resolution"], kwargs["algorithm"])
        result = np.degrees(np.arctan2(dx, -dy)) % 360.0
        flat = np.isclose(dx, 0.0) & np.isclose(dy, 0.0)
        return np.where(_valid_derivative_inputs(elevation) & ~flat, result, np.nan).astype(np.float32)


class HillshadeFeature(DEMFeature):
    """GDAL-compatible 0-255 hillshade."""

    name = "hillshade"

    @classmethod
    def calculate(cls, elevation: np.ndarray, **kwargs: Any) -> np.ndarray:
        dx, dy = _gradients(elevation, kwargs["x_resolution"], kwargs["y_resolution"], kwargs["algorithm"])
        slope = np.arctan(np.hypot(dx, dy))
        aspect = np.arctan2(dx, -dy)
        altitude = math.radians(kwargs["hillshade_altitude"])
        azimuth = math.radians(kwargs["hillshade_azimuth"])
        result = 255 * (np.sin(altitude) * np.cos(slope) +
                        np.cos(altitude) * np.sin(slope) * np.cos(azimuth - aspect))
        result = np.clip(result, 0, 255).astype(np.float32)
        return np.where(_valid_derivative_inputs(elevation), result, np.nan)


class DEMNodeConfig(BaseModel):
    """Validated parameters for a ``dem`` node."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    source: str = Field(min_length=1)
    features: tuple[str, ...] = ("elevation", "aspect", "slope", "hillshade")
    algorithm: str = "Horn"
    hillshade_azimuth: float = Field(default=315.0, ge=0, lt=360)
    hillshade_altitude: float = Field(default=45.0, ge=0, le=90)

    @field_validator("features", mode="before")
    @classmethod
    def validate_features(cls, value: Any) -> tuple[str, ...]:
        if not isinstance(value, (list, tuple)):
            raise TypeError("features must be a list")
        names = tuple(str(item).strip().lower() for item in value)
        if not names or any(not item for item in names):
            raise ValueError("features must contain non-empty names")
        if len(set(names)) != len(names):
            raise ValueError("features must not contain duplicates")
        unsupported = sorted(set(names) - set(DEMFeature.registry))
        if unsupported:
            raise ValueError("unsupported DEM features: " + ", ".join(unsupported))
        return names

    @field_validator("algorithm", mode="before")
    @classmethod
    def validate_algorithm(cls, value: Any) -> str:
        if not isinstance(value, str) or value not in {"Horn", "ZevenbergenThorne"}:
            raise ValueError("algorithm must be Horn or ZevenbergenThorne")
        return value


def _native_feature_chunk(paths: tuple[Path, ...], grid: Any, window: Any, config: DEMNodeConfig) -> np.ndarray:
    """Read a native DEM halo, calculate one output chunk, and align it."""
    try:
        import rasterio
        from rasterio.merge import merge
        from rasterio.warp import transform_bounds, reproject
        from rasterio.enums import Resampling
    except ImportError as error:
        raise RuntimeError("DEM processing requires rasterio") from error
    with rasterio.open(paths[0]) as first:
        source_crs = first.crs
        source_res = (abs(first.transform.a), abs(first.transform.e))
    with rasterio.Env():
        datasets = [rasterio.open(path) for path in paths]
        try:
            bounds = transform_bounds(grid.crs, source_crs, *window.bounds, densify_pts=21)
            bounds = (bounds[0] - source_res[0], bounds[1] - source_res[1],
                      bounds[2] + source_res[0], bounds[3] + source_res[1])
            source, transform = merge(datasets, bounds=bounds, nodata=datasets[0].nodata, masked=True)
            elevation = source[0].filled(np.nan).astype(np.float32)
        finally:
            for dataset in datasets:
                dataset.close()
    bands = []
    for feature_name in config.features:
        feature = DEMFeature.registry[feature_name]
        native = feature.calculate(
            elevation,
            x_resolution=source_res[0], y_resolution=source_res[1],
            algorithm=config.algorithm,
            hillshade_azimuth=config.hillshade_azimuth,
            hillshade_altitude=config.hillshade_altitude,
        )
        destination = np.full((window.height, window.width), np.nan, dtype=np.float32)
        native_transform = transform * rasterio.Affine.translation(1, 1)
        destination_transform = rasterio.Affine(
            grid.resolution[0], 0, window.bounds[0],
            0, -grid.resolution[1], window.bounds[3],
        )
        reproject(native, destination, src_transform=native_transform, src_crs=source_crs,
                  dst_transform=destination_transform, dst_crs=grid.crs,
                  src_nodata=np.nan, dst_nodata=np.nan,
                  resampling=getattr(Resampling, "bilinear"), num_threads=1)
        bands.append(destination)
    return np.stack(bands)


class DEMProcessor(Processor):
    """Build a lazy, canonical-grid DEM terrain feature cube."""

    type_name = "dem"
    config_model = DEMNodeConfig
    version = 2

    def validate(self, config: ProcessorConfig, *, node_id: str, inputs: Mapping[str, str], sources: Mapping[str, Any], outputs: tuple[Any, ...], output_grid: Any) -> None:
        if not isinstance(config, DEMNodeConfig):
            raise TypeError(f"{node_id} has an invalid dem configuration")
        if inputs:
            raise ValueError("dem does not accept upstream inputs")
        source = sources.get(config.source)
        if getattr(source, "source_type", None) != "dem.local":
            raise ValueError("dem.source must reference a dem.local source")
        if output_grid.grid is None:
            raise ValueError("dem requires a canonical output grid")
        if any(output.format not in {"cog", "xarray"} for output in outputs):
            raise ValueError("dem requires named COG or xarray outputs")

    def execute(self, config: ProcessorConfig, inputs: Mapping[str, Any], context: ProcessorContext) -> Any:
        if not isinstance(config, DEMNodeConfig) or inputs:
            raise TypeError("Invalid dem configuration or inputs")
        try:
            import xarray as xr
            import dask.array as da
            from dask import delayed
        except ImportError as error:
            raise RuntimeError("DEM processing requires xarray and Dask") from error
        source = context.pipeline.sources[config.source]
        paths = tuple(sorted(source.data_dir.glob(source.pattern)))
        if not paths:
            raise RuntimeError(f"No DEM GeoTIFFs matched {source.data_dir / source.pattern}")
        grid = context.pipeline.output_grid.grid
        arrays = []
        for window in grid.iter_windows():
            task = delayed(_native_feature_chunk)(paths, grid, window, config)
            arrays.append(da.from_delayed(task, shape=(len(config.features), window.height, window.width), dtype=np.float32))
        rows = []
        index = 0
        for row in range(grid.chunk_grid_shape[0]):
            rows.append(da.concatenate(arrays[index:index + grid.chunk_grid_shape[1]], axis=2))
            index += grid.chunk_grid_shape[1]
        data = da.concatenate(rows, axis=1)[None, ...]
        dataset = xr.Dataset(
            {"reflectance": (("time", "band", "y", "x"), data)},
            coords={"time": np.array(["1970-01-01"], dtype="datetime64[ns]"), "band": list(config.features),
                    "y": np.arange(grid.height), "x": np.arange(grid.width),
                    "product_id": ("time", ["dem"]), "granule_id": ("time", ["dem"]),
                    "source_fingerprint": ("time", [deterministic_cache_key([str(path) for path in paths])])},
            attrs={"crs": grid.crs, "transform": tuple(grid.affine[:6]), "nodata": -9999.0,
                   "processor": self.type_name,
                   "recipe_id": deterministic_cache_key({"version": self.version, **config.model_dump()})[:12]},
        )
        dataset["reflectance"].attrs["nodata"] = -9999.0
        return dataset
