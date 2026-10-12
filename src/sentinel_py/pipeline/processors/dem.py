"""GDAL-compatible terrain features from local DEM GeoTIFFs."""

from __future__ import annotations

import math
from collections.abc import Mapping
from pathlib import Path
from typing import Any, ClassVar

import numpy as np
from pydantic import BaseModel, ConfigDict, Field, field_validator

from sentinel_py.cache import deterministic_cache_key
from sentinel_py.pipeline.processors.base import (
    BaseProcessor,
    ProcessorConfig,
    ProcessorContext,
)


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


def _gradients(
    elevation: np.ndarray, x: float, y: float, algorithm: str
) -> tuple[np.ndarray, np.ndarray]:
    """Return GDAL Horn or Zevenbergen-Thorne gradients for a 3x3 window."""
    z = elevation
    if algorithm == "Horn":
        dx = (
            (z[:-2, 2:] + 2 * z[1:-1, 2:] + z[2:, 2:])
            - (z[:-2, :-2] + 2 * z[1:-1, :-2] + z[2:, :-2])
        ) / (8 * x)
        dy = (
            (z[2:, :-2] + 2 * z[2:, 1:-1] + z[2:, 2:])
            - (z[:-2, :-2] + 2 * z[:-2, 1:-1] + z[:-2, 2:])
        ) / (8 * y)
    else:
        dx = (z[1:-1, 2:] - z[1:-1, :-2]) / (2 * x)
        dy = (z[2:, 1:-1] - z[:-2, 1:-1]) / (2 * y)
    return dx, dy


def _valid_derivative_inputs(elevation: np.ndarray) -> np.ndarray:
    """Return validity for complete 3x3 neighborhoods."""
    valid = np.isfinite(elevation)
    return np.logical_and.reduce(
        [
            valid[:-2, :-2],
            valid[:-2, 1:-1],
            valid[:-2, 2:],
            valid[1:-1, :-2],
            valid[1:-1, 1:-1],
            valid[1:-1, 2:],
            valid[2:, :-2],
            valid[2:, 1:-1],
            valid[2:, 2:],
        ]
    )


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
        dx, dy = _gradients(
            elevation,
            kwargs["x_resolution"],
            kwargs["y_resolution"],
            kwargs["algorithm"],
        )
        result = np.degrees(np.arctan(np.hypot(dx, dy))).astype(np.float32)
        return np.where(_valid_derivative_inputs(elevation), result, np.nan)


class AspectFeature(DEMFeature):
    """GDAL-compatible aspect in degrees clockwise from north."""

    name = "aspect"

    @classmethod
    def calculate(cls, elevation: np.ndarray, **kwargs: Any) -> np.ndarray:
        dx, dy = _gradients(
            elevation,
            kwargs["x_resolution"],
            kwargs["y_resolution"],
            kwargs["algorithm"],
        )
        result = np.degrees(np.arctan2(dx, -dy)) % 360.0
        flat = np.isclose(dx, 0.0) & np.isclose(dy, 0.0)
        return np.where(
            _valid_derivative_inputs(elevation) & ~flat, result, np.nan
        ).astype(np.float32)


class HillshadeFeature(DEMFeature):
    """GDAL-compatible 0-255 hillshade."""

    name = "hillshade"

    @classmethod
    def calculate(cls, elevation: np.ndarray, **kwargs: Any) -> np.ndarray:
        dx, dy = _gradients(
            elevation,
            kwargs["x_resolution"],
            kwargs["y_resolution"],
            kwargs["algorithm"],
        )
        slope = np.arctan(np.hypot(dx, dy))
        aspect = np.arctan2(dx, -dy)
        altitude = math.radians(kwargs["hillshade_altitude"])
        azimuth = math.radians(kwargs["hillshade_azimuth"])
        result = 255 * (
            np.sin(altitude) * np.cos(slope)
            + np.cos(altitude) * np.sin(slope) * np.cos(azimuth - aspect)
        )
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


def _native_feature_chunk(
    paths: tuple[Path, ...], grid: Any, window: Any, config: DEMNodeConfig
) -> np.ndarray:
    """Read a native DEM halo, calculate one output chunk, and align it."""
    try:
        import rasterio
        from rasterio.enums import Resampling
        from rasterio.merge import merge
        from rasterio.warp import reproject, transform_bounds
    except ImportError as error:
        raise RuntimeError("DEM processing requires rasterio") from error
    with rasterio.open(paths[0]) as first:
        source_crs = first.crs
        source_res = (abs(first.transform.a), abs(first.transform.e))
        source_transform = first.transform
    with rasterio.Env():
        datasets = [rasterio.open(path) for path in paths]
        try:
            projected_bounds = transform_bounds(
                grid.crs, source_crs, *window.bounds, densify_pts=21
            )
            # Derivative and bilinear reprojection results must not depend on
            # the requested output-window extent.  Align every native read to
            # the source pixel lattice and retain a two-pixel halo: one pixel
            # for the 3x3 terrain derivative and one for resampling support.
            inverse = ~source_transform
            corners = [
                inverse * (x, y)
                for x, y in (
                    (projected_bounds[0], projected_bounds[1]),
                    (projected_bounds[0], projected_bounds[3]),
                    (projected_bounds[2], projected_bounds[1]),
                    (projected_bounds[2], projected_bounds[3]),
                )
            ]
            halo = 2
            native_chunk = 1024
            # Snap to a fixed native chunk lattice, rather than merely to a
            # pixel.  A point request and a polygon request that cover the
            # same source pixel therefore use the same terrain neighborhood.
            column_start = (
                math.floor(min(point[0] for point in corners) / native_chunk)
                * native_chunk
                - halo
            )
            column_end = (
                math.ceil(max(point[0] for point in corners) / native_chunk)
                * native_chunk
                + halo
            )
            row_start = (
                math.floor(min(point[1] for point in corners) / native_chunk)
                * native_chunk
                - halo
            )
            row_end = (
                math.ceil(max(point[1] for point in corners) / native_chunk)
                * native_chunk
                + halo
            )
            left, top = source_transform * (column_start, row_start)
            right, bottom = source_transform * (column_end, row_end)
            bounds = (left, bottom, right, top)
            source, transform = merge(
                datasets, bounds=bounds, nodata=datasets[0].nodata, masked=True
            )
            elevation = source[0].filled(np.nan).astype(np.float32)
        finally:
            for dataset in datasets:
                dataset.close()
    bands = []
    for feature_name in config.features:
        feature = DEMFeature.registry[feature_name]
        native = feature.calculate(
            elevation,
            x_resolution=source_res[0],
            y_resolution=source_res[1],
            algorithm=config.algorithm,
            hillshade_azimuth=config.hillshade_azimuth,
            hillshade_altitude=config.hillshade_altitude,
        )
        destination = np.full((window.height, window.width), np.nan, dtype=np.float32)
        native_transform = transform * rasterio.Affine.translation(1, 1)
        destination_transform = rasterio.Affine(
            grid.resolution[0],
            0,
            window.bounds[0],
            0,
            -grid.resolution[1],
            window.bounds[3],
        )
        reproject(
            native,
            destination,
            src_transform=native_transform,
            src_crs=source_crs,
            dst_transform=destination_transform,
            dst_crs=grid.crs,
            src_nodata=np.nan,
            dst_nodata=np.nan,
            resampling=Resampling.bilinear,
            num_threads=1,
        )
        bands.append(destination)
    return np.stack(bands)


class DEMProcessor(BaseProcessor):
    """Build a lazy, canonical-grid DEM terrain feature cube."""

    type_name = "dem"
    config_model = DEMNodeConfig
    # Version 3 aligns native source windows to a stable halo/chunk lattice.
    # It intentionally invalidates COG resume keys from extent-dependent terrain
    # derivatives produced by earlier versions.
    version = 3

    def validate(
        self,
        config: ProcessorConfig,
        *,
        node_id: str,
        inputs: Mapping[str, str],
        sources: Mapping[str, Any],
        outputs: tuple[Any, ...],
        output_grid: Any,
    ) -> None:
        if not isinstance(config, DEMNodeConfig):
            raise TypeError(f"{node_id} has an invalid dem configuration")
        if inputs:
            raise ValueError("dem does not accept upstream inputs")
        source = sources.get(config.source)
        if getattr(source, "source_type", None) != "dem.local":
            raise ValueError("dem.source must reference a dem.local source")
        if output_grid.chunks is None:
            raise ValueError("dem requires a canonical output grid")
        if any(output.format not in {"cog", "xarray", "parquet"} for output in outputs):
            raise ValueError("dem requires named COG or xarray outputs")

    def execute_raster(
        self,
        config: ProcessorConfig,
        inputs: Mapping[str, Any],
        context: ProcessorContext,
    ) -> Any:
        if not isinstance(config, DEMNodeConfig) or inputs:
            raise TypeError("Invalid dem configuration or inputs")
        try:
            import dask.array as da
            import xarray as xr
            from dask import delayed
        except ImportError as error:
            raise RuntimeError("DEM processing requires xarray and Dask") from error
        from sentinel_py.pipeline.aoi import point_grid
        from sentinel_py.pipeline.readers.arcticdem import select_dem_paths
        from sentinel_py.pipeline.readers.base import apply_polygon_mask

        source = context.pipeline.sources[config.source]
        paths = tuple(sorted(source.data_dir.glob(source.pattern)))
        if not paths:
            raise RuntimeError(
                f"No DEM GeoTIFFs matched {source.data_dir / source.pattern}"
            )
        pipeline = context.pipeline
        if pipeline.output_grid.chunks is None:
            raise ValueError("DEM requires output-grid chunks")
        datasets = {}
        for unit in pipeline.aoi_plan.units:
            grid = unit.grid or point_grid(
                unit,
                crs=pipeline.output_grid.crs,
                resolution=pipeline.output_grid.resolution,
                anchor=pipeline.output_grid.anchor,
                chunks=pipeline.output_grid.chunks,
            )
            selected_paths = select_dem_paths(paths, unit, grid=grid)
            if not selected_paths:
                raise RuntimeError(f"No DEM GeoTIFFs intersect AOI {unit.aoi_id}")
            arrays = []
            for window in grid.iter_windows():
                task = delayed(_native_feature_chunk)(
                    selected_paths, grid, window, config
                )
                arrays.append(
                    da.from_delayed(
                        task,
                        shape=(len(config.features), window.height, window.width),
                        dtype=np.float32,
                    )
                )
            rows = []
            index = 0
            for row in range(grid.chunk_grid_shape[0]):
                rows.append(
                    da.concatenate(
                        arrays[index : index + grid.chunk_grid_shape[1]], axis=2
                    )
                )
                index += grid.chunk_grid_shape[1]
            data = da.concatenate(rows, axis=1)[None, ...]
            dataset = xr.Dataset(
                {"reflectance": (("time", "band", "y", "x"), data)},
                coords={
                    "time": np.array(["1970-01-01"], dtype="datetime64[ns]"),
                    "band": list(config.features),
                    "y": np.arange(grid.height),
                    "x": np.arange(grid.width),
                    "product_id": ("time", ["dem"]),
                    "granule_id": ("time", ["dem"]),
                    "source_fingerprint": (
                        "time",
                        [deterministic_cache_key([str(path) for path in paths])],
                    ),
                },
                attrs={
                    "crs": grid.crs,
                    "transform": tuple(grid.affine[:6]),
                    "nodata": -9999.0,
                    "processor": self.type_name,
                    "time_representation": "static",
                    "recipe_id": deterministic_cache_key(
                        {"version": self.version, **config.model_dump()}
                    )[:12],
                },
            )
            dataset["reflectance"].attrs["nodata"] = -9999.0
            datasets[(unit.aoi_id, unit.component_id)] = apply_polygon_mask(
                dataset, unit
            )
        from sentinel_py.pipeline.artifacts import RasterArtifact

        return RasterArtifact(pipeline.aoi_plan, datasets)

    def execute_points(
        self,
        config: ProcessorConfig,
        inputs: Mapping[str, Any],
        context: ProcessorContext,
    ) -> Any:
        """Materialize only terrain-feature point pixels as normalized rows."""
        import pandas as pd

        from sentinel_py.pipeline.artifacts import PointSampleArtifact

        raster = self.execute_raster(config, inputs, context)
        records: list[dict[str, Any]] = []
        for key, dataset in raster.datasets.items():
            unit = raster.plan.by_key[key]
            values = dataset.reflectance.compute().values[0, :, 0, 0]
            record = {
                "aoi_id": unit.aoi_id,
                "component_id": unit.component_id,
                "point_index": int(unit.component_id.rsplit("-", 1)[-1]),
                "longitude": float(unit.geometry_wgs84.x),
                "latitude": float(unit.geometry_wgs84.y),
                "time": "1970-01-01",
                "product_id": "dem",
                "granule_id": "dem",
                "time_representation": "static",
                "source_fingerprint": str(dataset.source_fingerprint.values[0]),
            }
            record.update(
                {
                    feature: value.item()
                    for feature, value in zip(config.features, values)
                }
            )
            records.append(record)
        return PointSampleArtifact(
            pd.DataFrame.from_records(records), context.pipeline.aoi_plan
        )
