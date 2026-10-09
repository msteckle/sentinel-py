from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator
from pyproj import CRS

from sentinel_py.cache import deterministic_cache_key
from sentinel_py.enums import S2Bands
from sentinel_py.pipeline.processors.base import (
    BaseProcessor,
    ProcessorConfig,
    ProcessorContext,
)

DEFAULT_BANDS = ("B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B11", "B12")
DEFAULT_MASK_CLASSES = (0, 1, 3, 8, 9, 10, 11)

########################################################################################
# s2.preprocess
########################################################################################


# Pydantic validation model for s2.preprocess parameters -------------------------------


class S2PreprocessNodeConfig(BaseModel):
    """Validated parameters belonging specifically to ``s2.preprocess``."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    source: str = Field(min_length=1)
    bands: tuple[str, ...] = DEFAULT_BANDS
    mask_classes: tuple[int, ...] = DEFAULT_MASK_CLASSES
    nodata: int = Field(default=65535, ge=0, le=65535)

    @field_validator("bands", mode="before")
    @classmethod
    def validate_bands(cls, value: Any) -> tuple[str, ...]:
        if not isinstance(value, (list, tuple)):
            raise TypeError("bands must be a list")
        bands = tuple(dict.fromkeys(str(item).upper() for item in value))
        supported = {band.value for band in S2Bands}
        if not bands or any(band not in supported for band in bands):
            raise ValueError("bands must contain supported Sentinel-2 spectral bands")
        return bands

    @field_validator("mask_classes", mode="before")
    @classmethod
    def validate_mask_classes(cls, value: Any) -> tuple[int, ...]:
        if not isinstance(value, (list, tuple)):
            raise TypeError("mask_classes must be a list")
        if any(isinstance(item, bool) or not isinstance(item, int) for item in value):
            raise ValueError("mask_classes must contain integers")
        classes = tuple(sorted(set(value)))
        if any(item < 0 or item > 11 for item in classes):
            raise ValueError("mask_classes values must be from 0 through 11")
        return classes


# Processor implementation for s2.preprocess -------------------------------------------


class S2PreprocessProcessor(BaseProcessor):
    """Build a lazy canonical-grid dataset without reading raster pixels."""

    type_name = "s2.preprocess"
    config_model = S2PreprocessNodeConfig

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

        # Ensure the configuration is of the correct type
        if not isinstance(config, S2PreprocessNodeConfig):
            raise TypeError(f"{node_id} has an invalid s2.preprocess configuration")
        # Ensure no inputs are provided (preprocessing should always come first)
        if inputs:
            raise ValueError("s2.preprocess does not accept node inputs")
        # Ensure the source is configured and of the correct type
        source = sources.get(config.source)
        if source is None:
            raise ValueError(f"{node_id}.source does not reference a configured source")
        # Ensure the source is of the correct type
        if getattr(source, "source_type", None) != "s2.l2a.local":
            raise ValueError(f"{node_id}.source must reference an s2.l2a.local source")
        # Ensure the output grid is configured and of the correct type
        if output_grid.chunks is None:
            raise ValueError("s2.preprocess requires a canonical output grid")
        target_crs = CRS.from_user_input(output_grid.crs)
        if not target_crs.is_projected or any(
            not abs(axis.unit_conversion_factor - 1.0) < 1e-12
            for axis in target_crs.axis_info
        ):
            raise ValueError(
                "canonical s2.preprocess currently requires a projected metre CRS"
            )
        # Ensure the nodata value is set correctly
        if config.nodata != 65535:
            raise ValueError("canonical s2.preprocess currently requires nodata 65535")
        # Ensure the output pixels are square
        if output_grid.resolution[0] != output_grid.resolution[1]:
            raise ValueError(
                "canonical s2.preprocess currently requires square output pixels"
            )
        # Ensure the outputs are one of the existing formats we have
        if outputs and any(
            output.format not in {"cog", "xarray", "parquet"} for output in outputs
        ):
            raise ValueError(
                "canonical s2.preprocess requires named COG or xarray outputs"
            )

    def execute_raster(
        self,
        config: ProcessorConfig,
        inputs: Mapping[str, Any],
        context: ProcessorContext,
    ) -> Any:
        # Validate the configuration and inputs before proceeding
        if not isinstance(config, S2PreprocessNodeConfig):
            raise TypeError("Invalid s2.preprocess configuration")
        if inputs:
            raise ValueError("s2.preprocess does not accept upstream artifacts yet")

        from sentinel_py.pipeline.aoi import point_grid
        from sentinel_py.pipeline.readers.s2 import read_s2_unit

        # Access the pipeline and output grid
        pipeline = context.pipeline
        if pipeline.output_grid.chunks is None:
            raise ValueError("Lazy S2 preprocessing requires output-grid chunks")
        source = pipeline.sources[config.source]
        datasets = {}
        for unit in pipeline.aoi_plan.units:
            grid = unit.grid or point_grid(
                unit, crs=pipeline.output_grid.crs,
                resolution=pipeline.output_grid.resolution,
                anchor=pipeline.output_grid.anchor,
                chunks=pipeline.output_grid.chunks,
            )
            dataset = read_s2_unit(
                unit, data_dir=source.data_dir, bands=config.bands,
                resolution_m=round(min(grid.resolution)), years=pipeline.selection.years,
                speriod=pipeline.selection.start_period, eperiod=pipeline.selection.end_period,
                mask_classes=config.mask_classes, nodata=config.nodata, grid=grid,
            )
            dataset.attrs.update(
                pipeline=str(pipeline.path), processor=self.type_name,
                node=context.node.node_id, time_representation="acquisition",
                recipe_id=deterministic_cache_key({
                    "processor": self.type_name, "bands": config.bands,
                    "mask_classes": config.mask_classes, "nodata": config.nodata,
                    "crs": grid.crs, "transform": tuple(grid.affine[:6]),
                })[:12],
            )
            datasets[(unit.aoi_id, unit.component_id)] = dataset
        context.logger.info("Building lazy node=%s AOI units=%d", context.node.node_id, len(datasets))
        from sentinel_py.pipeline.artifacts import RasterArtifact
        return RasterArtifact(pipeline.aoi_plan, datasets)

    def execute_points(
        self,
        config: ProcessorConfig,
        inputs: Mapping[str, Any],
        context: ProcessorContext,
    ) -> Any:
        """Sample compact point windows and return normalized observation rows.

        This deliberately materializes only one output pixel per source observation;
        downstream point processors work on the resulting table rather than a raster
        Dask graph.
        """
        if not isinstance(config, S2PreprocessNodeConfig) or inputs:
            raise TypeError("Invalid s2.preprocess configuration or inputs")
        from sentinel_py.pipeline.artifacts import PointSampleArtifact
        from sentinel_py.pipeline.readers.s2 import sample_s2_points
        from sentinel_py.pipeline.readers.base import (
            load_point_sample_cache,
            point_sample_cache_key,
            point_sample_requests,
            save_point_sample_cache,
        )

        pipeline = context.pipeline
        source = pipeline.sources[config.source]
        requests = point_sample_requests(
            pipeline.aoi_plan.units,
            crs=pipeline.output_grid.crs,
            resolution=pipeline.output_grid.resolution,
            anchor=pipeline.output_grid.anchor,
        )
        source_state = source.data_dir / ".sentinel-py" / "cdse_downloads.parquet"
        fingerprint = (
            (str(source_state), source_state.stat().st_size, source_state.stat().st_mtime_ns)
            if source_state.exists() else (str(source.data_dir), source.data_dir.stat().st_mtime_ns)
        )
        cache_key = point_sample_cache_key(
            reader_name="s2.l2a.local",
            source_fingerprint=fingerprint,
            requests=requests,
            settings={
                "bands": config.bands, "mask_classes": config.mask_classes,
                "nodata": config.nodata, "years": pipeline.selection.years,
                "start": pipeline.selection.start_period, "end": pipeline.selection.end_period,
                "crs": pipeline.output_grid.crs, "resolution": pipeline.output_grid.resolution,
                "anchor": pipeline.output_grid.anchor,
            },
        )
        rows = load_point_sample_cache("s2.l2a.local", cache_key)
        if rows is not None:
            context.logger.info("Using cached S2 point samples (%d rows)", len(rows))
            return PointSampleArtifact(rows, pipeline.aoi_plan)

        reported = -1

        def report(completed: int, total: int) -> None:
            """Make pre-writer source work visible on the normal console."""
            nonlocal reported
            if completed in {0, total} or completed - reported >= 25:
                reported = completed
                context.logger.warning(
                    "Sampling Sentinel-2 point scene/windows: %d/%d", completed, total
                )

        rows = sample_s2_points(
            pipeline.aoi_plan.units,
            data_dir=source.data_dir,
            bands=config.bands,
            resolution_m=round(min(pipeline.output_grid.resolution)),
            years=pipeline.selection.years,
            speriod=pipeline.selection.start_period,
            eperiod=pipeline.selection.end_period,
            mask_classes=config.mask_classes,
            nodata=config.nodata,
            crs=pipeline.output_grid.crs,
            resolution=pipeline.output_grid.resolution,
            anchor=pipeline.output_grid.anchor,
            workers=pipeline.execution.workers,
            progress=report,
        )
        save_point_sample_cache("s2.l2a.local", cache_key, rows)
        return PointSampleArtifact(rows, pipeline.aoi_plan)
