from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator
from pyproj import CRS

from sentinel_py.cache import deterministic_cache_key
from sentinel_py.enums import S2Bands
from sentinel_py.pipeline.processors.base import (
    Processor,
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


class S2PreprocessProcessor(Processor):
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
        if output_grid.grid is None:
            raise ValueError("s2.preprocess requires a canonical output grid")
        target_crs = CRS.from_user_input(output_grid.grid.crs)
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
        if output_grid.grid.resolution[0] != output_grid.grid.resolution[1]:
            raise ValueError(
                "canonical s2.preprocess currently requires square output pixels"
            )
        # Ensure the outputs are one of the existing formats we have
        if outputs and any(
            output.format not in {"cog", "xarray"} for output in outputs
        ):
            raise ValueError(
                "canonical s2.preprocess requires named COG or xarray outputs"
            )

    def execute(
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

        # Import functions for discovering granules and building lazy datasets
        from sentinel_py.s2.discover import discover_s2_granules
        from sentinel_py.s2.lazy import build_lazy_s2_dataset

        # Access the pipeline and output grid
        pipeline = context.pipeline
        grid = pipeline.output_grid.grid
        if grid is None:
            raise ValueError("Lazy S2 preprocessing requires a canonical output grid")
        # Access the source and determine the asset resolution
        source = pipeline.sources[config.source]
        asset_resolution = round(min(grid.resolution))
        # Discover the granules matching the selection criteria
        granules = discover_s2_granules(
            source.data_dir,
            config.bands,
            asset_resolution,
            aoi=pipeline.selection.aoi,
            years=pipeline.selection.years,
            speriod=pipeline.selection.start_period,
            eperiod=pipeline.selection.end_period,
        )
        # Ensure that at least one granule was found
        if not granules:
            raise RuntimeError(
                f"Found 0 Sentinel-2 granules in {source.data_dir} for this selection"
            )
        # Log the details of the lazy dataset being built
        context.logger.info(
            "Building lazy node=%s scenes=%d spatial_chunks=%d",
            context.node.node_id,
            len(granules),
            grid.chunk_grid_shape[0] * grid.chunk_grid_shape[1],
        )
        # Build the lazy dataset for the discovered granules
        dataset = build_lazy_s2_dataset(
            granules,
            grid,
            bands=config.bands,
            mask_classes=config.mask_classes,
            nodata=config.nodata,
        )
        # Update the dataset attributes with pipeline and processing information
        dataset.attrs.update(
            pipeline=str(pipeline.path),
            processor=self.type_name,
            node=context.node.node_id,
            recipe_id=deterministic_cache_key(
                {
                    "processor": self.type_name,
                    "bands": config.bands,
                    "mask_classes": config.mask_classes,
                    "nodata": config.nodata,
                    "crs": grid.crs,
                    "transform": tuple(grid.affine[:6]),
                    "shape": grid.shape,
                    "chunks": (grid.chunks.y, grid.chunks.x),
                }
            )[:12],
        )
        # Return the constructed lazy dataset
        return dataset
