"""Adapter exposing the existing Sentinel-2 VRT workflow as a processor."""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from pydantic import BaseModel, ConfigDict, Field, field_validator

from sentinel_py.enums import S2Bands
from sentinel_py.pipeline.execution import run_preprocess_tasks
from sentinel_py.pipeline.processor import ProcessorConfig, ProcessorContext
from sentinel_py.s2.base import S2PreprocessConfig, S2PreprocessResult

DEFAULT_BANDS = ("B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B11", "B12")
DEFAULT_MASK_CLASSES = (0, 1, 3, 8, 9, 10, 11)


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
            raise ValueError("bands must be a list")
        bands = tuple(dict.fromkeys(str(item).upper() for item in value))
        supported = {band.value for band in S2Bands}
        if not bands or any(band not in supported for band in bands):
            raise ValueError("bands must contain supported Sentinel-2 spectral bands")
        return bands

    @field_validator("mask_classes", mode="before")
    @classmethod
    def validate_mask_classes(cls, value: Any) -> tuple[int, ...]:
        if not isinstance(value, (list, tuple)):
            raise ValueError("mask_classes must be a list")
        if any(isinstance(item, bool) or not isinstance(item, int) for item in value):
            raise ValueError("mask_classes must contain integers")
        classes = tuple(sorted(set(value)))
        if any(item < 0 or item > 11 for item in classes):
            raise ValueError("mask_classes values must be from 0 through 11")
        return classes


@dataclass(frozen=True)
class S2PreprocessArtifact:
    """Materialized result returned by the compatibility VRT processor."""

    recipe_id: str
    state_path: Path
    results: tuple[S2PreprocessResult, ...]


class S2PreprocessProcessor:
    """Run the existing S2 preprocessing implementation behind the node contract."""

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
        if not isinstance(config, S2PreprocessNodeConfig):
            raise TypeError(f"{node_id} has an invalid s2.preprocess configuration")
        if inputs:
            raise ValueError(
                "s2.preprocess does not accept node inputs while using the "
                "legacy VRT implementation"
            )
        source = sources.get(config.source)
        if source is None:
            raise ValueError(f"{node_id}.source does not reference a configured source")
        if getattr(source, "source_type", None) != "s2.l2a.local":
            raise ValueError(f"{node_id}.source must reference an s2.l2a.local source")
        if output_grid.crs != "native":
            raise ValueError("output_grid.crs must be 'native' for s2.preprocess")
        if output_grid.resolution_m not in {10, 20, 60}:
            raise ValueError("output_grid.resolution_m must be 10, 20, or 60")
        if len(outputs) != 1 or outputs[0].format != "vrt":
            raise ValueError(
                "s2.preprocess currently requires exactly one named VRT output"
            )

    def execute(
        self,
        config: ProcessorConfig,
        inputs: Mapping[str, Any],
        context: ProcessorContext,
    ) -> S2PreprocessArtifact:
        if not isinstance(config, S2PreprocessNodeConfig):
            raise TypeError("Invalid s2.preprocess configuration")
        if inputs:
            raise ValueError("s2.preprocess does not accept upstream artifacts yet")

        from sentinel_py.s2.discover import discover_s2_granules
        from sentinel_py.s2.preprocess import (
            preprocess_recipe_id,
            read_preprocess_state,
            validate_gdal_for_preprocessing,
            write_preprocess_results,
        )

        clock = time.monotonic()
        pipeline = context.pipeline
        source = pipeline.sources[config.source]
        output = context.outputs[0]
        gdal_version = validate_gdal_for_preprocessing()
        preprocess_config = S2PreprocessConfig(
            bands=config.bands,
            resolution_m=pipeline.output_grid.resolution_m,
            mask_classes=config.mask_classes,
            nodata=config.nodata,
        )
        recipe_id = preprocess_recipe_id(preprocess_config)
        context.logger.info(
            "Starting pipeline=%s node=%s recipe=%s execution=%s GDAL=%s",
            pipeline.path,
            context.node.node_id,
            recipe_id,
            pipeline.execution.method,
            gdal_version,
        )
        granules = discover_s2_granules(
            source.data_dir,
            config.bands,
            pipeline.output_grid.resolution_m,
            aoi=pipeline.selection.aoi,
            years=pipeline.selection.years,
            speriod=pipeline.selection.start_period,
            eperiod=pipeline.selection.end_period,
        )
        if not granules:
            raise RuntimeError(
                f"Found 0 Sentinel-2 granules in {source.data_dir} for this selection"
            )
        state = read_preprocess_state(output.path)
        cached_rows: dict[tuple[str, str], dict[str, object]] = {}
        if not state.empty:
            matching = state[state["recipe_id"] == recipe_id]
            cached_rows = {
                (str(row["product_id"]), str(row["granule_id"])): {
                    str(key): value for key, value in row.items()
                }
                for row in matching.to_dict("records")
            }
        context.logger.info("Planned %d Sentinel-2 granule task(s)", len(granules))
        results = run_preprocess_tasks(
            granules,
            preprocess_config,
            output.path,
            cached_rows,
            pipeline.execution,
        )
        results.sort(key=lambda item: (item.product_id, item.granule_id))
        state_path = write_preprocess_results(output.path, state, results)
        for result in results:
            log = context.logger.error if result.status == "failed" else context.logger.info
            log(
                "task=%s product=%s granule=%s status=%s error=%s",
                result.task_id,
                result.product_id,
                result.granule_id,
                result.status,
                result.error,
            )
        context.logger.info(
            "Completed node=%s elapsed_seconds=%.1f",
            context.node.node_id,
            time.monotonic() - clock,
        )
        return S2PreprocessArtifact(recipe_id, state_path, tuple(results))
