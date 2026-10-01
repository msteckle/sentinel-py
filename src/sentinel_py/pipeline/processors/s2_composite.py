"""Seasonal time-series composites for Sentinel-2 processing cubes."""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import datetime
from typing import Any, Literal

import numpy as np
from pydantic import BaseModel, ConfigDict, Field, field_validator

from sentinel_py.cache import deterministic_cache_key
from sentinel_py.pipeline.processors.base import (
    Processor,
    ProcessorConfig,
    ProcessorContext,
)

_PERIOD_PATTERN = re.compile(r"^(?P<count>[1-9][0-9]*)(?P<unit>D|W|M|Y)$")


class S2CompositeNodeConfig(BaseModel):
    """Validated parameters belonging specifically to the ``composite`` node."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    period: str = Field(default="all", min_length=1)
    aggregation: Literal["mean", "median", "maximum"] = "mean"

    @field_validator("period", mode="before")
    @classmethod
    def validate_period(cls, value: Any) -> str:
        """Validate and normalize a seasonal bin period."""
        if value is None:
            return "all"
        if not isinstance(value, str):
            raise TypeError("period must be a string such as '15D' or 'all'")
        normalized = value.strip().upper()
        if normalized != "ALL" and _PERIOD_PATTERN.fullmatch(normalized) is None:
            raise ValueError(
                "period must be 'all' or a positive period such as '15D', '2W', "
                "'1M', or '1Y'"
            )
        return "all" if normalized == "ALL" else normalized


def _canonical_date(value: np.datetime64, *, year: int = 2000) -> np.datetime64:
    """Map an acquisition timestamp to the leap-year seasonal calendar."""
    timestamp = datetime.fromisoformat(np.datetime_as_string(value, unit="D"))
    return np.datetime64(datetime(year, timestamp.month, timestamp.day), "ns")


def _season_boundaries(
    start_period: tuple[int, int],
    end_period: tuple[int, int],
    period: str,
) -> tuple[np.datetime64, ...]:
    """Return seasonal bin boundaries anchored at the configured start period."""
    import pandas as pd

    start = pd.Timestamp(2000, start_period[0], start_period[1])
    end = pd.Timestamp(2000, end_period[0], end_period[1])
    if period == "all":
        return (start.to_datetime64(), (end + pd.Timedelta(days=1)).to_datetime64())

    match = _PERIOD_PATTERN.fullmatch(period)
    if match is None:
        raise ValueError(f"Unsupported composite period: {period}")
    count = int(match.group("count"))
    unit = match.group("unit")
    if unit == "D":
        offset = pd.DateOffset(days=count)
    elif unit == "W":
        offset = pd.DateOffset(weeks=count)
    elif unit == "M":
        offset = pd.DateOffset(months=count)
    else:
        offset = pd.DateOffset(years=count)

    boundaries = [start]
    while boundaries[-1] <= end:
        boundaries.append(boundaries[-1] + offset)
    boundaries[-1] = min(boundaries[-1], end + pd.Timedelta(days=1))
    return tuple(item.to_datetime64() for item in boundaries)


def _seasonal_groups(
    times: np.ndarray,
    boundaries: tuple[np.datetime64, ...],
) -> tuple[tuple[int, ...], ...]:
    """Group source time indices into non-empty seasonal bins."""
    canonical = np.array([_canonical_date(value) for value in times])
    indices = np.searchsorted(boundaries, canonical, side="right") - 1
    groups: list[tuple[int, ...]] = []
    for group_index in range(len(boundaries) - 1):
        members = tuple(np.flatnonzero(indices == group_index).tolist())
        if members:
            groups.append(members)
    return tuple(groups)


def _composite_name(source: Any, selection: Any, aggregation: str) -> str:
    """Build the stable product name used by composite output writers."""
    years = selection.years
    if years is None:
        values = source.time.values
        years = tuple(
            datetime.fromisoformat(np.datetime_as_string(value, unit="D")).year
            for value in values
        )
    if not years:
        raise ValueError("Composite naming requires at least one selected year")
    start_month, start_day = selection.start_period
    end_month, end_day = selection.end_period
    return (
        f"composite_{min(years):04d}-{max(years):04d}_"
        f"{start_month:02d}-{start_day:02d}-{end_month:02d}-{end_day:02d}_"
        f"{aggregation}"
    )


class S2CompositeProcessor(Processor):
    """Aggregate an upstream lazy Sentinel-2 cube into seasonal composites."""

    type_name = "composite"
    config_model = S2CompositeNodeConfig

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
        """Validate the composite's upstream dependency and output formats."""
        del sources, output_grid
        if not isinstance(config, S2CompositeNodeConfig):
            raise TypeError(f"{node_id} has an invalid composite configuration")
        if len(inputs) != 1:
            raise ValueError("composite requires exactly one upstream input")
        if any(output.format not in {"cog", "xarray"} for output in outputs):
            raise ValueError("composite requires named COG or xarray outputs")

    def execute(
        self,
        config: ProcessorConfig,
        inputs: Mapping[str, Any],
        context: ProcessorContext,
    ) -> Any:
        """Build lazy aggregate reductions without loading the source cube."""
        if not isinstance(config, S2CompositeNodeConfig):
            raise TypeError("Invalid composite configuration")
        if len(inputs) != 1:
            raise ValueError("composite requires exactly one upstream input")

        try:
            import xarray as xr
        except ImportError as error:
            raise RuntimeError("Composite processing requires xarray") from error

        source = next(iter(inputs.values()))
        if not isinstance(source, xr.Dataset) or "reflectance" not in source:
            raise TypeError(
                "composite input must be an xarray Dataset with reflectance"
            )
        if source.sizes.get("time", 0) == 0:
            raise ValueError("composite input contains no time steps")

        selection = context.pipeline.selection
        boundaries = _season_boundaries(
            selection.start_period,
            selection.end_period,
            config.period,
        )
        groups = _seasonal_groups(source.time.values, boundaries)
        if not groups:
            raise ValueError("composite input contains no observations in the season")
        composite_name = _composite_name(source, selection, config.aggregation)

        nodata = source.reflectance.attrs.get("nodata")
        values = source.reflectance
        if nodata is not None:
            values = values.where(values != nodata)
        reductions = []
        bin_labels = []
        source_fingerprints = []
        for group_index, members in enumerate(groups):
            subset = values.isel(time=list(members))
            valid = subset.notnull().any(dim="time")
            safe_subset = subset.where(valid, 0)
            if config.aggregation == "mean":
                composite = safe_subset.mean(dim="time", skipna=True)
            elif config.aggregation == "median":
                composite = safe_subset.median(dim="time", skipna=True)
            else:
                composite = safe_subset.max(dim="time", skipna=True)
            composite = composite.where(valid)
            if nodata is not None:
                composite = composite.fillna(nodata).astype(np.float32)
            reductions.append(
                composite.expand_dims(
                    time=[boundaries[group_index].astype("datetime64[ns]")]
                )
            )
            bin_labels.append(
                f"bin_{group_index:04d}_{str(boundaries[group_index])[:10]}"
            )
            source_fingerprints.append(
                deterministic_cache_key(
                    {
                        "source_fingerprints": [
                            str(value)
                            for value in source.source_fingerprint.values[list(members)]
                        ]
                    }
                )
            )

        result = (
            xr.concat(reductions, dim="time")
            .to_dataset(name="reflectance")
            .chunk({"time": -1, "band": -1})
        )
        result = result.assign_coords(
            product_id=("time", [composite_name] * len(groups)),
            granule_id=("time", bin_labels),
            source_fingerprint=("time", source_fingerprints),
        )
        result["reflectance"].attrs.update(source.reflectance.attrs)
        result["reflectance"].attrs["nodata"] = nodata
        result.attrs.update(source.attrs)
        result.attrs.update(
            processor=self.type_name,
            composite_name=composite_name,
            aggregation=config.aggregation,
            period=config.period,
            recipe_id=deterministic_cache_key(
                {
                    "processor": self.type_name,
                    "period": config.period,
                    "aggregation": config.aggregation,
                    "source_recipe": source.attrs.get("recipe_id"),
                }
            )[:12],
        )
        context.logger.info(
            "Built composite node=%s bins=%d aggregation=%s period=%s",
            context.node.node_id,
            len(groups),
            config.aggregation,
            config.period,
        )
        return result
