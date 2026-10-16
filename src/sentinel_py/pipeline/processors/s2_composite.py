"""Seasonal time-series composites for gridded ``(time, band, y, x)`` cubes.

Built for Sentinel-2 reflectance and equally applied to Sentinel-1 backscatter: the
aggregated variable is ``reflectance`` when present, otherwise the cube's only
``(time, band, y, x)`` variable, or the node's ``variable``.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

import numpy as np
from pydantic import BaseModel, ConfigDict, Field, field_validator

from sentinel_py.cache import deterministic_cache_key
from sentinel_py.enums import S2_NODATA
from sentinel_py.pipeline.readers.base import (
    POINT_METADATA_COLUMNS,
    S1_POINT_ATTRIBUTE_COLUMNS,
    S1_POINT_POLARIZATIONS,
)
from sentinel_py.pipeline.processors.base import (
    BaseProcessor,
    make_recipe_id,
    mask_nodata,
    ProcessorConfig,
    ProcessorContext,
    select_cube_variable,
)

_PERIOD_PATTERN = re.compile(r"^(?P<count>[1-9][0-9]*)(?P<unit>D|W|M|Y)$")

# Bump when composite output changes so cached COG resume keys are invalidated.
# 3: bins after an empty bin kept their position in the compacted list, so their
# time coordinate and ``bin_NNNN_<date>`` label named the wrong seasonal slot.
PROCESSOR_VERSION = 3


########################################################################################
# Node configuration
########################################################################################


class S2CompositeNodeConfig(BaseModel):
    """Validated parameters belonging specifically to the ``composite`` node."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    period: str = Field(default="all", min_length=1)
    aggregation: Literal["mean", "median", "maximum"] = "mean"
    variable: str | None = Field(default=None, min_length=1)
    empty_bins: Literal["drop", "fill"] = "drop"

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


########################################################################################
# Seasonal grouping helpers
########################################################################################


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
    *,
    keep_empty: bool = False,
) -> tuple[tuple[int, tuple[int, ...]], ...]:
    """Group source time indices into seasonal bins.

    Parameters
    ----------
    times : numpy.ndarray
        Acquisition timestamps.
    boundaries : tuple[numpy.datetime64, ...]
        Bin boundaries from :func:`_season_boundaries`.
    keep_empty : bool, default False
        Also return bins without observations (with no members), so every seasonal
        slot is represented even when no data exists for it.

    Returns
    -------
    tuple[tuple[int, tuple[int, ...]], ...]
        ``(bin index, member time indices)`` pairs. The bin index is the position of
        the bin in the full season, whether or not earlier bins are returned.
    """
    bin_count = len(boundaries) - 1
    if len(times):
        canonical = np.array([_canonical_date(value) for value in times])
        indices = np.searchsorted(boundaries, canonical, side="right") - 1
    else:
        indices = np.array([], dtype=int)
    groups: list[tuple[int, tuple[int, ...]]] = []
    for bin_index in range(bin_count):
        members = tuple(np.flatnonzero(indices == bin_index).tolist())
        if members or keep_empty:
            groups.append((bin_index, members))
    return tuple(groups)


def _nodata_composite(values: Any) -> Any:
    """Return an all-NaN ``(band, y, x)`` composite shaped like ``values`` without time."""
    import dask.array as da
    import xarray as xr

    dims = [name for name in values.dims if name != "time"]
    shape = tuple(values.sizes[name] for name in dims)
    return xr.DataArray(
        da.full(shape, np.nan, dtype=np.float32, chunks=shape),
        dims=dims,
        coords={name: values.coords[name] for name in dims if name in values.coords},
    )


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


@dataclass(frozen=True)
class _PointBin:
    """One seasonal bin of a point composite and the labels written on its rows."""

    index: int
    time: str
    granule_id: str


def _point_bins(
    boundaries: tuple[np.datetime64, ...], period: str
) -> tuple[_PointBin, ...]:
    """Describe every seasonal bin of a point composite.

    The single bin of ``period: all`` keeps the labels point composites have always
    used (``time`` is the season start as ``MM-DD`` and ``granule_id`` is ``all``);
    binned periods label bins like the raster composites do.
    """
    bins = []
    for index in range(len(boundaries) - 1):
        stamp = str(boundaries[index])[:10]  # year 2000, the shared seasonal calendar
        bins.append(
            _PointBin(
                index,
                stamp[5:],
                "all" if period == "all" else f"bin_{index:04d}_{stamp}",
            )
        )
    return tuple(bins)


def _point_bin_indices(times: Any, boundaries: tuple[np.datetime64, ...]) -> np.ndarray:
    """Map observation times to seasonal bin indices, shared across years.

    Parameters
    ----------
    times : pandas.Series
        Acquisition timestamps as strings (``%Y%m%dT%H%M%S`` or ISO dates).
    boundaries : tuple[numpy.datetime64, ...]
        Bin boundaries from :func:`_season_boundaries`.

    Returns
    -------
    numpy.ndarray
        Bin index per row; ``-1`` or ``len(boundaries) - 1`` mean outside the season.

    Raises
    ------
    ValueError
        If a timestamp cannot be parsed.
    """
    import pandas as pd

    try:
        stamps = pd.to_datetime(times.astype(str), format="mixed")
    except (ValueError, TypeError) as error:
        raise ValueError(f"composite cannot parse point observation times: {error}") from error
    canonical = pd.to_datetime(
        {"year": 2000, "month": stamps.dt.month, "day": stamps.dt.day}
    )
    return np.searchsorted(
        np.array(boundaries, dtype="datetime64[ns]"),
        canonical.to_numpy(dtype="datetime64[ns]"),
        side="right",
    ) - 1


def _fill_missing_points(
    aggregate: Any, plan: Any, value_columns: list[str], bins: tuple[_PointBin, ...]
) -> Any:
    """Append an all-NaN composite row for every planned point and bin without data.

    Parameters
    ----------
    aggregate : pandas.DataFrame
        Composite rows with a ``bin`` column, for the (point, bin) pairs that had
        observations.
    plan : AOIPlan
        Pipeline AOI plan whose ``units`` list every point.
    value_columns : list[str]
        Value columns to set to NaN on the added rows.
    bins : tuple[_PointBin, ...]
        Every seasonal bin that must be present for every point.

    Returns
    -------
    pandas.DataFrame
        ``aggregate`` plus one nodata row per missing (point, bin) pair.
    """
    import pandas as pd

    from sentinel_py.enums import AOIKind
    from sentinel_py.pipeline.readers.base import point_row

    present = set(
        zip(aggregate["aoi_id"], aggregate["component_id"], aggregate["bin"], strict=True)
    )
    added = []
    for unit in getattr(plan, "units", ()):
        if unit.kind != AOIKind.point:
            continue
        for point_bin in bins:
            if (unit.aoi_id, unit.component_id, point_bin.index) in present:
                continue
            row = point_row(
                unit, time=point_bin.time, product_id="composite",
                granule_id=point_bin.granule_id, time_representation="composite",
                source_fingerprint="composite",
            )
            row.update({"bin": point_bin.index, **{name: np.nan for name in value_columns}})
            added.append(row)
    if not added:
        return aggregate
    if aggregate.empty:
        return pd.DataFrame(added)
    return pd.concat([aggregate, pd.DataFrame(added)], ignore_index=True)


########################################################################################
# Processor class for seasonal Sentinel-2 composites
########################################################################################


class S2CompositeProcessor(BaseProcessor):
    """Aggregate an upstream lazy Sentinel-2 cube into seasonal composites."""

    type_name = "composite"
    config_model = S2CompositeNodeConfig
    input_kind = None
    version = PROCESSOR_VERSION

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
        super().validate(
            config, node_id=node_id, inputs=inputs, sources=sources,
            outputs=outputs, output_grid=output_grid,
        )
        if not isinstance(config, S2CompositeNodeConfig):
            raise TypeError(f"{node_id} has an invalid composite configuration")
        self.single_input(inputs)

    def execute_raster(
        self,
        config: ProcessorConfig,
        inputs: Mapping[str, Any],
        context: ProcessorContext,
    ) -> Any:
        """Build lazy aggregate reductions without loading the source cube."""
        if not isinstance(config, S2CompositeNodeConfig):
            raise TypeError("Invalid composite configuration")

        try:
            import xarray as xr
        except ImportError as error:
            raise RuntimeError("Composite processing requires xarray") from error

        source = self.single_input(inputs)
        from sentinel_py.pipeline.artifacts import RasterArtifact
        if isinstance(source, RasterArtifact):
            return source.map_datasets(
                lambda dataset, unit: self.execute_raster(
                    config, {"data": dataset}, context
                )
            )
        if not isinstance(source, xr.Dataset):
            raise TypeError("composite input must be an xarray Dataset")
        variable = select_cube_variable(source, config.variable, owner="composite")
        fill = config.empty_bins == "fill"
        if source.sizes.get("time", 0) == 0 and not fill:
            raise ValueError(
                "composite input contains no time steps; set empty_bins: fill to "
                "write nodata composites for areas without data"
            )

        selection = context.pipeline.selection
        boundaries = _season_boundaries(
            selection.start_period,
            selection.end_period,
            config.period,
        )
        groups = _seasonal_groups(source.time.values, boundaries, keep_empty=fill)
        if not groups:
            raise ValueError("composite input contains no observations in the season")
        composite_name = _composite_name(source, selection, config.aggregation)

        nodata = source[variable].attrs.get("nodata")
        values = mask_nodata(source[variable], nodata)
        reductions = []
        bin_labels = []
        source_fingerprints = []
        for bin_index, members in groups:
            if members:
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
            else:
                composite = _nodata_composite(values)
            if nodata is not None:
                composite = composite.fillna(nodata).astype(np.float32)
            reductions.append(
                composite.expand_dims(
                    time=[boundaries[bin_index].astype("datetime64[ns]")]
                )
            )
            bin_labels.append(
                f"bin_{bin_index:04d}_{str(boundaries[bin_index])[:10]}"
            )
            source_fingerprints.append(
                deterministic_cache_key(
                    {
                        "source_fingerprints": [
                            str(value)
                            for value in source.source_fingerprint.values[list(members)]
                        ]
                        if members
                        else []
                    }
                )
            )

        chunking = {"time": -1, "band": -1}
        output_grid = getattr(context.pipeline, "output_grid", None)
        if output_grid is not None and output_grid.chunks is not None:
            chunking.update(y=output_grid.chunks.y, x=output_grid.chunks.x)
        result = (
            xr.concat(reductions, dim="time")
            .to_dataset(name=variable)
            .chunk(chunking)
        )
        result = result.assign_coords(
            product_id=("time", [composite_name] * len(groups)),
            granule_id=("time", bin_labels),
            source_fingerprint=("time", source_fingerprints),
        )
        result[variable].attrs.update(source[variable].attrs)
        result[variable].attrs["nodata"] = nodata
        result.attrs.update(source.attrs)
        result.attrs.update(
            processor=self.type_name,
            composite_name=composite_name,
            aggregation=config.aggregation,
            period=config.period,
            time_representation="composite",
            recipe_id=make_recipe_id(
                self.type_name,
                self.version,
                period=config.period,
                aggregation=config.aggregation,
                empty_bins=config.empty_bins,
                source_recipe=source.attrs.get("recipe_id"),
                # Added only for non-default variables so Sentinel-2 recipe IDs
                # (and the COG resume state keyed by them) stay unchanged.
                **({} if variable == "reflectance" else {"variable": variable}),
            ),
        )
        context.logger.info(
            "Built composite node=%s bins=%d aggregation=%s period=%s",
            context.node.node_id,
            len(groups),
            config.aggregation,
            config.period,
        )
        return result

    def execute_points(
        self,
        config: ProcessorConfig,
        inputs: Mapping[str, Any],
        context: ProcessorContext,
    ) -> Any:
        """Aggregate point observations into seasonal bins without raster tasks.

        Observations are binned by month and day, so every selected year falls into
        the same seasonal slots (a climatology). Missing values never enter an
        aggregate; with ``empty_bins: fill`` every planned point gets a row for
        every bin, nodata where it has no valid observation.
        """
        import pandas as pd

        from sentinel_py.pipeline.artifacts import PointSampleArtifact

        if not isinstance(config, S2CompositeNodeConfig) or len(inputs) != 1:
            raise TypeError("Invalid composite configuration or inputs")
        source = next(iter(inputs.values()))
        if not isinstance(source, PointSampleArtifact):
            raise TypeError("composite point input must be a PointSampleArtifact")
        selection = context.pipeline.selection
        boundaries = _season_boundaries(
            selection.start_period, selection.end_period, config.period
        )
        bins = _point_bins(boundaries, config.period)
        by_index = {point_bin.index: point_bin for point_bin in bins}

        rows = source.rows.copy()
        metadata = POINT_METADATA_COLUMNS | S1_POINT_ATTRIBUTE_COLUMNS
        value_columns = [name for name in rows.columns if name not in metadata]
        group_columns = ["aoi_id", "component_id", "point_index", "longitude", "latitude"]
        # S2 source rows may carry 0 (no-data) or 65535 (saturated) from older caches;
        # neither may participate in a point composite.
        for column in value_columns:
            if column in S1_POINT_POLARIZATIONS:
                continue  # floating-point backscatter already uses NaN nodata
            rows[column] = rows[column].replace((0, S2_NODATA), np.nan)

        if config.period == "all":
            rows["bin"] = 0
        elif len(rows):
            rows["bin"] = _point_bin_indices(rows["time"], boundaries)
            rows = rows[(rows["bin"] >= 0) & (rows["bin"] < len(bins))]
        else:
            rows["bin"] = pd.Series(dtype="int64")

        function = {"mean": "mean", "median": "median", "maximum": "max"}[config.aggregation]
        if rows.empty:
            aggregate = pd.DataFrame(columns=[*group_columns, "bin", *value_columns])
        else:
            grouped = rows.groupby([*group_columns, "bin"], dropna=False, sort=False)
            aggregate = getattr(grouped[value_columns], function)().reset_index()
            if config.period != "all":
                aggregate["source_fingerprint"] = grouped["source_fingerprint"].agg(
                    lambda values: deterministic_cache_key(
                        {"source_fingerprints": sorted(str(value) for value in values)}
                    )
                ).to_numpy()
        aggregate["time"] = aggregate["bin"].map({b.index: b.time for b in bins})
        aggregate["granule_id"] = aggregate["bin"].map(
            {index: point_bin.granule_id for index, point_bin in by_index.items()}
        )
        aggregate["product_id"] = "composite"
        aggregate["time_representation"] = "composite"
        if "source_fingerprint" not in aggregate:
            aggregate["source_fingerprint"] = "composite"
        if config.empty_bins == "fill":
            aggregate = _fill_missing_points(aggregate, source.plan, value_columns, bins)
        aggregate = aggregate[
            [
                *group_columns, *value_columns, "time", "product_id", "granule_id",
                "time_representation", "source_fingerprint", "bin",
            ]
        ]
        if config.period != "all":
            aggregate = aggregate.sort_values(
                ["aoi_id", "component_id", "bin"], kind="stable"
            ).reset_index(drop=True)
        context.logger.info(
            "Built point composite node=%s rows=%d bins=%d aggregation=%s period=%s",
            context.node.node_id, len(aggregate), len(bins), config.aggregation,
            config.period,
        )
        return PointSampleArtifact(aggregate.drop(columns="bin"), source.plan)
