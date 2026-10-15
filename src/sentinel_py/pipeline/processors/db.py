"""Decibel conversion of linear-power cubes (for example Sentinel-1 backscatter).

``to_db`` computes ``10 * log10(x)`` for strictly positive values and leaves zero,
negative, and missing values missing. It is element-wise and lazy, so chunking and
the Dask graph of the upstream node are preserved. Convert after compositing: a
mean must be taken in linear power, and a median is unaffected by the order.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import numpy as np
from pydantic import BaseModel, ConfigDict, Field

from sentinel_py.pipeline.artifacts import PointSampleArtifact, RasterArtifact
from sentinel_py.pipeline.processors.base import (
    BaseProcessor,
    ProcessorConfig,
    ProcessorContext,
    make_recipe_id,
    select_cube_variable,
)
from sentinel_py.pipeline.readers.base import S1_POINT_POLARIZATIONS

PROCESSOR_VERSION = 1
DB_SUFFIX = "_db"
UNITS_DB = "dB"


########################################################################################
# Node configuration
########################################################################################


class ToDBNodeConfig(BaseModel):
    """Validated parameters belonging to the ``to_db`` node."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    variable: str | None = Field(default=None, min_length=1)


########################################################################################
# Conversion
########################################################################################


def to_decibels(values: Any) -> Any:
    """Convert linear power to dB, masking values that are not strictly positive.

    Parameters
    ----------
    values : xarray.DataArray | pandas.Series | numpy.ndarray
        Linear power values; Dask-backed arrays stay lazy.

    Returns
    -------
    Any
        ``10 * log10(values)`` as float32 with zero, negative, and NaN input
        returned as NaN (no divide or invalid-value warnings).
    """
    if hasattr(values, "where"):
        positive = values.where(values > 0)
    else:
        array = np.asarray(values, dtype="float64")
        positive = np.where(array > 0, array, np.nan)
    return (10.0 * np.log10(positive)).astype(np.float32)


########################################################################################
# Processor class for to_db
########################################################################################


class ToDBProcessor(BaseProcessor):
    """Convert an upstream linear-power cube or point table to decibels."""

    type_name = "to_db"
    config_model = ToDBNodeConfig
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
        """Validate the single upstream dependency and output formats."""
        super().validate(
            config, node_id=node_id, inputs=inputs, sources=sources,
            outputs=outputs, output_grid=output_grid,
        )
        if not isinstance(config, ToDBNodeConfig):
            raise TypeError(f"{node_id} has an invalid to_db configuration")
        self.single_input(inputs)

    def execute_raster(
        self,
        config: ProcessorConfig,
        inputs: Mapping[str, Any],
        context: ProcessorContext,
    ) -> Any:
        """Return the cube with its linear variable replaced by ``<name>_db``."""
        if not isinstance(config, ToDBNodeConfig):
            raise TypeError("Invalid to_db configuration")
        import xarray as xr

        source = self.single_input(inputs)
        if isinstance(source, RasterArtifact):
            return source.map_datasets(
                lambda dataset, unit: self.execute_raster(
                    config, {"data": dataset}, context
                )
            )
        if not isinstance(source, xr.Dataset):
            raise TypeError("to_db input must be an xarray Dataset")

        variable = select_cube_variable(source, config.variable, owner="to_db")
        data = source[variable]
        if not np.issubdtype(data.dtype, np.floating):
            raise TypeError(
                f"to_db requires floating-point linear power; {variable!r} has dtype "
                f"{data.dtype} (integer digital numbers such as reflectance DN are "
                "not power values)"
            )
        if data.attrs.get("units") == UNITS_DB:
            raise ValueError(f"{variable!r} is already in dB")

        name = f"{variable}{DB_SUFFIX}"
        converted = to_decibels(data)
        converted.attrs = {
            **{k: v for k, v in data.attrs.items() if k != "long_name"},
            "long_name": f"{data.attrs.get('long_name', variable)} (dB)",
            "units": UNITS_DB,
            "nodata": float("nan"),
        }
        result = source.drop_vars(variable)
        result[name] = converted
        result.attrs.update(source.attrs)
        result.attrs.update(
            processor=self.type_name,
            recipe_id=make_recipe_id(
                self.type_name,
                self.version,
                variable=variable,
                source_recipe=source.attrs.get("recipe_id"),
            ),
        )
        context.logger.info(
            "Converted node=%s variable=%s to %s", context.node.node_id, variable, name
        )
        return result

    def execute_points(
        self,
        config: ProcessorConfig,
        inputs: Mapping[str, Any],
        context: ProcessorContext,
    ) -> Any:
        """Convert polarization value columns of point rows to dB in place."""
        if not isinstance(config, ToDBNodeConfig):
            raise TypeError("Invalid to_db configuration")
        source = self.single_input(inputs)
        if not isinstance(source, PointSampleArtifact):
            raise TypeError("to_db point input must be a PointSampleArtifact")
        rows = source.rows.copy()
        columns = [name for name in rows.columns if name in S1_POINT_POLARIZATIONS]
        if not columns:
            raise TypeError(
                "to_db point input has no polarization columns "
                f"({', '.join(sorted(S1_POINT_POLARIZATIONS))})"
            )
        if "units" in rows and (rows["units"] == UNITS_DB).any():
            raise ValueError("point rows are already in dB")
        for column in columns:
            rows[column] = to_decibels(rows[column]).astype("float64")
        rows["units"] = UNITS_DB
        context.logger.info(
            "Converted node=%s point columns %s to dB",
            context.node.node_id, ",".join(columns),
        )
        return PointSampleArtifact(rows, source.plan)
