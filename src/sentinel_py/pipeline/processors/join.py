"""Join the outputs of several nodes into one wide table (points) or cube (polygons).

A pipeline often processes different sources (a Sentinel-2 composite with spectral
indices, ArcticDEM terrain, a Sentinel-1 composite) and wants them in one place. The
``join`` node takes the first of its ``inputs`` as the base, which keeps its identity
and provenance and every one of its rows or time steps, and adds the value columns or
bands of the other inputs.

Static inputs (``time_representation`` ``static``, such as terrain) are matched on the
point or pixel only and repeated for every base time; all other inputs are matched on
the time label too, so seasonal bins of different sources line up. Point AOIs join
Parquet rows; polygon AOIs join each AOI unit's cube into one float32 ``reflectance``
cube whose bands are the inputs' bands in order, with NaN as nodata.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import numpy as np
from pydantic import BaseModel, ConfigDict

from sentinel_py.cache import deterministic_cache_key
from sentinel_py.pipeline.artifacts import PointSampleArtifact, RasterArtifact
from sentinel_py.pipeline.processors.base import (
    BaseProcessor,
    ProcessorConfig,
    ProcessorContext,
    make_recipe_id,
    mask_nodata,
    select_cube_variable,
)
from sentinel_py.pipeline.readers.base import POINT_METADATA_COLUMNS

PROCESSOR_VERSION = 1
POINT_KEY = ["aoi_id", "component_id"]
STATIC = "static"
JOINED_VARIABLE = "reflectance"


########################################################################################
# Node configuration
########################################################################################


class JoinNodeConfig(BaseModel):
    """Validated parameters of the ``join`` node (it has none besides its inputs)."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


########################################################################################
# Point join
########################################################################################


def join_point_rows(base: Any, others: Mapping[str, Any]) -> Any:
    """Left-join the value columns of ``others`` onto the rows of ``base``.

    Parameters
    ----------
    base : pandas.DataFrame
        Rows of the first input; all of them are kept, in order.
    others : Mapping[str, pandas.DataFrame]
        Rows of the remaining inputs keyed by input name.

    Returns
    -------
    pandas.DataFrame
        ``base`` with the value columns of every other input appended. Points with
        no matching row get missing values.

    Raises
    ------
    ValueError
        If an input has several rows for one join key, or a value column name occurs
        in more than one input.
    """
    result = base
    for name, rows in others.items():
        static = bool((rows["time_representation"] == STATIC).all()) and len(rows) > 0
        keys = POINT_KEY if static else [*POINT_KEY, "time"]
        duplicated = rows.duplicated(subset=keys, keep=False)
        if duplicated.any():
            raise ValueError(
                f"join input {name!r} has several rows per {' and '.join(keys)}; "
                "aggregate it first (for example with a composite node)"
            )
        values = [column for column in rows.columns if column not in POINT_METADATA_COLUMNS]
        clash = sorted(set(values) & set(result.columns))
        if clash:
            raise ValueError(
                f"join input {name!r} repeats column(s) already present: "
                + ", ".join(clash)
            )
        result = result.merge(rows[[*keys, *values]], on=keys, how="left", sort=False)
    return result


########################################################################################
# Raster join
########################################################################################


def _is_static(dataset: Any) -> bool:
    """Return whether a cube has no time dimension of its own (for example terrain)."""
    return dataset.attrs.get("time_representation") == STATIC


def _aligned_values(
    name: str, dataset: Any, base: Any, base_values: Any, *, is_base: bool
) -> Any:
    """Return one input's cube as float32 NaN-nodata values on the base grid and time.

    Inputs are matched to the base by grid position, never by x/y coordinate values
    (the terrain cube numbers its pixels), so the grids must be identical.
    """
    variable = select_cube_variable(dataset, None, owner=f"join input {name!r}")
    values = dataset[variable]
    if values.dims != ("time", "band", "y", "x"):
        raise ValueError(
            f"join input {name!r} variable {variable!r} must have dims "
            "(time, band, y, x)"
        )
    values = mask_nodata(values, values.attrs.get("nodata")).astype(np.float32)
    if is_base:
        return values
    same_grid = (
        str(dataset.attrs.get("crs")) == str(base.attrs.get("crs"))
        and np.allclose(dataset.attrs["transform"], base.attrs["transform"])
        and values.sizes["y"] == base_values.sizes["y"]
        and values.sizes["x"] == base_values.sizes["x"]
    )
    if not same_grid:
        raise ValueError(
            f"join input {name!r} is on a different grid than the first input; "
            "all inputs must share the output grid"
        )
    values = values.assign_coords(y=base_values.y, x=base_values.x)
    steps = base_values.sizes["time"]
    if _is_static(dataset):
        if values.sizes["time"] != 1:
            raise ValueError(f"static join input {name!r} must have one time step")
        return values.isel(time=np.zeros(steps, dtype=int)).assign_coords(
            time=base_values.time
        )
    return values.reindex(time=base_values.time)


def _time_fingerprints(dataset: Any, base: Any, *, static: bool) -> list[str]:
    """Return an input's source fingerprint for each base time step ('' if absent)."""
    if "source_fingerprint" not in dataset.coords:
        return [""] * base.sizes["time"]
    fingerprints = [str(value) for value in dataset.source_fingerprint.values]
    if static:
        return [fingerprints[0]] * base.sizes["time"]
    by_time = dict(zip(dataset.time.values.tolist(), fingerprints))
    return [by_time.get(value, "") for value in base.time.values.tolist()]


def join_cubes(base: Any, others: Mapping[str, Any], *, base_name: str = "base") -> Any:
    """Join the cubes of one AOI unit into a single float32 ``reflectance`` cube.

    Parameters
    ----------
    base : xarray.Dataset
        The first input; it fixes the grid, time axis and provenance.
    others : Mapping[str, xarray.Dataset]
        The remaining inputs keyed by input name.
    base_name : str, optional
        Name of the base input, used in messages and the recipe identifier.

    Returns
    -------
    xarray.Dataset
        A lazy cube with the inputs' bands concatenated in order (base first), NaN
        nodata, and ``recipe_id`` and per-time ``source_fingerprint`` covering every
        input so that changing any of them invalidates the written tiles.

    Raises
    ------
    ValueError
        If the grids differ, band names repeat, or a static input comes first while
        another input varies in time.
    """
    import xarray as xr

    if _is_static(base) and any(not _is_static(item) for item in others.values()):
        raise ValueError(
            "join: put the input that varies in time (for example the composite) "
            "first; the first input sets the time axis"
        )
    base_values = _aligned_values(base_name, base, base, None, is_base=True)
    parts = [base_values]
    for name, dataset in others.items():
        parts.append(_aligned_values(name, dataset, base, base_values, is_base=False))
    bands = [str(band) for part in parts for band in part.band.values]
    repeated = sorted({band for band in bands if bands.count(band) > 1})
    if repeated:
        raise ValueError(
            "join inputs repeat band name(s): " + ", ".join(repeated)
        )
    chunks = {
        "time": -1,
        "band": -1,
        "y": base_values.chunksizes.get("y", -1),
        "x": base_values.chunksizes.get("x", -1),
    }
    joined = xr.concat(parts, dim="band").chunk(chunks).to_dataset(name=JOINED_VARIABLE)
    joined[JOINED_VARIABLE].attrs.update(nodata=np.nan, long_name="joined cube")

    inputs = {base_name: base, **others}
    fingerprints = [
        _time_fingerprints(dataset, base, static=_is_static(dataset))
        for dataset in inputs.values()
    ]
    joined = joined.assign_coords(
        product_id=("time", [str(value) for value in base.product_id.values]),
        granule_id=("time", [str(value) for value in base.granule_id.values]),
        source_fingerprint=(
            "time",
            [
                deterministic_cache_key({"sources": list(step)})
                for step in zip(*fingerprints)
            ],
        ),
    )
    joined.attrs.update(
        crs=base.attrs["crs"],
        transform=base.attrs["transform"],
        nodata=np.nan,
        processor="join",
        time_representation=base.attrs.get("time_representation"),
        recipe_id=make_recipe_id(
            "join",
            PROCESSOR_VERSION,
            inputs=[[name, item.attrs.get("recipe_id")] for name, item in inputs.items()],
            bands=bands,
        ),
    )
    if "bounds" in base.attrs:
        joined.attrs["bounds"] = base.attrs["bounds"]
    return joined


########################################################################################
# Processor class for join
########################################################################################


class JoinProcessor(BaseProcessor):
    """Merge the outputs of several nodes into one wide table or cube."""

    type_name = "join"
    config_model = JoinNodeConfig
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
        """Require at least two inputs and unpacked outputs, besides generic checks."""
        super().validate(
            config, node_id=node_id, inputs=inputs, sources=sources,
            outputs=outputs, output_grid=output_grid,
        )
        if not isinstance(config, JoinNodeConfig):
            raise TypeError(f"{node_id} has an invalid join configuration")
        if len(inputs) < 2:
            raise ValueError("join requires at least two upstream inputs")
        for output in outputs:
            if getattr(output, "encoding", None) is not None:
                raise ValueError(
                    f"output {output.output_id!r} cannot set encoding: a joined cube "
                    "mixes bands of different units, which one scale and offset "
                    "cannot pack"
                )

    def execute_raster(
        self,
        config: ProcessorConfig,
        inputs: Mapping[str, Any],
        context: ProcessorContext,
    ) -> Any:
        """Join every AOI unit's cubes into one multi-band cube."""
        if not isinstance(config, JoinNodeConfig) or len(inputs) < 2:
            raise TypeError("Invalid join configuration or inputs")
        artifacts = list(inputs.items())
        for name, artifact in artifacts:
            if not isinstance(artifact, RasterArtifact):
                raise TypeError(f"join input {name!r} must be a RasterArtifact")
        (base_name, base), *others = artifacts
        for name, artifact in others:
            if set(artifact.datasets) != set(base.datasets):
                raise ValueError(
                    f"join input {name!r} covers different AOI units than {base_name!r}"
                )
        datasets = {
            key: join_cubes(
                dataset,
                {name: artifact.datasets[key] for name, artifact in others},
                base_name=base_name,
            )
            for key, dataset in base.datasets.items()
        }
        context.logger.info(
            "Joined node=%s base=%s inputs=%s AOI units=%d",
            context.node.node_id, base_name, ",".join(name for name, _ in others),
            len(datasets),
        )
        return RasterArtifact(base.plan, datasets)

    def execute_points(
        self,
        config: ProcessorConfig,
        inputs: Mapping[str, Any],
        context: ProcessorContext,
    ) -> Any:
        """Join the rows of every input onto the rows of the first one."""
        if not isinstance(config, JoinNodeConfig) or len(inputs) < 2:
            raise TypeError("Invalid join configuration or inputs")
        artifacts = list(inputs.items())
        for name, artifact in artifacts:
            if not isinstance(artifact, PointSampleArtifact):
                raise TypeError(f"join input {name!r} must be a PointSampleArtifact")
        (base_name, base), *others = artifacts
        rows = join_point_rows(base.rows, {name: item.rows for name, item in others})
        context.logger.info(
            "Joined node=%s base=%s inputs=%s rows=%d columns=%d",
            context.node.node_id, base_name, ",".join(name for name, _ in others),
            len(rows), len(rows.columns),
        )
        return PointSampleArtifact(rows, base.plan)
