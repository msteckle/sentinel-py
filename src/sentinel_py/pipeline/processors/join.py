"""Join the point samples of several nodes into one wide table.

A point pipeline often samples different sources (a Sentinel-2 composite with spectral
indices, ArcticDEM terrain, a Sentinel-1 composite) and wants one row per point. The
``join`` node takes the first of its ``inputs`` as the base, which keeps its identity
and provenance columns and every one of its rows, and adds the value columns of the
other inputs. Static inputs (``time_representation`` ``static``, such as terrain) are
joined on the point; all other inputs are joined on the point and the ``time`` label,
so seasonal bins of different sources line up.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel, ConfigDict

from sentinel_py.enums import ArtifactKind
from sentinel_py.pipeline.artifacts import PointSampleArtifact
from sentinel_py.pipeline.processors.base import (
    BaseProcessor,
    ProcessorConfig,
    ProcessorContext,
)
from sentinel_py.pipeline.readers.base import POINT_METADATA_COLUMNS

PROCESSOR_VERSION = 1
POINT_KEY = ["aoi_id", "component_id"]
STATIC = "static"


########################################################################################
# Node configuration
########################################################################################


class JoinNodeConfig(BaseModel):
    """Validated parameters of the ``join`` node (it has none besides its inputs)."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


########################################################################################
# Join
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
# Processor class for join
########################################################################################


class JoinProcessor(BaseProcessor):
    """Merge the point samples of several nodes into one wide point table."""

    type_name = "join"
    config_model = JoinNodeConfig
    output_kind = ArtifactKind.point
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
        """Require at least two upstream inputs besides the generic checks."""
        super().validate(
            config, node_id=node_id, inputs=inputs, sources=sources,
            outputs=outputs, output_grid=output_grid,
        )
        if not isinstance(config, JoinNodeConfig):
            raise TypeError(f"{node_id} has an invalid join configuration")
        if len(inputs) < 2:
            raise ValueError("join requires at least two upstream inputs")

    def execute_raster(
        self,
        config: ProcessorConfig,
        inputs: Mapping[str, Any],
        context: ProcessorContext,
    ) -> Any:
        """Reject polygon AOIs, whose nodes each write their own raster output."""
        raise ValueError(
            "join combines point samples; with polygon AOIs write one output per node"
        )

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
