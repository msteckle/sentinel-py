"""Resumable atomic Cloud Optimized GeoTIFF tile output.

Every data variable shaped ``(time, band, y, x)`` or ``(time, y, x)`` is written as
chunk-level tiles named ``<variable>.r<row>.c<column>.tif``; integer variables
(Sentinel-2 reflectance DN, SCL) and floating-point variables (Sentinel-1
backscatter, with NaN nodata) share one resumable code path. A float variable can
also be packed to ``int16``/``uint16`` through the output's ``encoding`` (GDAL scale
and offset tags), which roughly halves its size with a bounded quantization error.
"""

from __future__ import annotations

import logging
import os
import uuid
from dataclasses import replace
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np

import sentinel_py.pipeline.grid as pipeline_grid
from sentinel_py.cache import REGISTRY, RowStore, deterministic_cache_key
from sentinel_py.enums import AOIKind, ArtifactKind, OutputFormat, WriteStatus
from sentinel_py.pipeline.writers.base import BaseOutputWriter, OutputWritePlan
from sentinel_py.pipeline.writers.vrt import (
    VRTSpec,
    VRTTile,
    write_output_mosaics,
    write_unit_vrts,
)

COG_WRITER_VERSION = 1
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class COGWriteResult:
    """Serializable result returned by one COG worker task."""

    task_id: str
    output_id: str
    product_id: str
    granule_id: str
    variable: str
    row_index: int
    column_index: int
    output_path: Path
    cache_key: str
    status: str
    bytes_written: int
    error: str | None
    completed_at: str

    @property
    def state_row(self) -> dict[str, Any]:
        """Return the JSON/Parquet-friendly resume-state row for this result."""
        value = asdict(self)
        value["output_path"] = str(self.output_path)
        return value


def cog_state_path(output_dir: Path) -> Path:
    """Return the per-output-directory COG resume-state location."""
    return REGISTRY["cog_outputs"].path(output_dir)


def pack_values(
    values: np.ndarray, dtype: str, scale: float, offset: float
) -> tuple[np.ndarray, int]:
    """Quantize floating-point values to integers with a reserved nodata code.

    Parameters
    ----------
    values : numpy.ndarray
        Floating-point values; NaN and infinities become nodata.
    dtype : str
        ``"int16"`` or ``"uint16"``.
    scale, offset : float
        Stored integer ``q`` represents ``q * scale + offset``.

    Returns
    -------
    tuple[numpy.ndarray, int]
        The packed array and its nodata code (the most negative ``int16`` value or
        ``65535`` for ``uint16``). Out-of-range values are clipped to the nearest
        representable code.
    """
    info = np.iinfo(dtype)
    signed = info.min < 0
    sentinel = int(info.min if signed else info.max)
    low, high = (info.min + 1, info.max) if signed else (0, info.max - 1)
    finite = np.isfinite(values)
    scaled = np.rint((np.where(finite, values, 0.0).astype("float64") - offset) / scale)
    packed = np.clip(scaled, low, high).astype(dtype)
    return np.where(finite, packed, sentinel).astype(dtype), sentinel


def _write_cog_tile(
    values: np.ndarray,
    *,
    task_id: str,
    output_id: str,
    product_id: str,
    granule_id: str,
    variable: str,
    row_index: int,
    column_index: int,
    output_path: Path,
    cache_key: str,
    crs: str,
    transform: tuple[float, float, float, float, float, float],
    nodata: float,
    descriptions: tuple[str, ...],
    encoding: tuple[str, float, float] | None = None,
) -> COGWriteResult:
    """Write one array chunk atomically and return data instead of worker logs."""
    temporary = output_path.with_name(f".{output_path.name}.{uuid.uuid4().hex}.tmp.tif")
    identity = {
        "task_id": task_id, "output_id": output_id, "product_id": product_id,
        "granule_id": granule_id, "variable": variable, "row_index": row_index,
        "column_index": column_index, "output_path": output_path,
        "cache_key": cache_key,
    }
    try:
        import rasterio
        from affine import Affine

        output_path.parent.mkdir(parents=True, exist_ok=True)
        array = np.asarray(values)
        if encoding is not None:
            array, nodata = pack_values(array, *encoding)
        elif np.issubdtype(array.dtype, np.floating):
            array = np.where(np.isfinite(array), array, nodata).astype(array.dtype)
        if array.ndim == 2:
            array = array[np.newaxis, ...]
        with (
            pipeline_grid.RASTERIO_LOCK,
            rasterio.open(
                temporary,
                "w",
                driver="COG",
                width=array.shape[2],
                height=array.shape[1],
                count=array.shape[0],
                dtype=array.dtype,
                crs=crs,
                transform=Affine.from_gdal(*transform),
                nodata=nodata,
                compress="DEFLATE",
                # Predictor 3 is the floating-point predictor (about 12% smaller).
                predictor=3 if np.issubdtype(array.dtype, np.floating) else 2,
                blocksize=256,
                overview_resampling="nearest",
                num_threads="1",
            ) as destination,
        ):
            destination.write(array)
            destination.descriptions = descriptions
            if encoding is not None:
                destination.scales = (float(encoding[1]),) * array.shape[0]
                destination.offsets = (float(encoding[2]),) * array.shape[0]
        os.replace(temporary, output_path)
        return COGWriteResult(
            **identity,
            status=WriteStatus.written,
            bytes_written=output_path.stat().st_size,
            error=None,
            completed_at=datetime.now(UTC).isoformat(),
        )
    except Exception as error:
        return COGWriteResult(
            **identity,
            status=WriteStatus.failed,
            bytes_written=0,
            error=f"{type(error).__name__}: {error}",
            completed_at=datetime.now(UTC).isoformat(),
        )
    finally:
        temporary.unlink(missing_ok=True)


def _nodata_value(name: str, attrs: dict[str, Any]) -> int | float:
    """Return a variable's nodata as an ``int``, or NaN for floating-point data.

    Parameters
    ----------
    name : str
        Variable name, used in the error message.
    attrs : dict[str, Any]
        Variable attributes carrying ``nodata``.

    Returns
    -------
    int | float
        The integer sentinel (e.g. 65535) or ``nan``.

    Raises
    ------
    ValueError
        If the variable has no ``nodata`` attribute.
    """
    if "nodata" not in attrs:
        raise ValueError(f"COG variable {name!r} needs a 'nodata' attribute")
    value = float(attrs["nodata"])
    return value if np.isnan(value) else int(value)


def _tile_variables(artifact: Any) -> list[tuple[str, int | float, tuple[str, ...]]]:
    """List the dataset variables written as COG tiles.

    Parameters
    ----------
    artifact : xarray.Dataset
        Dataset whose ``(time, band, y, x)`` and ``(time, y, x)`` variables are
        tiled. Banded variables are listed first, then dataset order is kept.

    Returns
    -------
    list[tuple[str, int | float, tuple[str, ...]]]
        ``(variable name, nodata value, band descriptions)`` per tiled variable.
        Banded variables describe their bands by the ``band`` coordinate; unbanded
        variables use their upper-cased name (``scl`` becomes ``SCL``).
    """
    banded, unbanded = [], []
    for name, data in artifact.data_vars.items():
        if data.dims == ("time", "band", "y", "x"):
            target = banded
            descriptions = tuple(str(value) for value in artifact.band.values)
        elif data.dims == ("time", "y", "x"):
            target = unbanded
            descriptions = (str(name).upper(),)
        else:
            continue
        nodata = _nodata_value(str(name), data.attrs)
        target.append((str(name), nodata, descriptions))
    return banded + unbanded


def _failed_paths(results: Any) -> set[Path]:
    """Return the tile paths whose write failed."""
    return {
        result.output_path for result in results if result.status == WriteStatus.failed
    }


def _tile_encoding(output: Any, dtype: np.dtype) -> tuple[str, float, float] | None:
    """Return the ``(dtype, scale, offset)`` packing of a variable, if any.

    Only floating-point variables are packed; integer data (DN, SCL) is kept.
    """
    requested = getattr(output, "encoding", None)
    if requested is None or not np.issubdtype(dtype, np.floating):
        return None
    return requested.dtype, requested.scale, requested.offset


def _vrt_spec(
    artifact: Any,
    output: Any,
    time_index: int,
    variable: tuple[str, int | float, tuple[str, ...]],
    dtype: np.dtype,
    tiles: list[VRTTile],
) -> VRTSpec:
    """Describe the tiles of one (time bin, variable) for its VRT."""
    name, nodata, descriptions = variable
    transform = artifact.attrs["transform"]
    encoding = _tile_encoding(output, dtype)
    if encoding is None:
        stored, stored_nodata = np.dtype(dtype).name, float(nodata)
        scale = offset = None
    else:
        # Packed tiles store int codes with a reserved nodata code (see pack_values).
        info = np.iinfo(encoding[0])
        stored = encoding[0]
        stored_nodata = float(info.min if info.min < 0 else info.max)
        scale, offset = float(encoding[1]), float(encoding[2])
    return VRTSpec(
        product_id=str(artifact.product_id.values[time_index]),
        granule_id=str(artifact.granule_id.values[time_index]),
        variable=name,
        crs=str(artifact.attrs["crs"]),
        resolution=(float(transform[0]), abs(float(transform[4]))),
        dtype=stored,
        nodata=stored_nodata,
        descriptions=descriptions,
        scale=scale,
        offset=offset,
        tiles=tuple(tiles),
    )


class COGOutputWriter(BaseOutputWriter):
    """Build chunk-level COG tasks and maintain output-scoped resume state."""

    format_name = OutputFormat.cog
    accepted_artifact_kinds = frozenset({ArtifactKind.raster})

    def build(self, artifact: Any, output: Any, pipeline: Any) -> OutputWritePlan:
        """Plan chunk-level COG write tasks, skipping tiles already completed.

        Parameters
        ----------
        artifact : RasterArtifact | xarray.Dataset
            Lazy per-AOI datasets to materialize, or one dataset (used for each AOI
            component).
        output : OutputConfig
            Named output supplying the destination directory.
        pipeline : PipelineConfig
            Pipeline configuration used for naming and grid context.

        Returns
        -------
        OutputWritePlan
            Delayed tile tasks plus results reused from the resume state.
        """
        try:
            import xarray as xr
        except ImportError as error:
            raise RuntimeError("COG output requires xarray and Dask") from error
        from sentinel_py.pipeline.artifacts import RasterArtifact

        if isinstance(artifact, RasterArtifact):
            return self._build_artifact(artifact, output, pipeline)
        if not isinstance(artifact, xr.Dataset):
            raise TypeError("COG output requires an xarray Dataset artifact")
        if not _tile_variables(artifact):
            raise ValueError(
                "COG output requires a (time, band, y, x) or (time, y, x) variable"
            )
        return self._build_dataset(artifact, output)

    def _build_artifact(
        self, artifact: Any, output: Any, pipeline: Any
    ) -> OutputWritePlan:
        """Plan every AOI component of a raster artifact under its own directory."""
        if artifact.plan.kind != AOIKind.polygon:
            raise ValueError("COG output requires polygon AOIs")
        subplans = []
        for key, dataset in artifact.datasets.items():
            unit = artifact.plan.by_key[key]
            scoped_output = replace(
                output, path=output.path / unit.aoi_id / unit.component_id
            )
            subplans.append(self.build(dataset, scoped_output, pipeline))
        return OutputWritePlan(
            output,
            tuple(task for plan in subplans for task in plan.tasks),
            tuple(result for plan in subplans for result in plan.completed),
            {"subplans": tuple(subplans)},
        )

    def _build_dataset(self, artifact: Any, output: Any) -> OutputWritePlan:
        """Plan the tile tasks of one dataset, reusing completed tiles from state."""
        state_path = cog_state_path(output.path)
        state_store = RowStore(state_path, ["task_id"])
        state = state_store.read()
        cached = {
            str(row["task_id"]): row
            for row in state.to_dict("records")
            if row.get("status") in {WriteStatus.written, WriteStatus.skipped}
        }
        variables = _tile_variables(artifact)
        # One band chunk per tile; rechunking once keeps graph building O(variables).
        arrays = {
            name: (
                artifact[name].chunk({"band": -1})
                if "band" in artifact[name].dims
                else artifact[name]
            )
            for name, _, _ in variables
        }
        tasks: list[Any] = []
        completed: list[COGWriteResult] = []
        vrt_specs: list[VRTSpec] = []
        for time_index in range(artifact.sizes["time"]):
            for variable in variables:
                tiles: list[VRTTile] = []
                self._plan_variable_tiles(
                    artifact,
                    arrays,
                    output,
                    time_index,
                    variable,
                    cached,
                    tasks,
                    completed,
                    tiles,
                )
                vrt_specs.append(
                    _vrt_spec(
                        artifact, output, time_index, variable,
                        arrays[variable[0]].dtype, tiles,
                    )
                )
        return OutputWritePlan(
            output,
            tuple(tasks),
            tuple(completed),
            {
                "state_path": state_path,
                "state": state,
                "store": state_store,
                "vrt": tuple(vrt_specs),
                # Task ids already persisted by checkpoint(), so finalize() does
                # not write them twice.
                "checkpointed": set(),
            },
        )

    @staticmethod
    def _plan_variable_tiles(
        artifact: Any,
        arrays: dict[str, Any],
        output: Any,
        time_index: int,
        variable: tuple[str, int | float, tuple[str, ...]],
        cached: dict[str, Any],
        tasks: list[Any],
        completed: list[COGWriteResult],
        tiles: list[VRTTile],
    ) -> None:
        """Append one task (or one skipped result) per chunk of one time step.

        Parameters
        ----------
        artifact : xarray.Dataset
            Dataset carrying ``crs``, ``transform`` and ``recipe_id`` attributes.
        arrays : dict[str, xarray.DataArray]
            Tiled variables by name, banded ones rechunked to one chunk along
            ``band``.
        output : OutputConfig
            Output supplying ``output_id`` and the destination ``path``.
        time_index : int
            Position along ``time``.
        variable : tuple[str, int, tuple[str, ...]]
            ``(name, nodata, band descriptions)`` from :func:`_tile_variables`.
        cached : dict[str, Any]
            Completed state rows by task id.
        tasks, completed : list
            Output lists extended in place.
        tiles : list[VRTTile]
            Extended in place with the position of every tile, for the VRT.
        """
        from dask import delayed

        name, nodata, descriptions = variable
        transform = tuple(float(value) for value in artifact.attrs["transform"])
        x_resolution = transform[0]
        y_resolution = abs(transform[4])
        recipe_id = str(artifact.attrs["recipe_id"])
        product_id = str(artifact.product_id.values[time_index])
        granule_id = str(artifact.granule_id.values[time_index])
        source_fingerprint = str(artifact.source_fingerprint.values[time_index])

        data_array = arrays[name]
        has_band = "band" in data_array.dims
        encoding = _tile_encoding(output, data_array.dtype)
        array = data_array.data[time_index]
        delayed_blocks = array.to_delayed(optimize_graph=False)
        row_offset = 0
        for row_index, height in enumerate(array.chunks[-2]):
            column_offset = 0
            for column_index, width in enumerate(array.chunks[-1]):
                block = (
                    delayed_blocks[0, row_index, column_index]
                    if has_band
                    else delayed_blocks[row_index, column_index]
                )
                relative = (
                    Path(product_id)
                    / granule_id
                    / f"{name}.r{row_index:04d}.c{column_index:04d}.tif"
                )
                output_path = output.path / relative
                tiles.append(
                    VRTTile(
                        output_path,
                        transform[2] + column_offset * x_resolution,
                        transform[5] - row_offset * y_resolution,
                        width,
                        height,
                    )
                )
                key_payload = {
                    "writer_version": COG_WRITER_VERSION,
                    "output_id": output.output_id,
                    "recipe_id": recipe_id,
                    "source_fingerprint": source_fingerprint,
                    "variable": name,
                    "row": row_index,
                    "column": column_index,
                    "shape": (height, width),
                }
                if encoding is not None:
                    # Added only when packing so existing resume keys stay valid.
                    key_payload["encoding"] = list(encoding)
                cache_key = deterministic_cache_key(key_payload)
                task_id = deterministic_cache_key(
                    {"output_id": output.output_id, "relative_path": str(relative)}
                )
                identity = {
                    "task_id": task_id,
                    "output_id": output.output_id,
                    "product_id": product_id,
                    "granule_id": granule_id,
                    "variable": name,
                    "row_index": row_index,
                    "column_index": column_index,
                    "output_path": output_path,
                    "cache_key": cache_key,
                }
                cached_row = cached.get(task_id)
                if (
                    cached_row is not None
                    and cached_row.get("cache_key") == cache_key
                    and output_path.is_file()
                ):
                    completed.append(
                        COGWriteResult(
                            **identity,
                            status=WriteStatus.skipped,
                            bytes_written=output_path.stat().st_size,
                            error=None,
                            completed_at=datetime.now(UTC).isoformat(),
                        )
                    )
                else:
                    tasks.append(
                        delayed(_write_cog_tile, pure=False)(
                            block,
                            **identity,
                            crs=str(artifact.attrs["crs"]),
                            transform=(
                                transform[2] + column_offset * x_resolution,
                                transform[0],
                                transform[1],
                                transform[5] - row_offset * y_resolution,
                                transform[3],
                                transform[4],
                            ),
                            nodata=nodata,
                            descriptions=descriptions,
                            encoding=encoding,
                        )
                    )
                column_offset += width
            row_offset += height

    def checkpoint(
        self,
        plan: OutputWritePlan,
        computed: tuple[Any, ...],
        offset: int = 0,
    ) -> None:
        """Persist resume state for one computed batch of this output's tasks.

        The pipeline calls this after every materialization batch, so an
        interrupted run keeps the state of all tiles written before the interrupt.

        Parameters
        ----------
        plan : OutputWritePlan
            Plan returned by :meth:`build`.
        computed : tuple
            Results of a contiguous run of this output's tasks.
        offset : int, default 0
            Index of ``computed[0]`` within the output's task list.
        """
        if "subplans" in plan.metadata:
            position = 0
            for subplan in plan.metadata["subplans"]:
                low, high = position, position + len(subplan.tasks)
                start = max(low, offset)
                end = min(high, offset + len(computed))
                if start < end:
                    self.checkpoint(
                        subplan, computed[start - offset:end - offset], start - low
                    )
                position = high
            return
        checkpointed: set[str] = plan.metadata["checkpointed"]
        rows = [
            result.state_row for result in computed if result.task_id not in checkpointed
        ]
        if rows:
            plan.metadata["store"].append(rows)
            checkpointed.update(row["task_id"] for row in rows)

    def finalize(
        self,
        plan: OutputWritePlan,
        computed: tuple[Any, ...],
    ) -> tuple[COGWriteResult, ...]:
        """Persist resume state, write the VRTs, and return all tile results.

        A leaf plan (one AOI unit) writes a VRT per time bin and variable beside its
        tiles; the parent plan of a polygon output then writes one mosaic VRT per time
        bin and variable over every unit under the output directory.
        """
        if "subplans" in plan.metadata:
            results = []
            offset = 0
            for subplan in plan.metadata["subplans"]:
                count = len(subplan.tasks)
                results.extend(self.finalize(subplan, computed[offset:offset + count]))
                offset += count
            unit_specs = [
                subplan.metadata["vrt"]
                for subplan in plan.metadata["subplans"]
                if subplan.metadata.get("vrt")
            ]
            if unit_specs:
                try:
                    mosaics = write_output_mosaics(
                        unit_specs, plan.output.path, _failed_paths(results)
                    )
                except OSError as error:
                    logger.warning("Could not write the output VRT mosaics: %s", error)
                else:
                    logger.info(
                        "Wrote %d mosaic VRT(s) under %s", len(mosaics), plan.output.path
                    )
            return tuple(results)
        results = tuple(plan.completed) + tuple(computed)
        checkpointed: set[str] = plan.metadata["checkpointed"]
        store: RowStore = plan.metadata["store"]
        store.append(
            [
                result.state_row
                for result in results
                if result.task_id not in checkpointed
            ]
        )
        store.compact()
        if plan.metadata.get("vrt"):
            try:
                write_unit_vrts(
                    plan.metadata["vrt"], plan.output.path, _failed_paths(results)
                )
            except OSError as error:
                logger.warning("Could not write VRTs under %s: %s", plan.output.path, error)
        return results
