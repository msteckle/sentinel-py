"""Resumable atomic Cloud Optimized GeoTIFF tile output."""

from __future__ import annotations

import os
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from sentinel_py import raster_io
from sentinel_py.cache import (
    deterministic_cache_key,
    merge_state_rows,
    write_parquet_atomic,
)
from sentinel_py.pipeline.writers import OutputWritePlan

COG_WRITER_VERSION = 1
COG_STATE_NAME = "cog_outputs.parquet"


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
        value = asdict(self)
        value["output_path"] = str(self.output_path)
        return value


def cog_state_path(output_dir: Path) -> Path:
    return Path(output_dir) / ".sentinel-py" / COG_STATE_NAME


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
    nodata: int,
    descriptions: tuple[str, ...],
) -> COGWriteResult:
    """Write one array chunk atomically and return data instead of worker logs."""
    temporary = output_path.with_name(f".{output_path.name}.{uuid.uuid4().hex}.tmp.tif")
    try:
        import rasterio
        from affine import Affine

        output_path.parent.mkdir(parents=True, exist_ok=True)
        array = np.asarray(values)
        if array.ndim == 2:
            array = array[np.newaxis, ...]
        with (
            raster_io.RASTERIO_LOCK,
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
                predictor=2,
                blocksize=256,
                overview_resampling="nearest",
                num_threads="1",
            ) as destination,
        ):
            destination.write(array)
            destination.descriptions = descriptions
        os.replace(temporary, output_path)
        return COGWriteResult(
            task_id,
            output_id,
            product_id,
            granule_id,
            variable,
            row_index,
            column_index,
            output_path,
            cache_key,
            "written",
            output_path.stat().st_size,
            None,
            datetime.now(UTC).isoformat(),
        )
    except Exception as error:
        return COGWriteResult(
            task_id,
            output_id,
            product_id,
            granule_id,
            variable,
            row_index,
            column_index,
            output_path,
            cache_key,
            "failed",
            0,
            f"{type(error).__name__}: {error}",
            datetime.now(UTC).isoformat(),
        )
    finally:
        temporary.unlink(missing_ok=True)


class COGOutputWriter:
    """Build chunk-level COG tasks and maintain output-scoped resume state."""

    format_name = "cog"

    def build(self, artifact: Any, output: Any, pipeline: Any) -> OutputWritePlan:
        try:
            import xarray as xr
            from dask import delayed
        except ImportError as error:
            raise RuntimeError("COG output requires xarray and Dask") from error
        if not isinstance(artifact, xr.Dataset):
            raise TypeError("COG output requires an xarray Dataset artifact")
        required = {"reflectance", "scl"}
        if not required.issubset(artifact.data_vars):
            raise ValueError("COG output requires reflectance and scl variables")

        state_path = cog_state_path(output.path)
        state = pd.read_parquet(state_path) if state_path.is_file() else pd.DataFrame()
        cached = {
            str(row["task_id"]): row
            for row in state.to_dict("records")
            if row.get("status") in {"written", "skipped"}
        }
        transform = tuple(float(value) for value in artifact.attrs["transform"])
        x_resolution = transform[0]
        y_resolution = abs(transform[4])
        recipe_id = str(artifact.attrs["recipe_id"])
        tasks = []
        completed = []

        for time_index in range(artifact.sizes["time"]):
            product_id = str(artifact.product_id.values[time_index])
            granule_id = str(artifact.granule_id.values[time_index])
            source_fingerprint = str(artifact.source_fingerprint.values[time_index])
            for variable, nodata, descriptions in (
                (
                    "reflectance",
                    int(artifact.reflectance.attrs["nodata"]),
                    tuple(str(value) for value in artifact.band.values),
                ),
                ("scl", int(artifact.scl.attrs["nodata"]), ("SCL",)),
            ):
                array = artifact[variable].data[time_index]
                delayed_blocks = array.to_delayed(optimize_graph=False)
                y_chunks = array.chunks[-2]
                x_chunks = array.chunks[-1]
                row_offset = 0
                for row_index, height in enumerate(y_chunks):
                    column_offset = 0
                    for column_index, width in enumerate(x_chunks):
                        if variable == "reflectance":
                            if len(array.chunks[0]) != 1:
                                raise ValueError(
                                    "COG output requires all bands in one Dask chunk"
                                )
                            block = delayed_blocks[0, row_index, column_index]
                        else:
                            block = delayed_blocks[row_index, column_index]
                        relative = (
                            Path(product_id)
                            / granule_id
                            / f"{variable}.r{row_index:04d}.c{column_index:04d}.tif"
                        )
                        output_path = output.path / relative
                        cache_key = deterministic_cache_key(
                            {
                                "writer_version": COG_WRITER_VERSION,
                                "output_id": output.output_id,
                                "recipe_id": recipe_id,
                                "source_fingerprint": source_fingerprint,
                                "variable": variable,
                                "row": row_index,
                                "column": column_index,
                                "shape": (height, width),
                            }
                        )
                        task_id = deterministic_cache_key(
                            {
                                "output_id": output.output_id,
                                "relative_path": str(relative),
                            }
                        )
                        cached_row = cached.get(task_id)
                        if (
                            cached_row is not None
                            and cached_row.get("cache_key") == cache_key
                            and output_path.is_file()
                        ):
                            completed.append(
                                COGWriteResult(
                                    task_id,
                                    output.output_id,
                                    product_id,
                                    granule_id,
                                    variable,
                                    row_index,
                                    column_index,
                                    output_path,
                                    cache_key,
                                    "skipped",
                                    output_path.stat().st_size,
                                    None,
                                    datetime.now(UTC).isoformat(),
                                )
                            )
                        else:
                            window_transform = (
                                transform[2] + column_offset * x_resolution,
                                transform[0],
                                transform[1],
                                transform[5] - row_offset * y_resolution,
                                transform[3],
                                transform[4],
                            )
                            tasks.append(
                                delayed(_write_cog_tile, pure=False)(
                                    block,
                                    task_id=task_id,
                                    output_id=output.output_id,
                                    product_id=product_id,
                                    granule_id=granule_id,
                                    variable=variable,
                                    row_index=row_index,
                                    column_index=column_index,
                                    output_path=output_path,
                                    cache_key=cache_key,
                                    crs=str(artifact.attrs["crs"]),
                                    transform=window_transform,
                                    nodata=nodata,
                                    descriptions=descriptions,
                                )
                            )
                        column_offset += width
                    row_offset += height
        return OutputWritePlan(
            output,
            tuple(tasks),
            tuple(completed),
            {"state_path": state_path, "state": state},
        )

    def finalize(
        self,
        plan: OutputWritePlan,
        computed: tuple[Any, ...],
    ) -> tuple[COGWriteResult, ...]:
        results = tuple(plan.completed) + tuple(computed)
        state = merge_state_rows(
            plan.metadata["state"],
            (result.state_row for result in results),
            key_columns=["task_id"],
        )
        write_parquet_atomic(state, plan.metadata["state_path"], index=False)
        return results
