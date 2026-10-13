"""COG output writer: tile planning, ids, resume state, checkpoints, and tile writes."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

xr = pytest.importorskip("xarray")
da = pytest.importorskip("dask.array")
rasterio = pytest.importorskip("rasterio")

from sentinel_py.cache import RowStore  # noqa: E402
from sentinel_py.enums import WriteStatus  # noqa: E402
from sentinel_py.pipeline.writers.base import OutputWritePlan  # noqa: E402
from sentinel_py.pipeline.writers.cog import (  # noqa: E402
    COGOutputWriter,
    COGWriteResult,
    _write_cog_tile,
)


def _artifact(recipe_id: str = "recipe", with_scl: bool = True):
    reflectance = da.from_array(
        np.arange(2 * 2 * 4 * 4, dtype=np.float32).reshape(2, 2, 4, 4),
        chunks=(1, 2, 2, 2),
    )
    variables = {
        "reflectance": (("time", "band", "y", "x"), reflectance, {"nodata": 65535})
    }
    if with_scl:
        scl = da.from_array(np.full((2, 4, 4), 4, dtype=np.uint8), chunks=(1, 2, 2))
        variables["scl"] = (("time", "y", "x"), scl, {"nodata": 0})
    return xr.Dataset(
        variables,
        coords={
            "time": np.array(["2000-06-01", "2000-06-16"], dtype="datetime64[ns]"),
            "band": ["B02", "B03"],
            "y": [3, 2, 1, 0],
            "x": [0, 1, 2, 3],
            "product_id": ("time", ["prod_a", "prod_b"]),
            "granule_id": ("time", ["gran_a", "gran_b"]),
            "source_fingerprint": ("time", ["fp_a", "fp_b"]),
        },
        attrs={
            "crs": "EPSG:3338",
            "transform": (20.0, 0.0, 1000.0, 0.0, -20.0, 5000.0),
            "recipe_id": recipe_id,
        },
    )


def _compute(plan):
    import dask

    return dask.compute(*plan.tasks, scheduler="single-threaded")


def test_build_plans_one_task_per_time_variable_and_chunk(tmp_path):
    output = SimpleNamespace(output_id="result", path=tmp_path)

    plan = COGOutputWriter().build(_artifact(), output, None)

    # 2 times x (reflectance + scl) x 2x2 spatial chunks
    assert len(plan.tasks) == 16
    assert plan.completed == ()
    assert plan.metadata["checkpointed"] == set()


def test_build_without_scl_plans_reflectance_tiles_only(tmp_path):
    output = SimpleNamespace(output_id="result", path=tmp_path)

    plan = COGOutputWriter().build(_artifact(with_scl=False), output, None)

    assert len(plan.tasks) == 8


def test_written_tiles_are_georeferenced_and_named_by_product_and_chunk(tmp_path):
    output = SimpleNamespace(output_id="result", path=tmp_path)
    plan = COGOutputWriter().build(_artifact(), output, None)

    results = _compute(plan)

    assert all(result.status == WriteStatus.written for result in results)
    tile = tmp_path / "prod_a" / "gran_a" / "reflectance.r0001.c0001.tif"
    assert tile.is_file()
    with rasterio.open(tile) as dataset:
        assert dataset.count == 2
        assert dataset.descriptions == ("B02", "B03")
        # Chunk (row 1, column 1) starts 2 pixels right of and below the origin.
        assert dataset.transform.c == 1000.0 + 2 * 20.0
        assert dataset.transform.f == 5000.0 - 2 * 20.0
    with rasterio.open(tmp_path / "prod_b" / "gran_b" / "scl.r0000.c0000.tif") as scl:
        assert scl.count == 1 and scl.descriptions == ("SCL",)


def test_rebuilding_after_finalize_skips_completed_tiles(tmp_path):
    output = SimpleNamespace(output_id="result", path=tmp_path)
    writer = COGOutputWriter()
    plan = writer.build(_artifact(), output, None)
    written = _compute(plan)
    writer.finalize(plan, written)

    rebuilt = writer.build(_artifact(), output, None)

    assert rebuilt.tasks == ()
    assert len(rebuilt.completed) == 16
    assert {result.status for result in rebuilt.completed} == {WriteStatus.skipped}
    assert {result.task_id for result in rebuilt.completed} == {
        result.task_id for result in written
    }


def test_changed_recipe_invalidates_completed_tiles(tmp_path):
    output = SimpleNamespace(output_id="result", path=tmp_path)
    writer = COGOutputWriter()
    plan = writer.build(_artifact(), output, None)
    writer.finalize(plan, _compute(plan))

    rebuilt = writer.build(_artifact(recipe_id="other"), output, None)

    assert len(rebuilt.tasks) == 16
    assert rebuilt.completed == ()


def test_task_and_cache_keys_are_stable(tmp_path):
    output = SimpleNamespace(output_id="result", path=tmp_path)
    plan = COGOutputWriter().build(_artifact(), output, None)

    results = _compute(plan)
    first = next(
        r for r in results
        if r.product_id == "prod_a" and r.variable == "reflectance"
        and (r.row_index, r.column_index) == (0, 0)
    )

    assert first.task_id == "2b0a432b8fdd7a7e78ef456a56926955"
    assert first.cache_key == "377b5c46409ef9d8a8d7521eb02adbf4"


def test_cog_writer_checkpoints_batches_and_finalize_does_not_duplicate(tmp_path: Path):
    from sentinel_py.pipeline.writers.base import OutputWritePlan
    from sentinel_py.pipeline.writers.cog import COGOutputWriter, COGWriteResult

    def result(index: int) -> COGWriteResult:
        return COGWriteResult(
            f"task-{index}", "out", "p", "g", "reflectance", 0, index,
            tmp_path / f"{index}.tif", "key", "written", 10, None, "2026-01-01",
        )

    state_path = tmp_path / "state.parquet"
    store = RowStore(state_path, ["task_id"])
    plan = OutputWritePlan(
        None,
        tuple(object() for _ in range(4)),
        (),
        {"state_path": state_path, "state": pd.DataFrame(), "store": store,
         "checkpointed": set()},
    )
    writer = COGOutputWriter()
    all_results = tuple(result(i) for i in range(4))

    # Two batches finish before the run is interrupted: nothing else is needed.
    writer.checkpoint(plan, all_results[:2], 0)
    writer.checkpoint(plan, all_results[2:3], 2)
    assert sorted(store.read()["task_id"]) == ["task-0", "task-1", "task-2"]

    final = writer.finalize(plan, all_results)

    assert len(final) == 4
    assert store.part_paths() == []
    on_disk = pd.read_parquet(state_path)
    assert sorted(on_disk["task_id"]) == [f"task-{i}" for i in range(4)]


def test_cog_checkpoint_routes_batches_to_subplans(tmp_path: Path):
    from sentinel_py.pipeline.writers.base import OutputWritePlan
    from sentinel_py.pipeline.writers.cog import COGOutputWriter, COGWriteResult

    def result(name: str) -> COGWriteResult:
        return COGWriteResult(
            name, "out", "p", "g", "reflectance", 0, 0,
            tmp_path / f"{name}.tif", "key", "written", 1, None, "2026-01-01",
        )

    def leaf(name: str, count: int) -> OutputWritePlan:
        store = RowStore(tmp_path / f"{name}.parquet", ["task_id"])
        return OutputWritePlan(
            None, tuple(object() for _ in range(count)), (),
            {"store": store, "checkpointed": set(), "state_path": store.path,
             "state": pd.DataFrame()},
        )

    first, second = leaf("a", 2), leaf("b", 2)
    plan = OutputWritePlan(None, (), (), {"subplans": (first, second)})
    writer = COGOutputWriter()
    results = tuple(result(f"t{i}") for i in range(4))

    # A batch that straddles both sub-plans: tasks 1..2 of the output's 4 tasks.
    writer.checkpoint(plan, results[1:3], 1)

    assert first.metadata["store"].read()["task_id"].tolist() == ["t1"]
    assert second.metadata["store"].read()["task_id"].tolist() == ["t2"]


def test_cog_tile_replaces_nonfinite_float_values(tmp_path) -> None:
    rasterio = pytest.importorskip("rasterio")
    from sentinel_py.pipeline.writers.cog import _write_cog_tile

    output_path = tmp_path / "index.tif"
    result = _write_cog_tile(
        np.array([[1.0, np.nan]], dtype=np.float32),
        task_id="task",
        output_id="output",
        product_id="product",
        granule_id="granule",
        variable="reflectance",
        row_index=0,
        column_index=0,
        output_path=output_path,
        cache_key="cache",
        crs="EPSG:32606",
        transform=(20, 0, 0, 0, -20, 20),
        nodata=65535,
        descriptions=("NDGI",),
    )

    assert result.status == "written"
    with rasterio.open(output_path) as dataset:
        assert dataset.dtypes == ("float32",)
        assert dataset.nodata == 65535
        np.testing.assert_array_equal(dataset.read(1), [[1.0, 65535.0]])


def test_cog_writer_normalizes_split_band_chunks(tmp_path):
    da = pytest.importorskip("dask.array")
    from sentinel_py.pipeline.writers.cog import COGOutputWriter

    artifact = xr.Dataset(
        {
            "reflectance": (
                ("time", "band", "y", "x"),
                da.from_array(
                    np.ones((2, 2, 2, 2), dtype=np.float32), chunks=(1, 1, 2, 2)
                ),
                {"nodata": 65535},
            )
        },
        coords={
            "time": np.array(["2000-06-01", "2000-06-16"], dtype="datetime64[ns]"),
            "band": ["B02", "B03"],
            "y": [1, 0],
            "x": [0, 1],
            "product_id": ("time", ["composite_name", "composite_name"]),
            "granule_id": ("time", ["bin_0000", "bin_0001"]),
            "source_fingerprint": ("time", ["a", "b"]),
        },
        attrs={
            "crs": "EPSG:3338",
            "transform": (20, 0, 0, 0, -20, 40),
            "recipe_id": "recipe",
        },
    )
    output = SimpleNamespace(output_id="result", path=tmp_path)

    plan = COGOutputWriter().build(artifact, output, None)

    assert len(plan.tasks) == 2
