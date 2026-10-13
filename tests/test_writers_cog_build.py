"""Characterization of ``COGOutputWriter.build``: tile planning, ids, and resume."""

from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pytest

xr = pytest.importorskip("xarray")
da = pytest.importorskip("dask.array")
rasterio = pytest.importorskip("rasterio")

from sentinel_py.enums import WriteStatus  # noqa: E402
from sentinel_py.pipeline.writers.cog import COGOutputWriter  # noqa: E402


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
