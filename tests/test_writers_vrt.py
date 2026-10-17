"""VRT mosaics of COG tiles: per AOI unit, per output, encoding, and relocation."""

from __future__ import annotations

import logging
import shutil
from dataclasses import replace
from types import SimpleNamespace

import numpy as np
import pytest

xr = pytest.importorskip("xarray")
da = pytest.importorskip("dask.array")
rasterio = pytest.importorskip("rasterio")

from sentinel_py.enums import AOIKind, WriteStatus  # noqa: E402
from sentinel_py.pipeline.artifacts import RasterArtifact  # noqa: E402
from sentinel_py.pipeline.config import OutputConfig  # noqa: E402
from sentinel_py.pipeline.writers.cog import COGOutputWriter  # noqa: E402
from test_writers_cog import (  # noqa: E402
    _artifact,
    _backscatter_artifact,
    _compute,
    _db_artifact,
    _encoding,
)


def _write(artifact, tmp_path, **output_fields):
    """Build, compute and finalize one dataset; return the results."""
    output = SimpleNamespace(output_id="result", path=tmp_path, **output_fields)
    writer = COGOutputWriter()
    plan = writer.build(artifact, output, None)
    return writer.finalize(plan, _compute(plan))


def test_vrt_reads_back_as_the_stitched_tiles(tmp_path) -> None:
    artifact = _artifact()
    _write(artifact, tmp_path)

    with rasterio.open(tmp_path / "prod_a" / "gran_a" / "reflectance.vrt") as vrt:
        assert (vrt.count, vrt.height, vrt.width) == (2, 4, 4)
        assert vrt.descriptions == ("B02", "B03")
        assert vrt.nodata == 65535 and vrt.crs.to_string() == "EPSG:3338"
        assert tuple(vrt.transform)[:6] == (20.0, 0.0, 1000.0, 0.0, -20.0, 5000.0)
        np.testing.assert_array_equal(vrt.read(), artifact.reflectance.values[0])
    with rasterio.open(tmp_path / "prod_b" / "gran_b" / "scl.vrt") as scl:
        assert scl.dtypes == ("uint8",) and scl.descriptions == ("SCL",)
        np.testing.assert_array_equal(scl.read(1), np.full((4, 4), 4))


def test_ragged_edge_tiles_are_placed_correctly(tmp_path) -> None:
    artifact = _artifact(with_scl=False).isel(x=slice(0, 3), y=slice(0, 3))
    artifact["reflectance"] = artifact["reflectance"].chunk({"y": 2, "x": 2})
    _write(artifact, tmp_path)

    with rasterio.open(tmp_path / "prod_a" / "gran_a" / "reflectance.vrt") as vrt:
        assert (vrt.height, vrt.width) == (3, 3)
        np.testing.assert_array_equal(vrt.read(), artifact.reflectance.values[0])


def test_nan_nodata_float_vrt(tmp_path) -> None:
    artifact = _backscatter_artifact()
    _write(artifact, tmp_path)

    with rasterio.open(tmp_path / "comp" / "bin_a" / "backscatter.vrt") as vrt:
        assert np.isnan(vrt.nodata) and vrt.dtypes == ("float32", "float32")
        read = vrt.read()
        expected = artifact.backscatter.values[0]
        np.testing.assert_array_equal(np.isnan(read), np.isnan(expected))
        np.testing.assert_array_equal(read[~np.isnan(read)], expected[~np.isnan(expected)])


def test_packed_variable_keeps_scale_offset_and_unpacks(tmp_path) -> None:
    artifact, values = _db_artifact()
    _write(artifact, tmp_path, encoding=_encoding())

    with rasterio.open(tmp_path / "comp" / "bin_a" / "backscatter.vrt") as vrt:
        assert vrt.dtypes == ("int16", "int16") and vrt.nodata == -32768
        assert vrt.scales == (0.01, 0.01) and vrt.offsets == (0.0, 0.0)
        stored = vrt.read().astype("float64")
        valid = stored != -32768
        # NaN input stays the reserved code; the rest unpacks within a quantization step.
        assert not valid[0, 0, 0] and valid.sum() == valid.size - 1
        np.testing.assert_allclose(
            (stored * 0.01)[valid], values[0][valid], atol=0.01
        )


def test_moved_output_directory_still_opens(tmp_path) -> None:
    source = tmp_path / "written"
    _write(_artifact(), source)
    moved = tmp_path / "elsewhere" / "copy"
    shutil.copytree(source, moved)
    shutil.rmtree(source)

    with rasterio.open(moved / "prod_a" / "gran_a" / "reflectance.vrt") as vrt:
        assert vrt.read().shape == (2, 4, 4)


def test_a_failed_tile_leaves_that_vrt_out(tmp_path) -> None:
    artifact = _artifact(with_scl=False)
    output = SimpleNamespace(output_id="result", path=tmp_path)
    writer = COGOutputWriter()
    plan = writer.build(artifact, output, None)
    computed = list(_compute(plan))
    broken = next(
        index for index, result in enumerate(computed) if result.product_id == "prod_a"
    )
    computed[broken].output_path.unlink()
    computed[broken] = replace(computed[broken], status=WriteStatus.failed)

    writer.finalize(plan, tuple(computed))

    assert not (tmp_path / "prod_a" / "gran_a" / "reflectance.vrt").exists()
    assert (tmp_path / "prod_b" / "gran_b" / "reflectance.vrt").exists()


def test_decimated_vrt_reads_work_through_tile_overviews(tmp_path) -> None:
    columns = np.arange(1024, dtype=np.float32)
    values = np.tile(columns, (1024, 1))[np.newaxis, np.newaxis, ...]
    artifact = xr.Dataset(
        {"reflectance": (("time", "band", "y", "x"), da.from_array(values, chunks=(1, 1, 1024, 1024)), {"nodata": -1.0})},
        coords={
            "time": np.array(["2000-06-01"], dtype="datetime64[ns]"),
            "band": ["b"],
            "product_id": ("time", ["p"]),
            "granule_id": ("time", ["g"]),
            "source_fingerprint": ("time", ["f"]),
        },
        attrs={
            "crs": "EPSG:3338",
            "transform": (20.0, 0.0, 0.0, 0.0, -20.0, 20480.0),
            "recipe_id": "r",
        },
    )
    _write(artifact, tmp_path)

    with rasterio.open(tmp_path / "p" / "g" / "reflectance.r0000.c0000.tif") as tile:
        assert tile.overviews(1)  # the COG tile carries pyramids
    with rasterio.open(tmp_path / "p" / "g" / "reflectance.vrt") as vrt:
        small = vrt.read(1, out_shape=(128, 128))
    assert small.shape == (128, 128)
    assert abs(float(small[:, 64].mean()) - 64 * 8) < 16  # still the x gradient


def _two_unit_artifact(second_bands=("B02", "B03")):
    """Two AOI units at distant places, the second shifted in value."""
    first = _artifact(with_scl=False)
    second = _artifact(with_scl=False)
    second["reflectance"] = (second["reflectance"] + 1000).assign_attrs(nodata=65535)
    second = second.assign_coords(band=list(second_bands))
    second.attrs["transform"] = (20.0, 0.0, 3000.0, 0.0, -20.0, 4000.0)
    keys = [("a", "part-0000"), ("b", "part-0000")]
    plan = SimpleNamespace(
        kind=AOIKind.polygon,
        by_key={key: SimpleNamespace(aoi_id=key[0], component_id=key[1]) for key in keys},
    )
    return RasterArtifact(plan, dict(zip(keys, (first, second)))), first, second


def test_mosaic_vrt_covers_every_unit_and_leaves_gaps_as_nodata(tmp_path) -> None:
    artifact, first, second = _two_unit_artifact()
    output = OutputConfig("result", "node", tmp_path, "cog")
    writer = COGOutputWriter()
    plan = writer.build(artifact, output, None)
    writer.finalize(plan, _compute(plan))

    assert (tmp_path / "a" / "part-0000" / "prod_a" / "gran_a" / "reflectance.vrt").exists()
    with rasterio.open(tmp_path / "prod_a" / "gran_a" / "reflectance.vrt") as mosaic:
        # Unit a spans x 1000-1080, y 5000-4920; unit b x 3000-3080, y 4000-3920.
        assert mosaic.bounds.left == 1000.0 and mosaic.bounds.right == 3080.0
        assert mosaic.bounds.top == 5000.0 and mosaic.bounds.bottom == 3920.0
        window_a = mosaic.read(window=mosaic.window(1000, 4920, 1080, 5000))
        window_b = mosaic.read(window=mosaic.window(3000, 3920, 3080, 4000))
        np.testing.assert_array_equal(window_a, first.reflectance.values[0])
        np.testing.assert_array_equal(window_b, second.reflectance.values[0])
        gap = mosaic.read(1, window=mosaic.window(2000, 4400, 2020, 4420))
        assert (gap == 65535).all()


def test_mosaic_leaves_out_a_unit_whose_bands_differ(tmp_path, caplog) -> None:
    artifact, first, _ = _two_unit_artifact(second_bands=("X", "Y"))
    output = OutputConfig("result", "node", tmp_path, "cog")
    writer = COGOutputWriter()
    plan = writer.build(artifact, output, None)

    with caplog.at_level(logging.WARNING):
        writer.finalize(plan, _compute(plan))

    assert "differ" in caplog.text
    with rasterio.open(tmp_path / "prod_a" / "gran_a" / "reflectance.vrt") as mosaic:
        assert mosaic.bounds.right == 1080.0 and mosaic.descriptions == ("B02", "B03")
