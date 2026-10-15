"""``to_db`` processor: raster and point conversion, masking, and refusals."""

from __future__ import annotations

import logging
import warnings
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

xr = pytest.importorskip("xarray")
da = pytest.importorskip("dask.array")

from sentinel_py.pipeline.artifacts import PointSampleArtifact, RasterArtifact  # noqa: E402
from sentinel_py.pipeline.processors import get_default_registry  # noqa: E402
from sentinel_py.pipeline.processors.base import ProcessorContext  # noqa: E402
from sentinel_py.pipeline.processors.db import (  # noqa: E402
    ToDBNodeConfig,
    ToDBProcessor,
    to_decibels,
)


def _cube(values=None, dtype="float32", attrs=None) -> xr.Dataset:
    values = (
        np.array([[[[0.1, 1.0]], [[0.01, 0.0]]]], dtype=dtype)
        if values is None
        else values
    )
    return xr.Dataset(
        {
            "backscatter": (
                ("time", "band", "y", "x"),
                da.from_array(values, chunks=(1, 2, 1, 1)),
                {"nodata": float("nan"), "long_name": "sigma0", **(attrs or {})},
            ),
            "quality": (("time", "y", "x"), np.ones((1, 1, 2), dtype="uint8")),
        },
        coords={
            "time": np.array(["2024-06-01"], dtype="datetime64[ns]"),
            "band": ["VV", "VH"],
            "y": [0.0],
            "x": [0.0, 1.0],
            "product_id": ("time", ["p"]),
        },
        attrs={"recipe_id": "upstream", "crs": "EPSG:3338"},
    )


def _context(point: bool = False) -> ProcessorContext:
    kind = "point" if point else "polygon"
    return ProcessorContext(
        SimpleNamespace(aoi_plan=SimpleNamespace(kind=kind)),
        SimpleNamespace(node_id="db"),
        (),
        logging.getLogger("test-to-db"),
    )


def test_to_decibels_masks_non_positive_and_nan_without_warnings():
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        result = to_decibels(np.array([0.1, 1.0, 10.0, 0.0, -1.0, np.nan]))
    np.testing.assert_allclose(result[:3], [-10.0, 0.0, 10.0], atol=1e-6)
    assert np.isnan(result[3:]).all()
    assert result.dtype == np.float32


def test_raster_conversion_renames_variable_and_stays_lazy():
    result = ToDBProcessor().execute(ToDBNodeConfig(), {"data": _cube()}, _context())

    assert "backscatter" not in result and "backscatter_db" in result
    assert "quality" in result  # unrelated variables are kept
    converted = result["backscatter_db"]
    assert converted.chunks is not None  # still dask-backed
    assert converted.chunks == _cube()["backscatter"].chunks
    assert converted.dtype == np.float32
    assert converted.attrs["units"] == "dB"
    assert converted.attrs["long_name"] == "sigma0 (dB)"
    assert np.isnan(converted.attrs["nodata"])
    values = converted.values[0, :, 0, :]
    np.testing.assert_allclose(values[0], [-10.0, 0.0], atol=1e-6)
    assert values[1, 0] == pytest.approx(-20.0, abs=1e-5)
    assert np.isnan(values[1, 1])  # zero stays missing, not -inf
    assert result.attrs["crs"] == "EPSG:3338"
    assert result.attrs["processor"] == "to_db"


def test_recipe_id_depends_on_upstream_recipe_and_variable():
    processor = ToDBProcessor()
    base = processor.execute(ToDBNodeConfig(), {"data": _cube()}, _context())
    changed = _cube()
    changed.attrs["recipe_id"] = "other-upstream"
    other = processor.execute(ToDBNodeConfig(), {"data": changed}, _context())
    assert base.attrs["recipe_id"] != other.attrs["recipe_id"]
    assert len(base.attrs["recipe_id"]) == 12


def test_raster_artifact_is_converted_per_unit():
    plan = SimpleNamespace(by_key={("a", "c"): object()})
    artifact = RasterArtifact(plan, {("a", "c"): _cube()})
    result = ToDBProcessor().execute(
        ToDBNodeConfig(), {"data": artifact}, _context()
    )
    assert isinstance(result, RasterArtifact)
    assert "backscatter_db" in result.datasets[("a", "c")]


@pytest.mark.parametrize(
    ("cube", "match"),
    [
        (_cube(np.ones((1, 2, 1, 2), dtype="uint16")), "floating-point linear power"),
        (_cube(attrs={"units": "dB"}), "already in dB"),
    ],
)
def test_conversion_refusals(cube, match):
    with pytest.raises((TypeError, ValueError), match=match):
        ToDBProcessor().execute(ToDBNodeConfig(), {"data": cube}, _context())


def test_variable_selection_errors():
    cube = _cube()
    cube["other"] = cube["backscatter"] * 2
    with pytest.raises(TypeError, match="exactly one"):
        ToDBProcessor().execute(ToDBNodeConfig(), {"data": cube}, _context())
    result = ToDBProcessor().execute(
        ToDBNodeConfig(variable="other"), {"data": cube}, _context()
    )
    assert "other_db" in result and "backscatter" in result


def _rows(**columns) -> PointSampleArtifact:
    frame = pd.DataFrame(
        {
            "aoi_id": ["site", "site"],
            "granule_id": ["g1", "g2"],
            "flight_direction": ["DESCENDING", "DESCENDING"],
            **columns,
        }
    )
    return PointSampleArtifact(frame, SimpleNamespace())


def test_point_rows_convert_polarization_columns_only():
    source = _rows(VV=[0.1, 0.0], VH=[0.01, 1.0])
    result = ToDBProcessor().execute_points(
        ToDBNodeConfig(), {"data": source}, _context(point=True)
    )
    rows = result.rows
    assert rows["VV"].iloc[0] == pytest.approx(-10.0, abs=1e-5)
    assert np.isnan(rows["VV"].iloc[1])
    assert rows["VH"].tolist() == pytest.approx([-20.0, 0.0], abs=1e-5)
    assert set(rows["units"]) == {"dB"}
    assert rows["flight_direction"].tolist() == ["DESCENDING"] * 2
    assert source.rows["VV"].iloc[0] == 0.1  # input table is not mutated


def test_point_rows_refusals():
    processor = ToDBProcessor()
    with pytest.raises(TypeError, match="no polarization columns"):
        processor.execute_points(
            ToDBNodeConfig(), {"data": _rows(B02=[1, 2])}, _context(point=True)
        )
    converted = processor.execute_points(
        ToDBNodeConfig(), {"data": _rows(VV=[0.1, 0.2])}, _context(point=True)
    )
    with pytest.raises(ValueError, match="already in dB"):
        processor.execute_points(
            ToDBNodeConfig(), {"data": converted}, _context(point=True)
        )


def test_registered_with_a_version():
    processor = get_default_registry().get("to_db")
    assert processor.version == 1 and processor.type_name == "to_db"
    with pytest.raises(ValueError, match="exactly one upstream input"):
        processor.validate(
            ToDBNodeConfig(), node_id="db", inputs={}, sources={}, outputs=(),
            output_grid=None,
        )
