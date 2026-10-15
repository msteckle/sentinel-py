from __future__ import annotations

import logging
import warnings
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

xr = pytest.importorskip("xarray")

from sentinel_py.pipeline.processors.base import ProcessorContext  # noqa: E402
from sentinel_py.pipeline.artifacts import PointSampleArtifact  # noqa: E402
from sentinel_py.pipeline.processors.s2_composite import (  # noqa: E402
    S2CompositeNodeConfig,
    S2CompositeProcessor,
)


def _source() -> xr.Dataset:
    return xr.Dataset(
        {
            "reflectance": (
                ("time", "band", "y", "x"),
                np.array(
                    [[[[1]], [[9]]], [[[5]], [[3]]], [[[7]], [[4]]]], dtype=np.uint16
                ),
                {"nodata": 65535},
            )
        },
        coords={
            "time": np.array(
                ["2023-06-02", "2024-06-03", "2024-06-20"], dtype="datetime64[ns]"
            ),
            "band": ["B02", "B03"],
            "y": [0],
            "x": [0],
            "source_fingerprint": ("time", ["a", "b", "c"]),
        },
        attrs={"recipe_id": "source"},
    )


def _context() -> ProcessorContext:
    pipeline = SimpleNamespace(
        selection=SimpleNamespace(
            years=(2023, 2024), start_period=(6, 1), end_period=(8, 31)
        )
    )
    return ProcessorContext(
        pipeline,
        SimpleNamespace(node_id="composite"),
        (),
        logging.getLogger("test-composite"),
    )


def test_composite_uses_shared_seasonal_bins_across_years():
    result = S2CompositeProcessor().execute(
        S2CompositeNodeConfig(period="15D", aggregation="maximum"),
        {"data": _source()},
        _context(),
    )

    assert result.sizes["time"] == 2
    np.testing.assert_array_equal(
        result.reflectance.values[:, :, 0, 0],
        np.array([[5, 9], [7, 4]], dtype=np.float32),
    )
    assert result.attrs["period"] == "15D"
    assert result.attrs["composite_name"] == "composite_2023-2024_06-01-08-31_maximum"
    assert result.product_id.values.tolist() == [
        "composite_2023-2024_06-01-08-31_maximum",
        "composite_2023-2024_06-01-08-31_maximum",
    ]
    assert result.granule_id.values.tolist() == [
        "bin_0000_2000-06-01",
        "bin_0001_2000-06-16",
    ]
    assert result.reflectance.chunks[0] == (2,)
    assert result.reflectance.chunks[1] == (2,)
    assert result.reflectance.chunks[2:] == ((1,), (1,))


def test_composite_keeps_all_bands_available_for_index_calculations():
    result = S2CompositeProcessor().execute(
        S2CompositeNodeConfig(period="15D", aggregation="mean"),
        {"data": _source()},
        _context(),
    )

    index = result.reflectance.sel(band="B03") - result.reflectance.sel(band="B02")
    assert index.chunks[0] == (2,)




def test_composite_defaults_to_all_and_ignores_nodata():
    source = _source()
    source["reflectance"].values[1, 0, 0, 0] = 65535
    result = S2CompositeProcessor().execute(
        S2CompositeNodeConfig(),
        {"data": source},
        _context(),
    )

    assert result.sizes["time"] == 1
    np.testing.assert_allclose(
        result.reflectance.values[0, :, 0, 0],
        np.array([4, 5.3333335], dtype=np.float32),
    )


def test_composite_masks_all_nodata_without_runtime_warning():
    source = _source()
    source["reflectance"].values[:] = 65535

    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        result = S2CompositeProcessor().execute(
            S2CompositeNodeConfig(period="15D", aggregation="median"),
            {"data": source},
            _context(),
        )

    assert np.all(result.reflectance.values == 65535)


def test_point_composite_ignores_s2_nodata_sentinel():
    """Masked S2 rows must not dominate a point median composite."""
    rows = pd.DataFrame([
        {"aoi_id": "site", "component_id": "part-0000", "point_index": 0,
         "longitude": 0.0, "latitude": 0.0, "time": "2024-06-01",
         "product_id": "p1", "granule_id": "g1", "time_representation": "acquisition",
         "source_fingerprint": "one", "B02": 0},
        {"aoi_id": "site", "component_id": "part-0000", "point_index": 0,
         "longitude": 0.0, "latitude": 0.0, "time": "2024-06-05",
         "product_id": "p2", "granule_id": "g2", "time_representation": "acquisition",
         "source_fingerprint": "two", "B02": 400},
        {"aoi_id": "site", "component_id": "part-0000", "point_index": 0,
         "longitude": 0.0, "latitude": 0.0, "time": "2024-06-10",
         "product_id": "p3", "granule_id": "g3", "time_representation": "acquisition",
         "source_fingerprint": "three", "B02": 600},
    ])
    result = S2CompositeProcessor().execute_points(
        S2CompositeNodeConfig(period="all", aggregation="median"),
        {"data": PointSampleArtifact(rows, SimpleNamespace())},
        _context(),
    )
    assert result.rows.loc[0, "B02"] == 500


@pytest.mark.parametrize("period", ["all", "ALL", " All "])
def test_composite_accepts_all_period(period: str):
    assert S2CompositeNodeConfig(period=period).period == "all"


@pytest.mark.parametrize("period", ["15", "0D", "day", "1H"])
def test_composite_rejects_unsupported_period(period: str):
    with pytest.raises(ValueError):
        S2CompositeNodeConfig(period=period)


def _backscatter() -> xr.Dataset:
    """A Sentinel-1-style float cube with NaN nodata and no ``reflectance``."""
    values = np.array(
        [[[[0.1]], [[0.02]]], [[[0.3]], [[np.nan]]], [[[0.2]], [[0.04]]]],
        dtype=np.float32,
    )
    return xr.Dataset(
        {
            "backscatter": (
                ("time", "band", "y", "x"),
                values,
                {"nodata": float("nan"), "long_name": "sigma0"},
            )
        },
        coords={
            "time": np.array(
                ["2023-06-02", "2024-06-03", "2024-06-20"], dtype="datetime64[ns]"
            ),
            "band": ["VV", "VH"],
            "y": [0],
            "x": [0],
            "source_fingerprint": ("time", ["a", "b", "c"]),
        },
        attrs={"recipe_id": "s1-source"},
    )


def test_composite_aggregates_a_float_backscatter_cube():
    result = S2CompositeProcessor().execute(
        S2CompositeNodeConfig(aggregation="mean"), {"data": _backscatter()}, _context()
    )

    assert list(result.data_vars) == ["backscatter"]
    assert result.backscatter.dtype == np.float32
    np.testing.assert_allclose(
        result.backscatter.values[0, :, 0, 0], [0.2, 0.03], rtol=1e-6
    )  # VH ignores the NaN observation
    assert result.backscatter.attrs["long_name"] == "sigma0"
    assert np.isnan(result.backscatter.attrs["nodata"])


def test_composite_keeps_all_nan_pixels_as_nan_without_warnings():
    source = _backscatter()
    source["backscatter"].values[:] = np.nan
    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        result = S2CompositeProcessor().execute(
            S2CompositeNodeConfig(period="15D", aggregation="median"),
            {"data": source},
            _context(),
        )
    assert np.isnan(result.backscatter.values).all()


def test_composite_variable_selection_and_errors():
    source = _backscatter()
    source["extra"] = source["backscatter"] * 2
    processor = S2CompositeProcessor()

    with pytest.raises(TypeError, match="exactly one"):
        processor.execute(S2CompositeNodeConfig(), {"data": source}, _context())
    with pytest.raises(TypeError, match="'missing'"):
        processor.execute(
            S2CompositeNodeConfig(variable="missing"), {"data": source}, _context()
        )
    result = processor.execute(
        S2CompositeNodeConfig(variable="extra"), {"data": source}, _context()
    )
    assert list(result.data_vars) == ["extra"]
    np.testing.assert_allclose(result.extra.values[0, 0, 0, 0], 0.4, rtol=1e-6)


def test_composite_recipe_id_is_unchanged_for_reflectance_and_varies_by_variable():
    from sentinel_py.pipeline.processors.base import make_recipe_id

    processor = S2CompositeProcessor()
    config = S2CompositeNodeConfig(period="15D", aggregation="maximum")
    s2 = processor.execute(config, {"data": _source()}, _context())
    # Recorded recipe: Sentinel-2 composites must keep their pre-generalization ID.
    assert s2.attrs["recipe_id"] == make_recipe_id(
        "composite", 2, period="15D", aggregation="maximum", source_recipe="source"
    )
    s1 = processor.execute(config, {"data": _backscatter()}, _context())
    assert s1.attrs["recipe_id"] != s2.attrs["recipe_id"]


def test_point_composite_keeps_s1_zeros_and_skips_scene_attributes():
    def row(index: int, vv: float) -> dict:
        return {
            "aoi_id": "site", "component_id": "part-0000", "point_index": 0,
            "longitude": 0.0, "latitude": 0.0, "time": f"2024-06-0{index}",
            "product_id": f"p{index}", "granule_id": f"g{index}",
            "time_representation": "acquisition", "source_fingerprint": str(index),
            "VV": vv, "VH": 0.05, "flight_direction": "DESCENDING",
            "relative_orbit": 73, "orbit_type": "POEORB",
        }

    rows = pd.DataFrame([row(1, 0.0), row(2, 0.2)])
    result = S2CompositeProcessor().execute_points(
        S2CompositeNodeConfig(period="all", aggregation="mean"),
        {"data": PointSampleArtifact(rows, SimpleNamespace())},
        _context(),
    )

    assert result.rows.loc[0, "VV"] == pytest.approx(0.1)  # zero stays a value
    assert result.rows.loc[0, "VH"] == pytest.approx(0.05)
    assert not {"flight_direction", "relative_orbit", "orbit_type"} & set(
        result.rows.columns
    )
