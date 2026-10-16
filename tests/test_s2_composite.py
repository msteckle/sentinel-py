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


def test_composite_recipe_id_covers_empty_bins_and_varies_by_variable():
    from sentinel_py.pipeline.processors.base import make_recipe_id

    processor = S2CompositeProcessor()
    config = S2CompositeNodeConfig(period="15D", aggregation="maximum")
    s2 = processor.execute(config, {"data": _source()}, _context())
    # Recorded recipe for version 3 (bin labels fixed; see PROCESSOR_VERSION).
    assert s2.attrs["recipe_id"] == make_recipe_id(
        "composite", 3, period="15D", aggregation="maximum", empty_bins="drop",
        source_recipe="source",
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


# Empty seasonal bins and missing data -------------------------------------------------


def _sparse_cube(float_data: bool = False) -> xr.Dataset:
    """Observations in bin 0 (06-02) and bin 2 (07-04) of 15-day bins from 06-01."""
    if float_data:
        values, nodata, dtype = np.array([[[[1.0]]], [[[3.0]]]]), float("nan"), "float32"
    else:
        values, nodata, dtype = np.array([[[[10]]], [[[30]]]]), 65535, "uint16"
    return xr.Dataset(
        {
            "reflectance": (
                ("time", "band", "y", "x"),
                values.astype(dtype),
                {"nodata": nodata},
            )
        },
        coords={
            "time": np.array(["2023-06-02", "2024-07-04"], dtype="datetime64[ns]"),
            "band": ["B02"], "y": [0], "x": [0],
            "source_fingerprint": ("time", ["a", "b"]),
        },
        attrs={"recipe_id": "source"},
    )


def test_dropped_bins_keep_their_true_seasonal_position():
    """Regression: the label of a bin after an empty one named the wrong slot."""
    result = S2CompositeProcessor().execute(
        S2CompositeNodeConfig(period="15D", aggregation="mean"),
        {"data": _sparse_cube()}, _context(),
    )
    assert result.granule_id.values.tolist() == [
        "bin_0000_2000-06-01", "bin_0002_2000-07-01",
    ]
    assert [str(t)[:10] for t in result.time.values] == ["2000-06-01", "2000-07-01"]


def test_fill_emits_every_bin_with_nodata_for_empty_ones():
    result = S2CompositeProcessor().execute(
        S2CompositeNodeConfig(period="15D", aggregation="mean", empty_bins="fill"),
        {"data": _sparse_cube()}, _context(),
    )
    labels = result.granule_id.values.tolist()
    assert labels[:4] == [
        "bin_0000_2000-06-01", "bin_0001_2000-06-16",
        "bin_0002_2000-07-01", "bin_0003_2000-07-16",
    ]
    # 06-01 .. 08-31 in 15-day bins: six full bins and a two-day remainder.
    assert len(labels) == result.sizes["time"] == 7
    values = result.reflectance.values[:, 0, 0, 0]
    assert values.tolist() == [10.0, 65535.0, 30.0, 65535.0, 65535.0, 65535.0, 65535.0]
    assert result.source_fingerprint.values[1] != result.source_fingerprint.values[0]


def test_fill_uses_nan_for_floating_point_cubes_and_ignores_nan_inputs():
    cube = _sparse_cube(float_data=True)
    cube["reflectance"].values[0, 0, 0, 0] = np.nan  # the only bin-0 sample is NaN
    result = S2CompositeProcessor().execute(
        S2CompositeNodeConfig(period="15D", aggregation="median", empty_bins="fill"),
        {"data": cube}, _context(),
    )
    values = result.reflectance.values[:, 0, 0, 0]
    assert np.isnan(values[[0, 1, 3, 4, 5, 6]]).all() and values[2] == 3.0


def test_zero_time_cube_needs_fill():
    empty = _sparse_cube(float_data=True).isel(time=slice(0, 0))
    with pytest.raises(ValueError, match="empty_bins: fill"):
        S2CompositeProcessor().execute(
            S2CompositeNodeConfig(), {"data": empty}, _context()
        )
    result = S2CompositeProcessor().execute(
        S2CompositeNodeConfig(period="all", empty_bins="fill"), {"data": empty}, _context()
    )
    assert result.sizes["time"] == 1 and np.isnan(result.reflectance.values).all()
    assert result.granule_id.values.tolist() == ["bin_0000_2000-06-01"]


def _point_unit(index: int):
    from shapely.geometry import Point

    from sentinel_py.enums import AOIKind

    return SimpleNamespace(
        aoi_id=f"site-{index}", component_id=f"part-{index}", kind=AOIKind.point,
        geometry_wgs84=Point(-150.0 + index, 68.0),
    )


def _point_rows(*indices: int) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "aoi_id": f"site-{i}", "component_id": f"part-{i}", "point_index": i,
                "longitude": -150.0 + i, "latitude": 68.0, "time": "20240601T000000",
                "product_id": "p", "granule_id": "g", "time_representation": "acquisition",
                "source_fingerprint": "f", "VV": 0.1 * (i + 1), "VH": 0.01,
            }
            for i in indices
        ]
    )


def test_point_composite_fill_adds_nodata_rows_for_points_without_data():
    plan = SimpleNamespace(units=(_point_unit(0), _point_unit(1)))
    result = S2CompositeProcessor().execute_points(
        S2CompositeNodeConfig(period="all", aggregation="mean", empty_bins="fill"),
        {"data": PointSampleArtifact(_point_rows(0), plan)}, _context(),
    )
    rows = result.rows.set_index("aoi_id")
    assert list(rows.index) == ["site-0", "site-1"]
    assert rows.loc["site-0", "VV"] == pytest.approx(0.1)
    assert np.isnan(rows.loc["site-1", "VV"]) and np.isnan(rows.loc["site-1", "VH"])
    assert rows.loc["site-1", "time_representation"] == "composite"
    # Without fill the point is simply absent.
    plain = S2CompositeProcessor().execute_points(
        S2CompositeNodeConfig(period="all", aggregation="mean"),
        {"data": PointSampleArtifact(_point_rows(0), plan)}, _context(),
    )
    assert list(plain.rows["aoi_id"]) == ["site-0"]


def test_point_composite_fill_handles_a_table_with_no_rows():
    plan = SimpleNamespace(units=(_point_unit(0), _point_unit(1)))
    empty = _point_rows(0).iloc[0:0].astype({"VV": "float64", "VH": "float64"})
    result = S2CompositeProcessor().execute_points(
        S2CompositeNodeConfig(period="all", aggregation="median", empty_bins="fill"),
        {"data": PointSampleArtifact(empty, plan)}, _context(),
    )
    assert len(result.rows) == 2 and result.rows[["VV", "VH"]].isna().all().all()
    assert result.rows[["VV", "VH"]].isna().all().all()


# Binned (climatological) point composites ------------------------------------------


def _obs(point: int, time: str, vv: float, vh: float = 0.01) -> dict:
    row = _point_rows(point).iloc[0].to_dict()
    row.update(time=time, VV=vv, VH=vh, product_id=f"p-{time}", granule_id=f"g-{time}",
               source_fingerprint=f"fp-{time}-{point}")
    return row


def _binned(rows, *, units=(0, 1), **config):
    plan = SimpleNamespace(units=tuple(_point_unit(i) for i in units))
    return S2CompositeProcessor().execute_points(
        S2CompositeNodeConfig(**config),
        {"data": PointSampleArtifact(pd.DataFrame(rows), plan)}, _context(),
    ).rows


def test_binned_point_composite_pools_years_into_shared_seasonal_slots():
    rows = _binned(
        [
            _obs(0, "20230602T164404", 0.1),    # bin 0 (06-01), year 2023
            _obs(0, "2024-06-10", 0.3),         # bin 0, year 2024, ISO date
            _obs(0, "20240704T170031", 0.5),    # bin 2 (07-01)
            _obs(1, "20230820T164404", 0.7),    # bin 5 (08-15)
        ],
        period="15D", aggregation="mean",
    )
    assert rows[["aoi_id", "granule_id", "time"]].values.tolist() == [
        ["site-0", "bin_0000_2000-06-01", "06-01"],
        ["site-0", "bin_0002_2000-07-01", "07-01"],
        ["site-1", "bin_0005_2000-08-15", "08-15"],
    ]
    assert rows["VV"].tolist() == pytest.approx([0.2, 0.5, 0.7])
    assert set(rows["time_representation"]) == {"composite"}
    assert set(rows["product_id"]) == {"composite"}
    assert "bin" not in rows.columns


def test_binned_point_composite_ignores_nan_and_keeps_all_nan_bins_as_nan():
    rows = _binned(
        [
            _obs(0, "2023-06-02", np.nan),
            _obs(0, "2024-06-10", 0.3),            # the valid one in bin 0
            _obs(0, "2024-07-04", np.nan, np.nan),  # bin 2 has only NaN
        ],
        period="15D", aggregation="median", units=(0,),
    )
    by_bin = rows.set_index("granule_id")
    assert by_bin.loc["bin_0000_2000-06-01", "VV"] == pytest.approx(0.3)
    assert np.isnan(by_bin.loc["bin_0002_2000-07-01", "VV"])
    assert np.isnan(by_bin.loc["bin_0002_2000-07-01", "VH"])


def test_binned_point_composite_fill_gives_every_point_every_bin():
    rows = _binned(
        [_obs(0, "2024-06-10", 0.3), _obs(1, "2024-08-20", 0.7)],
        period="15D", aggregation="maximum", empty_bins="fill",
    )
    assert len(rows) == 2 * 7  # two points, seven 15-day bins from 06-01 to 08-31
    assert rows.groupby("aoi_id").size().tolist() == [7, 7]
    assert rows["granule_id"].iloc[:7].tolist() == [
        f"bin_{i:04d}_{d}" for i, d in enumerate(
            ["2000-06-01", "2000-06-16", "2000-07-01", "2000-07-16",
             "2000-07-31", "2000-08-15", "2000-08-30"]
        )
    ]
    valid = rows.dropna(subset=["VV"])
    assert valid[["aoi_id", "granule_id"]].values.tolist() == [
        ["site-0", "bin_0000_2000-06-01"], ["site-1", "bin_0005_2000-08-15"],
    ]
    # Rows are ordered by point then bin, and fill rows carry real point identity.
    assert rows["aoi_id"].is_monotonic_increasing
    assert rows.loc[rows["aoi_id"] == "site-1", "latitude"].eq(68.0).all()


def test_binned_point_composite_drops_out_of_season_and_rejects_bad_times():
    rows = _binned(
        [_obs(0, "2023-09-10", 9.0), _obs(0, "2023-06-02", 0.1)],
        period="15D", aggregation="mean", units=(0,),
    )
    assert rows["VV"].tolist() == [0.1]  # September is outside the 06-01..08-31 season

    with pytest.raises(ValueError, match="cannot parse"):
        _binned([_obs(0, "not-a-date", 0.1)], period="15D", units=(0,))


def test_binned_point_fingerprint_names_the_members_and_all_stays_literal():
    first = _binned([_obs(0, "2023-06-02", 0.1), _obs(0, "2024-06-10", 0.3)],
                    period="15D", units=(0,))
    reordered = _binned([_obs(0, "2024-06-10", 0.3), _obs(0, "2023-06-02", 0.1)],
                        period="15D", units=(0,))
    fewer = _binned([_obs(0, "2023-06-02", 0.1)], period="15D", units=(0,))
    assert first["source_fingerprint"].iloc[0] == reordered["source_fingerprint"].iloc[0]
    assert first["source_fingerprint"].iloc[0] != fewer["source_fingerprint"].iloc[0]
    single = _binned([_obs(0, "2023-06-02", 0.1)], period="all", units=(0,))
    assert single["source_fingerprint"].iloc[0] == "composite"


def test_period_all_point_columns_keep_their_historic_order():
    rows = _binned([_obs(0, "2023-06-02", 0.1)], period="all", units=(0,))
    assert list(rows.columns) == [
        "aoi_id", "component_id", "point_index", "longitude", "latitude", "VV", "VH",
        "time", "product_id", "granule_id", "time_representation", "source_fingerprint",
    ]


def test_binned_point_composite_fill_with_no_rows_at_all():
    empty = _point_rows(0).iloc[0:0].astype({"VV": "float64", "VH": "float64"})
    plan = SimpleNamespace(units=(_point_unit(0),))
    result = S2CompositeProcessor().execute_points(
        S2CompositeNodeConfig(period="15D", aggregation="median", empty_bins="fill"),
        {"data": PointSampleArtifact(empty, plan)}, _context(),
    )
    assert len(result.rows) == 7 and result.rows[["VV", "VH"]].isna().all().all()
