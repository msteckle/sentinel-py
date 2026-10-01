from __future__ import annotations

import logging
from types import SimpleNamespace

import numpy as np
import pytest

xr = pytest.importorskip("xarray")

from sentinel_py.pipeline.processors.base import ProcessorContext  # noqa: E402
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


@pytest.mark.parametrize("period", ["all", "ALL", " All "])
def test_composite_accepts_all_period(period: str):
    assert S2CompositeNodeConfig(period=period).period == "all"


@pytest.mark.parametrize("period", ["15", "0D", "day", "1H"])
def test_composite_rejects_unsupported_period(period: str):
    with pytest.raises(ValueError):
        S2CompositeNodeConfig(period=period)
