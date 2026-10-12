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
from sentinel_py.pipeline.processors.s2_index import (  # noqa: E402
    NDGI,
    NDWI1,
    S2IndexNodeConfig,
    S2IndexProcessor,
    SpectralIndex,
)
from sentinel_py.pipeline.processors.base import get_default_registry  # noqa: E402


def _source() -> xr.Dataset:
    values = np.array(
        [
            [[[2]], [[4]], [[8]], [[10]], [[12]]],
            [[[3]], [[5]], [[9]], [[11]], [[13]]],
        ],
        dtype=np.uint16,
    )
    return xr.Dataset(
        {
            "reflectance": (
                ("time", "band", "y", "x"),
                values,
                {"nodata": 65535},
            )
        },
        coords={
            "time": np.array(["2023-06-01", "2023-06-16"], dtype="datetime64[ns]"),
            "band": ["B03", "B04", "B08", "B11", "B12"],
            "y": [0],
            "x": [0],
            "product_id": ("time", ["product", "product"]),
            "granule_id": ("time", ["bin0", "bin1"]),
            "source_fingerprint": ("time", ["source0", "source1"]),
        },
        attrs={
            "crs": "EPSG:32606",
            "transform": (20, 0, 0, 0, -20, 20),
            "recipe_id": "source-recipe",
        },
    )


def _context() -> ProcessorContext:
    return ProcessorContext(
        SimpleNamespace(),
        SimpleNamespace(node_id="indices"),
        (),
        logging.getLogger("test-index"),
    )


def _all_band_source() -> xr.Dataset:
    """Return one small reflectance cube containing every index input band."""
    bands = ["B02", "B03", "B04", "B05", "B08", "B11", "B12"]
    values = np.array([0.1, 0.2, 0.3, 0.4, 0.6, 0.7, 0.8], dtype=np.float32)
    return xr.Dataset(
        {
            "reflectance": (
                ("time", "band", "y", "x"),
                values.reshape(1, len(bands), 1, 1),
                {"nodata": 65535},
            )
        },
        coords={
            "time": [np.datetime64("2024-06-01", "ns")],
            "band": bands,
            "y": [0],
            "x": [0],
        },
    )


def test_index_config_normalizes_names_and_rejects_duplicates() -> None:
    assert S2IndexNodeConfig(indices=["ndgi", "NDWI1"]).indices == (
        "NDGI",
        "NDWI1",
    )
    with pytest.raises(ValueError, match="duplicates"):
        S2IndexNodeConfig(indices=["NDGI", "ndgi"])
    with pytest.raises(ValueError, match="unsupported"):
        S2IndexNodeConfig(indices=["NDVI"])


def test_builtin_index_classes_are_registered_with_band_metadata() -> None:
    assert SpectralIndex.registry["NDGI"] is NDGI
    assert SpectralIndex.registry["NDWI1"] is NDWI1
    assert NDGI.bands == ("B03", "B04", "B08")
    assert NDWI1.bands == ("B08", "B11")


def test_new_index_subclass_is_automatically_registered_and_executable() -> None:
    class TESTINDEX(SpectralIndex):
        name = "TESTINDEX"
        bands = ("B03",)

        @classmethod
        def formula(cls, values):
            return values.sel(band="B03") * 2

        @classmethod
        def denominator(cls, values):
            return values.sel(band="B03")

    assert SpectralIndex.registry["TESTINDEX"] is TESTINDEX
    result = S2IndexProcessor().execute(
        S2IndexNodeConfig(indices=["TESTINDEX"]),
        {"data": _source()},
        _context(),
    )
    np.testing.assert_array_equal(
        result.reflectance.sel(band="TESTINDEX").values[:, 0, 0],
        [4, 6],
    )
    del SpectralIndex.registry["TESTINDEX"]


def test_index_subclass_rejects_duplicate_names() -> None:
    with pytest.raises(ValueError, match="duplicate spectral index name"):

        class DUPLICATE(SpectralIndex):
            name = "NDGI"
            bands = ("B03",)

            @classmethod
            def formula(cls, values):
                return values

            @classmethod
            def denominator(cls, values):
                return values


def test_index_appends_float32_bands_for_each_timestep() -> None:
    result = S2IndexProcessor().execute(
        S2IndexNodeConfig(indices=["NDGI", "NDWI1"]),
        {"data": _source()},
        _context(),
    )

    assert result.band.values.tolist() == [
        "B03",
        "B04",
        "B08",
        "B11",
        "B12",
        "NDGI",
        "NDWI1",
    ]
    assert result.reflectance.dtype == np.float32
    assert result.sizes["time"] == 2
    expected_ndgi = (0.62 * 2 + 0.38 * 8 - 4) / (0.62 * 2 + 0.38 * 8 + 4)
    expected_ndwi1 = (8 - 10) / (8 + 10)
    np.testing.assert_allclose(
        result.reflectance.sel(band=["NDGI", "NDWI1"]).values[:, :, 0, 0],
        [
            [expected_ndgi, expected_ndwi1],
            [
                (0.62 * 3 + 0.38 * 9 - 5) / (0.62 * 3 + 0.38 * 9 + 5),
                (9 - 11) / (9 + 11),
            ],
        ],
        rtol=1e-6,
        atol=1e-7,
    )
    assert result.source_fingerprint.values[0] != "source0"


def test_index_masks_nodata_and_zero_denominator() -> None:
    source = _source()
    source["reflectance"].values[0, 2, 0, 0] = 65535
    source["reflectance"].values[1, 2, 0, 0] = 0
    source["reflectance"].values[1, 3, 0, 0] = 0

    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        result = S2IndexProcessor().execute(
            S2IndexNodeConfig(indices=["NDGI", "NDWI1"]),
            {"data": source},
            _context(),
        )

    assert np.isnan(result.reflectance.sel(band="NDGI").values[0, 0, 0])
    assert np.isnan(result.reflectance.sel(band="NDWI1").values[1, 0, 0])


def test_index_preserves_dask_laziness() -> None:
    da = pytest.importorskip("dask.array")
    source = _source()
    source["reflectance"] = source.reflectance.chunk(
        {"time": 1, "band": -1, "y": 1, "x": 1}
    )

    result = S2IndexProcessor().execute(
        S2IndexNodeConfig(indices=["NDGI", "NDWI1"]),
        {"data": source},
        _context(),
    )

    assert isinstance(result.reflectance.data, da.Array)
    assert result.reflectance.chunks[0] == (1, 1)


def test_all_builtin_indices_execute_with_finite_valid_inputs() -> None:
    names = tuple(SpectralIndex.registry)
    result = S2IndexProcessor().execute(
        S2IndexNodeConfig(indices=names),
        {"data": _all_band_source()},
        _context(),
    )

    assert result.band.values.tolist()[-len(names) :] == list(names)
    assert np.isfinite(result.reflectance.sel(band=list(names)).values).all()


def test_scale_sensitive_indices_use_quantification_value() -> None:
    source = _all_band_source() * 10_000
    source["reflectance"].attrs["nodata"] = 65535
    source = source.assign_coords(quantification_value=("time", [10_000]))

    result = S2IndexProcessor().execute(
        S2IndexNodeConfig(indices=["OSAVI", "MSAVI"]),
        {"data": source},
        _context(),
    )
    reference = S2IndexProcessor().execute(
        S2IndexNodeConfig(indices=["OSAVI", "MSAVI"]),
        {"data": _all_band_source()},
        _context(),
    )

    np.testing.assert_allclose(
        result.reflectance.sel(band=["OSAVI", "MSAVI"]),
        reference.reflectance.sel(band=["OSAVI", "MSAVI"]),
    )

    source["reflectance"].loc[{"band": "B08"}] = 65535
    masked = S2IndexProcessor().execute(
        S2IndexNodeConfig(indices=["OSAVI"]),
        {"data": source},
        _context(),
    )
    assert np.isnan(masked.reflectance.sel(band="OSAVI")).all()


def test_square_root_indices_mask_invalid_domains_without_warnings() -> None:
    source = _all_band_source()
    source["reflectance"].loc[{"band": "B04"}] = -10

    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        result = S2IndexProcessor().execute(
            S2IndexNodeConfig(indices=["MSAVI", "MTVI2", "TCI"]),
            {"data": source},
            _context(),
        )

    assert np.isnan(result.reflectance.sel(band=["MSAVI", "MTVI2", "TCI"])).all()


def test_point_indices_use_the_same_safe_formula_contract() -> None:
    rows = pd.DataFrame(
        [
            {
                "B03": 0.0,
                "B04": 0.0,
                "B05": 0.0,
                "B08": 0.0,
                "quantification_value": 10_000,
            },
            {
                "B03": 2_000,
                "B04": 3_000,
                "B05": 4_000,
                "B08": 65535,
                "quantification_value": 10_000,
            },
        ]
    )
    source = PointSampleArtifact(rows, SimpleNamespace())

    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        result = S2IndexProcessor().execute_points(
            S2IndexNodeConfig(indices=["GNDVI", "TCARI", "TCI"]),
            {"data": source},
            _context(),
        )

    assert result.rows.loc[0, ["GNDVI", "TCARI", "TCI"]].isna().all()
    assert np.isnan(result.rows.loc[1, "GNDVI"])
    assert np.isfinite(result.rows.loc[1, ["TCARI", "TCI"]]).all()


def test_index_rejects_missing_band_and_invalid_input_count() -> None:
    processor = S2IndexProcessor()
    with pytest.raises(ValueError, match="exactly one"):
        processor.execute(S2IndexNodeConfig(indices=["NDGI"]), {}, _context())
    with pytest.raises(ValueError, match="missing bands"):
        processor.execute(
            S2IndexNodeConfig(indices=["NDWI1"]),
            {"data": _source().drop_sel(band="B11")},
            _context(),
        )


def test_index_is_registered_as_a_builtin() -> None:
    assert get_default_registry().get("s2.index").type_name == "s2.index"


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
