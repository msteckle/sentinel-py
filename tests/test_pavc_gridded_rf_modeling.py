"""The ``pavc_gridded_rf`` processor: fit from point rows, predict lazily on grids."""

from __future__ import annotations

import logging
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import numpy as np
import pandas as pd
import pytest
from pydantic import ValidationError

pytest.importorskip("sklearn")
dask = pytest.importorskip("dask")
da = pytest.importorskip("dask.array")
xr = pytest.importorskip("xarray")

from sentinel_py.enums import AOIKind  # noqa: E402
from sentinel_py.pipeline.artifacts import (  # noqa: E402
    PointSampleArtifact,
    RasterArtifact,
)
from sentinel_py.pipeline.processors.base import (  # noqa: E402
    ProcessorContext,
    get_default_registry,
)
from sentinel_py.pipeline.processors.pavc_gridded_rf.modeling import (  # noqa: E402
    PavcGriddedRFNodeConfig,
    PavcGriddedRFProcessor,
)

PLOTS = 8
SIZE = 4
BANDS = ["b0", "b1", "b2"]


def _features(index: int) -> np.ndarray:
    """Feature vector of plot ``index``, shared by its point row and its box cube."""
    return np.array([index, 2.0 * index, (index * 7) % 5], dtype=np.float32)


########################################################################################
# Builders
########################################################################################


def _rows(*, with_attributes: bool = True) -> pd.DataFrame:
    """Joined point rows: identity columns, three features, S1 attribute columns."""
    records = []
    for index in range(PLOTS):
        record: dict[str, Any] = {
            "aoi_id": f"site-{index}",
            "component_id": f"point-{index}",
            "point_index": index,
            "longitude": -150.0 + index,
            "latitude": 70.0,
            "time": "2000-06-01",
            "product_id": "p",
            "granule_id": "g",
            "time_representation": "static",
            "source_fingerprint": f"fp{index}",
            **dict(zip(BANDS, _features(index).tolist(), strict=True)),
        }
        if with_attributes:
            record.update(flight_direction="ASCENDING", relative_orbit=7, units="dB")
        records.append(record)
    return pd.DataFrame.from_records(records)


def _point_artifact(rows: pd.DataFrame | None = None) -> PointSampleArtifact:
    """Point artifact of the eight plots."""
    plan = SimpleNamespace(kind=AOIKind.point)
    return PointSampleArtifact(_rows() if rows is None else rows, plan)


def _write_cover(tmp_path: Path) -> None:
    """PFT table: graminoid rises with the plot index, water falls."""
    pd.DataFrame(
        {
            "site_visit_id": [f"site-{index}" for index in range(PLOTS)],
            "graminoid": [10.0 * index for index in range(PLOTS)],
            "water": [100.0 - 10.0 * index for index in range(PLOTS)],
            "qa_review_flag": ["no"] * PLOTS,
        }
    ).to_csv(tmp_path / "cover.csv", index=False)


def _config(**overrides: Any) -> PavcGriddedRFNodeConfig:
    """Fit configuration for the synthetic project."""
    values: dict[str, Any] = {
        "pft_table": "cover.csv",
        "model_dir": "models",
        "pfts": ["graminoid", "water"],
        "n_estimators": 20,
        "max_depth": None,
        "max_features": 2,
    }
    return PavcGriddedRFNodeConfig.model_validate({**values, **overrides})


def _context(tmp_path: Path, kind: AOIKind = AOIKind.point) -> ProcessorContext:
    """Processor context for a pipeline file in ``tmp_path``."""
    pipeline = SimpleNamespace(
        path=tmp_path / "pipeline.yaml", aoi_plan=SimpleNamespace(kind=kind)
    )
    return ProcessorContext(
        pipeline, SimpleNamespace(node_id="rf"), (), logging.getLogger("rf-test")
    )


def _fit(tmp_path: Path, rows: pd.DataFrame | None = None, **overrides: Any) -> Any:
    """Run the fit step and return its artifact."""
    return PavcGriddedRFProcessor().execute(
        _config(**overrides), {"f": _point_artifact(rows)}, _context(tmp_path)
    )


def _box_transform(index: int) -> tuple[float, ...]:
    """Affine tuple of box ``index``, 10 m pixels in UTM zone 6N."""
    return (10.0, 0.0, 500000.0 + 1000.0 * index, 0.0, -10.0, 7000000.0)


def _raster_artifact(*, nan_box: int | None = None) -> RasterArtifact:
    """Eight box cubes whose constant features equal the plots' feature rows."""
    datasets, units = {}, []
    for index in range(PLOTS):
        data = np.broadcast_to(
            _features(index)[:, None, None], (len(BANDS), SIZE, SIZE)
        ).copy()
        cube = xr.Dataset(
            {
                "reflectance": (
                    ("time", "band", "y", "x"),
                    da.from_array(data[None], chunks=(1, 3, 2, 2)),
                )
            },
            coords={
                "time": np.array(["1970-01-01"], dtype="datetime64[ns]"),
                "band": BANDS,
                "y": np.arange(SIZE),
                "x": np.arange(SIZE),
                "product_id": ("time", ["p"]),
                "granule_id": ("time", ["g"]),
                "source_fingerprint": ("time", [f"fp{index}"]),
            },
            attrs={
                "crs": "EPSG:32606",
                "transform": _box_transform(index),
                "recipe_id": "upstream",
                "time_representation": "static",
            },
        )
        if index == nan_box:
            cube["reflectance"][0, 0, 0, 0] = np.nan
        key = (f"site-{index}", "part-0000")
        datasets[key] = cube
        units.append(SimpleNamespace(aoi_id=key[0], component_id=key[1]))
    plan = SimpleNamespace(kind=AOIKind.polygon, units=tuple(units))
    return RasterArtifact(plan, datasets)


def _predict_config(tmp_path: Path, **overrides: Any) -> PavcGriddedRFNodeConfig:
    """Predict-mode configuration pointing at the one fitted model directory."""
    (fitted,) = (tmp_path / "models").iterdir()
    return PavcGriddedRFNodeConfig.model_validate(
        {"models": f"models/{fitted.name}", **overrides}
    )


def _predict(
    tmp_path: Path, source: RasterArtifact | None = None, **overrides: Any
) -> Any:
    """Predict on the box cubes with the fitted models."""
    return PavcGriddedRFProcessor().execute(
        _predict_config(tmp_path, **overrides),
        {"f": source or _raster_artifact()},
        _context(tmp_path, AOIKind.polygon),
    )


########################################################################################
# Configuration
########################################################################################


def test_processor_is_registered() -> None:
    assert isinstance(
        get_default_registry().get("pavc_gridded_rf"), PavcGriddedRFProcessor
    )


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"pfts": []}, "non-empty list"),
        ({"pfts": ["graminoid", "moss"]}, "unknown PFT\\(s\\): moss"),
        ({"pfts": ["forb", "forb"]}, "must not repeat"),
        ({"missing_cover": "ignore"}, "missing_cover"),
        ({"n_estimators": 0}, "n_estimators"),
        ({"cook": True}, "cook"),
        ({"training": "t.csv"}, "training"),
        ({"sample_radius_m": 30}, "sample_radius_m"),
        ({"pft_table": None}, "give pft_table to fit them"),
        ({"models": "m"}, "fit keys are not allowed: .*pft_table"),
    ],
)
def test_config_rejections(overrides: dict[str, Any], message: str) -> None:
    with pytest.raises(ValidationError, match=message):
        _config(**overrides)


def test_predict_config_needs_only_models() -> None:
    config = PavcGriddedRFNodeConfig.model_validate({"models": "m"})
    assert config.models == "m" and config.pfts is None
    PavcGriddedRFNodeConfig.model_validate({"models": "m", "pfts": ["water"]})
    with pytest.raises(ValidationError, match="not allowed: n_estimators"):
        PavcGriddedRFNodeConfig.model_validate({"models": "m", "n_estimators": 5})


def test_config_accepts_yaml_style_values() -> None:
    config = _config(max_features="sqrt", max_depth=50)

    assert config.pfts == ("graminoid", "water")


def test_validate_requires_exactly_one_input() -> None:
    processor = PavcGriddedRFProcessor()
    kwargs = {"sources": {}, "outputs": (), "output_grid": None}

    processor.validate(_config(), node_id="rf", inputs={"x": "join"}, **kwargs)
    with pytest.raises(ValueError, match="exactly one upstream input"):
        processor.validate(_config(), node_id="rf", inputs={}, **kwargs)


########################################################################################
# Fitting from point rows
########################################################################################


def test_fit_saves_models_and_returns_per_plot_oob_rows(tmp_path: Path) -> None:
    _write_cover(tmp_path)

    result = _fit(tmp_path)

    assert isinstance(result, PointSampleArtifact)
    rows = result.rows
    assert len(rows) == PLOTS
    assert {"aoi_id", "longitude", "latitude"} <= set(rows.columns)
    assert list(rows.columns[-4:]) == [
        "graminoid_observed",
        "graminoid_oob",
        "water_observed",
        "water_oob",
    ]
    assert str(rows["graminoid_oob"].dtype) == "Int16"
    assert rows["graminoid_oob"].between(0, 100).all()
    (fitted,) = (tmp_path / "models").iterdir()
    assert {path.name for path in fitted.iterdir()} == {
        "manifest.json",
        "training_features.parquet",
        "oob_predictions.parquet",
        "graminoid.joblib",
        "water.joblib",
    }
    manifest = pd.read_json(fitted / "manifest.json", typ="series")
    # Only numeric feature columns: no identity, time or S1 attribute columns.
    assert manifest["features"] == BANDS
    assert manifest["pfts"] == ["graminoid", "water"]


def test_sentinel2_scene_attribute_is_not_a_feature(tmp_path: Path) -> None:
    _write_cover(tmp_path)
    rows = _rows().assign(quantification_value=10000)

    _fit(tmp_path, rows)

    (fitted,) = (tmp_path / "models").iterdir()
    manifest = pd.read_json(fitted / "manifest.json", typ="series")
    assert manifest["features"] == BANDS


def test_second_fit_reuses_models(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    _write_cover(tmp_path)
    first = _fit(tmp_path)

    with caplog.at_level(logging.INFO, logger="rf-test"):
        second = _fit(tmp_path)

    assert "Reusing PFT models" in caplog.text
    pd.testing.assert_frame_equal(first.rows, second.rows)


def test_fit_recipe_follows_every_result_affecting_input(tmp_path: Path) -> None:
    _write_cover(tmp_path)

    def recipes() -> set[str]:
        """Names of the recipe folders created so far."""
        return {path.name for path in (tmp_path / "models").iterdir()}

    _fit(tmp_path)
    assert len(recipes()) == 1
    _fit(tmp_path)
    assert len(recipes()) == 1
    _fit(tmp_path, n_estimators=21)
    _fit(tmp_path, seed=1)
    _fit(tmp_path, drop_qa_flagged=False)
    _fit(tmp_path, pfts=["graminoid"])
    changed = _rows()
    changed.loc[0, "b0"] = 99.0
    _fit(tmp_path, changed)
    cover = pd.read_csv(tmp_path / "cover.csv")
    cover.loc[0, "graminoid"] = 1.0
    cover.to_csv(tmp_path / "cover.csv", index=False)
    _fit(tmp_path)
    assert len(recipes()) == 7


def test_qa_flagged_plots_are_excluded_from_training(tmp_path: Path) -> None:
    _write_cover(tmp_path)
    cover = pd.read_csv(tmp_path / "cover.csv")
    cover.loc[3, "qa_review_flag"] = "yes"
    cover.to_csv(tmp_path / "cover.csv", index=False)

    rows = _fit(tmp_path).rows

    flagged = rows[rows["aoi_id"] == "site-3"].iloc[0]
    assert pd.isna(flagged["graminoid_observed"]) and pd.isna(flagged["graminoid_oob"])
    assert rows["graminoid_observed"].notna().sum() == PLOTS - 1


def test_missing_features_drop_plots_with_a_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    _write_cover(tmp_path)
    rows = _rows()
    rows.loc[2, "b1"] = np.nan

    with caplog.at_level(logging.WARNING, logger="rf-test"):
        result = _fit(tmp_path, rows).rows

    assert "1 plot(s) have missing features (for example site-2)" in caplog.text
    assert pd.isna(result.loc[result["aoi_id"] == "site-2", "water_oob"]).all()


def test_too_few_complete_plots_names_the_sampled_count(tmp_path: Path) -> None:
    _write_cover(tmp_path)
    rows = _rows()
    rows.loc[1:, "b0"] = np.nan

    with pytest.raises(
        ValueError, match="1 plot\\(s\\) with complete feature values out of 8"
    ):
        _fit(tmp_path, rows)


def test_several_rows_per_plot_are_rejected(tmp_path: Path) -> None:
    _write_cover(tmp_path)
    rows = pd.concat([_rows(), _rows().assign(time="2000-07-01")])

    with pytest.raises(ValueError, match="needs one row per plot"):
        _fit(tmp_path, rows)


def test_rows_without_numeric_features_are_rejected(tmp_path: Path) -> None:
    _write_cover(tmp_path)

    with pytest.raises(ValueError, match="no numeric feature columns"):
        _fit(tmp_path, _rows().drop(columns=BANDS))


def test_missing_pft_table_and_oversized_max_features_are_reported(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="pft_table file not found"):
        _fit(tmp_path)

    _write_cover(tmp_path)
    with pytest.raises(ValueError, match="max_features=9 exceeds the 3 input"):
        _fit(tmp_path, max_features=9)


def test_fit_needs_a_point_aoi_and_predict_needs_a_polygon_aoi(tmp_path: Path) -> None:
    _write_cover(tmp_path)
    processor = PavcGriddedRFProcessor()

    with pytest.raises(ValueError, match="fits models in a point AOI pipeline"):
        processor.execute(
            _config(), {"f": _raster_artifact()}, _context(tmp_path, AOIKind.polygon)
        )

    _fit(tmp_path)
    with pytest.raises(ValueError, match="predicts with saved models on a polygon AOI"):
        processor.execute(
            _predict_config(tmp_path), {"f": _point_artifact()}, _context(tmp_path)
        )


########################################################################################
# Predicting with saved models
########################################################################################


def test_predict_returns_rounded_uint8_cover(tmp_path: Path) -> None:
    _write_cover(tmp_path)
    _fit(tmp_path)
    (tmp_path / "cover.csv").unlink()  # prediction needs only the saved models

    result = _predict(tmp_path, _raster_artifact(nan_box=3))

    cube = result.datasets[("site-6", "part-0000")]
    assert cube["cover"].dtype == np.uint8
    assert cube.band.values.tolist() == ["graminoid", "water"]
    assert cube.attrs["nodata"] == 255 and cube["cover"].attrs["nodata"] == 255
    values = cube["cover"].values
    assert values.shape == (1, 2, SIZE, SIZE)
    assert values.max() <= 100
    # Plot 6 holds graminoid 60 and water 40; the forest lands near them (bootstrap
    # sampling keeps it from being exact).
    assert abs(int(values[0, 0, 0, 0]) - 60) <= 15
    assert abs(int(values[0, 1, 0, 0]) - 40) <= 15
    # One NaN feature pixel is nodata in every band; its neighbours are not.
    nan_cube = result.datasets[("site-3", "part-0000")]["cover"].values
    assert nan_cube[0, :, 0, 0].tolist() == [255, 255]
    assert nan_cube[0, 0, 0, 1] != 255


def test_predict_graph_is_lazy_and_carries_model_paths_only(tmp_path: Path) -> None:
    _write_cover(tmp_path)
    _fit(tmp_path)

    data = _predict(tmp_path).datasets[("site-1", "part-0000")]["cover"].data

    assert hasattr(data, "dask")
    assert data.chunks[2] == (2, 2)
    assert "_predict_block" in " ".join(data.__dask_graph__().layers)


def test_predict_can_select_a_subset_of_pfts(tmp_path: Path) -> None:
    _write_cover(tmp_path)
    _fit(tmp_path)

    cube = _predict(tmp_path, pfts=["water"]).datasets[("site-1", "part-0000")]

    assert cube.band.values.tolist() == ["water"]
    assert cube["cover"].values.shape == (1, 1, SIZE, SIZE)


def test_predict_recipe_follows_model_file_content(tmp_path: Path) -> None:
    _write_cover(tmp_path)
    _fit(tmp_path)
    key = ("site-0", "part-0000")

    def recipe() -> str:
        """Recipe ID of a prediction with the current model files."""
        return _predict(tmp_path).datasets[key].attrs["recipe_id"]

    before = recipe()
    assert recipe() == before
    (fitted,) = (tmp_path / "models").iterdir()
    with (fitted / "water.joblib").open("ab") as handle:
        handle.write(b"x")
    assert recipe() != before


def test_predict_failures(tmp_path: Path) -> None:
    _write_cover(tmp_path)
    processor = PavcGriddedRFProcessor()
    context = _context(tmp_path, AOIKind.polygon)

    with pytest.raises(ValueError, match="models directory has no manifest.json"):
        processor.execute(
            PavcGriddedRFNodeConfig.model_validate({"models": "nowhere"}),
            {"f": _raster_artifact()},
            context,
        )

    _fit(tmp_path)
    with pytest.raises(
        ValueError, match="do not include: forb; available: graminoid, water"
    ):
        _predict(tmp_path, pfts=["forb"])

    renamed = _raster_artifact()
    for key, dataset in renamed.datasets.items():
        renamed.datasets[key] = dataset.assign_coords(band=["b0", "b1", "other"])
    with pytest.raises(ValueError, match="first differ at position 2"):
        _predict(tmp_path, renamed)

    multi = _raster_artifact()
    key = ("site-0", "part-0000")
    multi.datasets[key] = xr.concat([multi.datasets[key]] * 2, dim="time")
    with pytest.raises(ValueError, match="one time step per AOI unit"):
        _predict(tmp_path, multi)

    (fitted,) = (tmp_path / "models").iterdir()
    (fitted / "graminoid.joblib").unlink()
    with pytest.raises(ValueError, match="model file\\(s\\) missing"):
        _predict(tmp_path)


def test_cover_cube_is_written_as_uint8_cog_tiles(tmp_path: Path) -> None:
    rasterio = pytest.importorskip("rasterio")
    from sentinel_py.pipeline.writers.cog import COGOutputWriter

    _write_cover(tmp_path)
    _fit(tmp_path)
    result = _predict(tmp_path, _raster_artifact(nan_box=2))
    out = tmp_path / "out"
    output = SimpleNamespace(output_id="cover", path=out)
    plan = COGOutputWriter().build(
        result.datasets[("site-2", "part-0000")], output, None
    )

    dask.compute(*plan.tasks, scheduler="single-threaded")

    with rasterio.open(out / "p" / "g" / "cover.r0000.c0000.tif") as dataset:
        assert dataset.dtypes == ("uint8", "uint8")
        assert dataset.nodata == 255
        assert dataset.descriptions == ("graminoid", "water")
        assert dataset.read(1)[0, 0] == 255
