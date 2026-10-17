"""The ``pavc_gridded_rf`` processor: fit at plots, predict lazily on AOI boxes."""

from __future__ import annotations

import logging
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import numpy as np
import pandas as pd
import pytest
from affine import Affine
from pydantic import ValidationError
from pyproj import Transformer

pytest.importorskip("sklearn")
dask = pytest.importorskip("dask")
da = pytest.importorskip("dask.array")
xr = pytest.importorskip("xarray")

from sentinel_py.enums import AOIKind  # noqa: E402
from sentinel_py.pipeline.artifacts import RasterArtifact  # noqa: E402
from sentinel_py.pipeline.processors.base import (  # noqa: E402
    ProcessorContext,
    get_default_registry,
)
from sentinel_py.pipeline.processors.pavc_gridded_rf.modeling import (  # noqa: E402
    PavcGriddedRFNodeConfig,
    PavcGriddedRFProcessor,
)
from sentinel_py.pipeline.processors.pavc_gridded_rf.training import (  # noqa: E402
    point_window,
)

BOXES = 8
SIZE = 4
BANDS = ["b0", "b1", "b2"]


def _box_transform(index: int) -> tuple[float, ...]:
    """Affine tuple of box ``index``, 10 m pixels in UTM zone 6N."""
    return (10.0, 0.0, 500000.0 + 1000.0 * index, 0.0, -10.0, 7000000.0)


def _cube(index: int, values: np.ndarray) -> Any:
    """A one-time-step feature cube with constant per-band values."""
    data = np.broadcast_to(values[:, None, None], (len(BANDS), SIZE, SIZE)).copy()
    return xr.Dataset(
        {
            "reflectance": (
                ("time", "band", "y", "x"),
                da.from_array(data[None].astype(np.float32), chunks=(1, 3, 2, 2)),
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


def _artifact(*, nan_box: int | None = None) -> RasterArtifact:
    """Eight boxes whose features follow the box index."""
    datasets = {}
    units = []
    for index in range(BOXES):
        values = np.array([index, 2.0 * index, (index * 7) % 5], dtype=np.float32)
        cube = _cube(index, values)
        if index == nan_box:
            cube["reflectance"][0, 0, 0, 0] = np.nan
        key = (f"site-{index}", "part-0000")
        datasets[key] = cube
        units.append(SimpleNamespace(aoi_id=key[0], component_id=key[1]))
    plan = SimpleNamespace(kind=AOIKind.polygon, units=tuple(units))
    return RasterArtifact(plan, datasets)


def _write_tables(tmp_path: Path) -> None:
    """Training and PFT CSVs whose plots sit at the centre of each box."""
    to_lonlat = Transformer.from_crs("EPSG:32606", "EPSG:4326", always_xy=True)
    rows, cover = [], []
    for index in range(BOXES):
        x0, y0 = _box_transform(index)[2], _box_transform(index)[5]
        lon, lat = to_lonlat.transform(x0 + 20.0, y0 - 20.0)
        site = f"site-{index}"
        rows.append({"site_visit_id": site, "latitude": lat, "longitude": lon})
        cover.append(
            {
                "site_visit_id": site,
                "graminoid": 10.0 * index,
                "water": 100.0 - 10.0 * index,
                "qa_review_flag": "no",
            }
        )
    pd.DataFrame(rows).to_csv(tmp_path / "training.csv", index=False)
    pd.DataFrame(cover).to_csv(tmp_path / "cover.csv", index=False)


def _config(**overrides: Any) -> PavcGriddedRFNodeConfig:
    """Node configuration for the synthetic project."""
    values: dict[str, Any] = {
        "training": "training.csv",
        "pft_table": "cover.csv",
        "model_dir": "models",
        "pfts": ["graminoid", "water"],
        "n_estimators": 20,
        "max_depth": None,
        "max_features": 2,
    }
    return PavcGriddedRFNodeConfig.model_validate({**values, **overrides})


def _context(tmp_path: Path, kind: AOIKind = AOIKind.polygon) -> ProcessorContext:
    """Processor context for a pipeline file in ``tmp_path``."""
    pipeline = SimpleNamespace(
        path=tmp_path / "pipeline.yaml",
        aoi_plan=SimpleNamespace(kind=kind),
        execution=None,
    )
    return ProcessorContext(
        pipeline, SimpleNamespace(node_id="rf"), (), logging.getLogger("rf-test")
    )


@pytest.fixture(autouse=True)
def synchronous_sampling(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """Compute sampling tasks in-process and count the compute calls."""
    calls: list[int] = []

    def compute(tasks: Any, execution: Any, *, progress_callback: Any = None) -> Any:
        """Stand-in for the Dask backend."""
        calls.append(len(tasks))
        results = dask.compute(*tasks, scheduler="synchronous")
        for result in results:
            if progress_callback is not None:
                progress_callback(result)
        return tuple(results)

    monkeypatch.setattr("sentinel_py.pipeline.execution.compute_dask_tasks", compute)
    return calls


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
        ({"sample_radius_m": -1.0}, "sample_radius_m"),
        ({"cook": True}, "cook"),
    ],
)
def test_config_rejections(overrides: dict[str, Any], message: str) -> None:
    with pytest.raises(ValidationError, match=message):
        _config(**overrides)


def test_config_accepts_yaml_style_numbers() -> None:
    config = _config(sample_radius_m=30, max_features="sqrt", max_depth=50)

    assert config.sample_radius_m == 30.0
    assert config.pfts == ("graminoid", "water")


def test_validate_requires_exactly_one_input() -> None:
    processor = PavcGriddedRFProcessor()
    kwargs = {"sources": {}, "outputs": (), "output_grid": None}

    processor.validate(_config(), node_id="rf", inputs={"x": "join"}, **kwargs)
    with pytest.raises(ValueError, match="exactly one upstream input"):
        processor.validate(_config(), node_id="rf", inputs={}, **kwargs)


########################################################################################
# Sampling window
########################################################################################


def test_point_window_selects_pixel_neighbourhood_and_rejects_outside() -> None:
    affine = Affine(10.0, 0.0, 0.0, 0.0, -10.0, 0.0)

    rows, cols, mask = point_window(affine, (4, 4), 25.0, -25.0, 0.0)
    assert (rows, cols) == (slice(2, 3), slice(2, 3))
    assert mask.tolist() == [[True]]

    rows, cols, mask = point_window(affine, (4, 4), 25.0, -25.0, 10.0)
    assert (rows, cols) == (slice(1, 4), slice(1, 4))
    assert mask.sum() == 5  # centre plus its four neighbours at 10 m

    assert point_window(affine, (4, 4), 45.0, -25.0, 0.0) is None


########################################################################################
# Fit and predict
########################################################################################


def test_fit_and_predict_returns_rounded_uint8_cover(tmp_path: Path) -> None:
    _write_tables(tmp_path)
    source = _artifact(nan_box=3)

    result = PavcGriddedRFProcessor().execute(
        _config(), {"features": source}, _context(tmp_path)
    )

    cube = result.datasets[("site-6", "part-0000")]
    assert cube["cover"].dtype == np.uint8
    assert cube.band.values.tolist() == ["graminoid", "water"]
    assert cube.attrs["nodata"] == 255
    assert cube["cover"].attrs["nodata"] == 255
    values = cube["cover"].values
    assert values.shape == (1, 2, SIZE, SIZE)
    assert values.max() <= 100
    # Box 6 holds graminoid 60 and water 40; the forest lands near them (bootstrap
    # sampling keeps it from being exact).
    assert abs(int(values[0, 0, 0, 0]) - 60) <= 15
    assert abs(int(values[0, 1, 0, 0]) - 40) <= 15
    # One NaN feature pixel is nodata in every band; its neighbours are not.
    nan_cube = result.datasets[("site-3", "part-0000")]["cover"].values
    assert nan_cube[0, :, 0, 0].tolist() == [255, 255]
    assert nan_cube[0, 0, 0, 1] != 255
    manifest = list((tmp_path / "models").glob("*/manifest.json"))
    assert len(manifest) == 1
    assert {path.name for path in manifest[0].parent.iterdir()} == {
        "manifest.json",
        "graminoid.joblib",
        "water.joblib",
    }


def test_second_run_reuses_models_without_sampling(
    tmp_path: Path, synchronous_sampling: list[int], caplog: pytest.LogCaptureFixture
) -> None:
    _write_tables(tmp_path)
    processor = PavcGriddedRFProcessor()
    first = processor.execute(_config(), {"f": _artifact()}, _context(tmp_path))
    assert synchronous_sampling == [BOXES]

    with caplog.at_level(logging.INFO, logger="rf-test"):
        second = processor.execute(_config(), {"f": _artifact()}, _context(tmp_path))

    assert synchronous_sampling == [BOXES]
    assert "Reusing PFT models" in caplog.text
    key = ("site-5", "part-0000")
    assert first.datasets[key].attrs["recipe_id"] == second.datasets[key].attrs["recipe_id"]


def test_recipe_id_changes_with_every_result_affecting_input(tmp_path: Path) -> None:
    _write_tables(tmp_path)
    processor = PavcGriddedRFProcessor()
    key = ("site-0", "part-0000")

    def recipe(config: PavcGriddedRFNodeConfig, source: RasterArtifact) -> str:
        """Recipe ID produced for one configuration and input."""
        result = processor.execute(config, {"f": source}, _context(tmp_path))
        return result.datasets[key].attrs["recipe_id"]

    base = recipe(_config(), _artifact())
    assert recipe(_config(), _artifact()) == base
    assert recipe(_config(n_estimators=21), _artifact()) != base
    assert recipe(_config(seed=1), _artifact()) != base
    assert recipe(_config(drop_qa_flagged=False), _artifact()) != base
    assert recipe(_config(pfts=["graminoid"]), _artifact()) != base
    changed = _artifact()
    changed.datasets[key].coords["source_fingerprint"] = ("time", ["new"])
    assert recipe(_config(), changed) != base
    cover = pd.read_csv(tmp_path / "cover.csv")
    cover.loc[0, "graminoid"] = 1.0
    cover.to_csv(tmp_path / "cover.csv", index=False)
    assert recipe(_config(), _artifact()) != base


def test_prediction_graph_is_lazy_and_uses_model_paths_only(tmp_path: Path) -> None:
    _write_tables(tmp_path)

    result = PavcGriddedRFProcessor().execute(
        _config(), {"f": _artifact()}, _context(tmp_path)
    )

    data = result.datasets[("site-1", "part-0000")]["cover"].data
    assert hasattr(data, "dask")
    assert data.chunks[2] == (2, 2)
    layers = " ".join(data.__dask_graph__().layers)
    assert "_predict_block" in layers


def test_cover_cube_is_written_as_uint8_cog_tiles(tmp_path: Path) -> None:
    rasterio = pytest.importorskip("rasterio")
    from sentinel_py.pipeline.writers.cog import COGOutputWriter

    _write_tables(tmp_path)
    result = PavcGriddedRFProcessor().execute(
        _config(), {"f": _artifact(nan_box=2)}, _context(tmp_path)
    )
    out = tmp_path / "out"
    output = SimpleNamespace(output_id="cover", path=out)
    plan = COGOutputWriter().build(result.datasets[("site-2", "part-0000")], output, None)

    dask.compute(*plan.tasks, scheduler="single-threaded")

    tile = out / "p" / "g" / "cover.r0000.c0000.tif"
    with rasterio.open(tile) as dataset:
        assert dataset.dtypes == ("uint8", "uint8")
        assert dataset.nodata == 255
        assert dataset.descriptions == ("graminoid", "water")
        assert dataset.read(1)[0, 0] == 255


########################################################################################
# Failures
########################################################################################


def test_point_aoi_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="requires a polygon AOI"):
        PavcGriddedRFProcessor().execute(
            _config(), {"f": _artifact()}, _context(tmp_path, AOIKind.point)
        )


def test_missing_training_file_is_reported(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="training file not found"):
        PavcGriddedRFProcessor().execute(
            _config(), {"f": _artifact()}, _context(tmp_path)
        )


def test_multiple_time_steps_are_rejected(tmp_path: Path) -> None:
    _write_tables(tmp_path)
    source = _artifact()
    key = ("site-0", "part-0000")
    source.datasets[key] = xr.concat(
        [source.datasets[key], source.datasets[key]], dim="time"
    )

    with pytest.raises(ValueError, match="one time step per AOI unit"):
        PavcGriddedRFProcessor().execute(_config(), {"f": source}, _context(tmp_path))


def test_max_features_above_feature_count_is_rejected(tmp_path: Path) -> None:
    _write_tables(tmp_path)

    with pytest.raises(ValueError, match="max_features=9 exceeds the 3 input"):
        PavcGriddedRFProcessor().execute(
            _config(max_features=9), {"f": _artifact()}, _context(tmp_path)
        )


def test_plots_outside_every_box_are_skipped_with_a_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    _write_tables(tmp_path)
    source = _artifact()
    del source.datasets[("site-7", "part-0000")]

    with caplog.at_level(logging.WARNING, logger="rf-test"):
        PavcGriddedRFProcessor().execute(_config(), {"f": source}, _context(tmp_path))

    assert "1 of 8 training plots fall outside every AOI box" in caplog.text


def test_too_few_complete_plots_names_the_sampled_count(tmp_path: Path) -> None:
    _write_tables(tmp_path)
    source = _artifact()
    for index in range(1, BOXES):  # every box but one has an all-NaN feature
        source.datasets[(f"site-{index}", "part-0000")]["reflectance"].loc[
            {"band": "b0"}
        ] = np.nan

    with pytest.raises(ValueError, match="1 plot\\(s\\) with complete feature values out of 8"):
        PavcGriddedRFProcessor().execute(_config(), {"f": source}, _context(tmp_path))
