"""The ``join`` processor merges point samples of several nodes into one table."""

from __future__ import annotations

import logging
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from sentinel_py.pipeline import PipelineConfig
from sentinel_py.pipeline.artifacts import PointSampleArtifact, RasterArtifact
from sentinel_py.pipeline.processors.base import ProcessorContext, get_default_registry
from sentinel_py.pipeline.processors.join import JoinNodeConfig, JoinProcessor, join_point_rows
from sentinel_py.pipeline.run import run_pipeline
from test_dem import _write_dem_project


def _rows(values: dict[str, list], *, representation: str, times: list[str]) -> pd.DataFrame:
    """Point rows for the points ``p-0`` and ``p-1`` of site ``s``, one per time."""
    count = len(times) * 2
    return pd.DataFrame(
        {
            "aoi_id": ["s"] * count,
            "component_id": [f"point-{index % 2}" for index in range(count)],
            "point_index": [index % 2 for index in range(count)],
            "longitude": [0.0] * count,
            "latitude": [0.0] * count,
            "time": [time for time in times for _ in range(2)],
            "product_id": ["p"] * count,
            "granule_id": ["g"] * count,
            "time_representation": [representation] * count,
            "source_fingerprint": ["f"] * count,
            **values,
        }
    )


def test_static_inputs_broadcast_and_seasonal_inputs_match_on_time() -> None:
    spectral = _rows({"B02": [1, 2, 3, 4]}, representation="composite", times=["06-01", "07-01"])
    terrain = _rows({"slope": [10.0, 20.0]}, representation="static", times=["1970-01-01"])
    radar = _rows({"VV": [-5.0, -6.0]}, representation="composite", times=["07-01"])

    joined = join_point_rows(spectral, {"terrain": terrain, "radar": radar})

    assert joined["B02"].tolist() == [1, 2, 3, 4]  # the base rows keep their order
    assert joined["slope"].tolist() == [10.0, 20.0, 10.0, 20.0]
    # The radar input only has the 07-01 bin, so the 06-01 rows are missing.
    assert np.isnan(joined["VV"].iloc[:2]).all()
    assert joined["VV"].iloc[2:].tolist() == [-5.0, -6.0]
    assert joined["time"].tolist() == ["06-01", "06-01", "07-01", "07-01"]


def test_join_rejects_ambiguous_rows_and_repeated_columns() -> None:
    base = _rows({"B02": [1, 2]}, representation="composite", times=["all"])
    seasonal = _rows({"VV": [1, 2, 3, 4]}, representation="static", times=["a", "b"])
    with pytest.raises(ValueError, match="several rows per aoi_id and component_id"):
        join_point_rows(base, {"radar": seasonal})
    clash = _rows({"B02": [3, 4]}, representation="composite", times=["all"])
    with pytest.raises(ValueError, match="repeats column.*B02"):
        join_point_rows(base, {"other": clash})


def test_join_validation_and_encoding_rejection() -> None:
    processor = get_default_registry().get("join")
    assert isinstance(processor, JoinProcessor)
    kwargs = {"node_id": "n", "sources": {}, "outputs": (), "output_grid": None}
    with pytest.raises(ValueError, match="at least two"):
        processor.validate(JoinNodeConfig(), inputs={"a": "x"}, **kwargs)
    processor.validate(JoinNodeConfig(), inputs={"a": "x", "b": "y"}, **kwargs)
    packed = SimpleNamespace(
        output_id="samples", format="cog", encoding=SimpleNamespace(dtype="int16")
    )
    with pytest.raises(ValueError, match="cannot set encoding"):
        processor.validate(
            JoinNodeConfig(), inputs={"a": "x", "b": "y"},
            **{**kwargs, "outputs": (packed,)},
        )


def test_join_type_checks_inputs() -> None:
    context = ProcessorContext(
        None, SimpleNamespace(node_id="j"), (), logging.getLogger("t")
    )
    rows = _rows({"B02": [1, 2]}, representation="composite", times=["all"])
    with pytest.raises(TypeError, match="PointSampleArtifact"):
        JoinProcessor().execute_points(
            JoinNodeConfig(),
            {"a": PointSampleArtifact(rows, SimpleNamespace()), "b": object()},
            context,
        )


def test_pipeline_writes_one_parquet_for_two_terrain_nodes(tmp_path) -> None:
    path = _write_dem_project(tmp_path, "{type: dem.local, data_dir: dem}")
    path.write_text(
        path.read_text()
        .replace(
            "  - {id: terrain, type: dem, source: dem, features: [slope, aspect]}\n",
            "  - {id: slope, type: dem, source: dem, features: [slope]}\n"
            "  - {id: aspect, type: dem, source: dem, features: [aspect]}\n"
            "  - {id: terrain, type: join, inputs: {base: slope, other: aspect}}\n",
        )
    )

    run_pipeline(PipelineConfig.from_file(path), logging.getLogger("join-test"))
    rows = pd.read_parquet(tmp_path / "output" / "samples.parquet")

    assert len(rows) == 1
    assert rows["slope"].iloc[0] == pytest.approx(45.0, abs=0.01)
    assert rows["aspect"].iloc[0] == pytest.approx(90.0, abs=0.01)


def test_remote_example_pipeline_validates(tmp_path) -> None:
    import json
    import shutil
    from pathlib import Path

    example = Path(__file__).parents[1] / "examples" / "pavc_random_sample_remote_processing.yaml"
    (tmp_path / "examples").mkdir()
    shutil.copy(example, tmp_path / "examples" / example.name)
    (tmp_path / "data" / "s1" / "raw").mkdir(parents=True)
    (tmp_path / "data" / "arcticdem" / "10m").mkdir(parents=True)
    aoi = tmp_path / "data" / "aois" / "pavc_random_sample.geojson"
    aoi.parent.mkdir(parents=True)
    aoi.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"site_visit_id": "a"},
                        "geometry": {"type": "Point", "coordinates": [-150.0, 68.0]},
                    }
                ],
            }
        )
    )

    config = PipelineConfig.from_file(tmp_path / "examples" / example.name)

    assert {source.source_type for source in config.sources.values()} == {
        "s2.l2a.remote",
        "dem.remote",
        "s1.grd.local",
        "dem.local",
    }
    # Sentinel-1 backscatter joins the table through the same node.
    assert config.nodes[-1].inputs == {
        "base": "indices",
        "terrain": "terrain",
        "radar": "s1-db",
    }
    assert [node.node_id for node in config.ordered_nodes][-1] == "samples"


def test_remote_boxes_example_pipeline_validates(tmp_path) -> None:
    import json
    import shutil
    from pathlib import Path

    example = (
        Path(__file__).parents[1] / "examples" / "pavc_random_sample_boxes_remote_processing.yaml"
    )
    (tmp_path / "examples").mkdir()
    shutil.copy(example, tmp_path / "examples" / example.name)
    (tmp_path / "data" / "s1" / "raw").mkdir(parents=True)
    (tmp_path / "data" / "arcticdem" / "10m").mkdir(parents=True)
    aoi = tmp_path / "data" / "aois" / "pavc_random_sample_boxes.geojson"
    aoi.parent.mkdir(parents=True)
    aoi.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"site_visit_id": "a"},
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [
                                [[-150.0, 68.0], [-149.99, 68.0], [-149.99, 68.01],
                                 [-150.0, 68.01], [-150.0, 68.0]]
                            ],
                        },
                    }
                ],
            }
        )
    )

    config = PipelineConfig.from_file(tmp_path / "examples" / example.name)

    assert config.aoi_plan.kind == "polygon"
    # One joined output, as for points: no per-node outputs and no packing.
    assert set(config.outputs) == {"samples"}
    output = config.outputs["samples"]
    assert output.node == "samples" and output.format == "cog" and output.encoding is None
    assert config.nodes[-1].inputs == {
        "base": "indices",
        "terrain": "terrain",
        "radar": "s1-db",
    }


########################################################################################
# Raster join
########################################################################################

xr = pytest.importorskip("xarray")
da = pytest.importorskip("dask.array")

from sentinel_py.pipeline.processors.join import join_cubes  # noqa: E402

TRANSFORM = (20.0, 0.0, 1000.0, 0.0, -20.0, 5000.0)
TIMES = np.array(["2000-06-01", "2000-07-01"], dtype="datetime64[ns]")


def _cube(variable, bands, values, *, times, nodata, representation, recipe, fingerprint,
          index_coords=False, transform=TRANSFORM, size=4):
    """A (time, band, y, x) dataset shaped like the output of a real node."""
    data = da.from_array(np.asarray(values, dtype=np.float32), chunks=(1, len(bands), 2, 2))
    coords = {
        "time": times,
        "band": list(bands),
        "y": np.arange(size) if index_coords else [4990.0 - 20.0 * i for i in range(size)],
        "x": np.arange(size) if index_coords else [1010.0 + 20.0 * i for i in range(size)],
        "product_id": ("time", [f"{recipe}-product"] * len(times)),
        "granule_id": ("time", [f"{recipe}-g{i}" for i in range(len(times))]),
        "source_fingerprint": ("time", [f"{fingerprint}{i}" for i in range(len(times))]),
    }
    return xr.Dataset(
        {variable: (("time", "band", "y", "x"), data, {"nodata": nodata})},
        coords=coords,
        attrs={
            "crs": "EPSG:3338",
            "transform": transform,
            "time_representation": representation,
            "recipe_id": recipe,
        },
    )


def _index_like(**overrides):
    values = np.arange(2 * 3 * 4 * 4, dtype=np.float32).reshape(2, 3, 4, 4)
    values[0, 0, 0, 0] = 65535  # nodata DN in the first band
    defaults = dict(times=TIMES, nodata=65535, representation="composite",
                    recipe="idx", fingerprint="i")
    return _cube("reflectance", ["B02", "B03", "NDWI1"], values, **{**defaults, **overrides})


def _dem_like(**overrides):
    values = np.full((1, 2, 4, 4), 700.0, dtype=np.float32)
    values[0, 1] = 3.0
    values[0, 0, 3, 3] = -9999
    defaults = dict(times=np.array(["1970-01-01"], dtype="datetime64[ns]"), nodata=-9999,
                    representation="static", recipe="dem", fingerprint="d",
                    index_coords=True)
    return _cube("reflectance", ["elevation", "slope"], values, **{**defaults, **overrides})


def _s1_like(times=TIMES, **overrides):
    values = np.linspace(-25.0, -5.0, len(times) * 2 * 16, dtype=np.float32)
    values = values.reshape(len(times), 2, 4, 4)
    defaults = dict(times=times, nodata=float("nan"), representation="composite",
                    recipe="s1", fingerprint="s")
    return _cube("backscatter_db", ["VV", "VH"], values, **{**defaults, **overrides})


def test_cubes_join_into_one_float_cube_on_the_base_grid() -> None:
    index, dem, s1 = _index_like(), _dem_like(), _s1_like()

    joined = join_cubes(index, {"terrain": dem, "radar": s1}, base_name="indices")

    cube = joined["reflectance"]
    assert cube.dims == ("time", "band", "y", "x") and cube.dtype == np.float32
    assert cube.band.values.tolist() == [
        "B02", "B03", "NDWI1", "elevation", "slope", "VV", "VH"
    ]
    # Real-world coordinates come from the base; the terrain cube numbered its pixels.
    assert joined.x.values.tolist() == index.x.values.tolist()
    assert joined.y.values.tolist() == index.y.values.tolist()
    assert np.isnan(joined.attrs["nodata"]) and np.isnan(cube.attrs["nodata"])
    assert joined.attrs["crs"] == "EPSG:3338" and joined.attrs["transform"] == TRANSFORM
    assert joined.attrs["time_representation"] == "composite"
    assert cube.data.chunks[2:] == ((2, 2), (2, 2))

    values = cube.compute().values
    # Every input's nodata becomes NaN; valid values pass through unchanged.
    assert np.isnan(values[0, 0, 0, 0])
    assert values[1, 0, 0, 0] == index.reflectance.values[1, 0, 0, 0]
    assert np.isnan(values[:, 3, 3, 3]).all()  # terrain nodata, in both time bins
    # Static terrain repeats in every base time bin.
    assert (values[:, 3, 0, 0] == 700.0).all() and (values[:, 4, 0, 0] == 3.0).all()
    np.testing.assert_array_equal(values[:, 5:], s1.backscatter_db.values)
    # Provenance comes from the base.
    assert joined.product_id.values.tolist() == ["idx-product"] * 2
    assert joined.granule_id.values.tolist() == ["idx-g0", "idx-g1"]


def test_a_bin_missing_from_an_input_becomes_nan() -> None:
    s1 = _s1_like(times=TIMES[1:])

    cube = join_cubes(_index_like(), {"radar": s1})["reflectance"].compute()

    assert np.isnan(cube.sel(band=["VV", "VH"]).isel(time=0)).all()
    np.testing.assert_array_equal(
        cube.sel(band=["VV", "VH"]).isel(time=1).values, s1.backscatter_db.values[0]
    )


def test_join_rejects_repeated_bands_other_grids_and_static_base() -> None:
    with pytest.raises(ValueError, match="repeat band name.*B02"):
        join_cubes(_index_like(), {"again": _index_like(recipe="other")})
    shifted = _s1_like(transform=(20.0, 0.0, 1020.0, 0.0, -20.0, 5000.0))
    with pytest.raises(ValueError, match="different grid"):
        join_cubes(_index_like(), {"radar": shifted})
    with pytest.raises(ValueError, match="varies in time"):
        join_cubes(_dem_like(), {"radar": _s1_like()})


def test_recipe_and_fingerprint_cover_every_input() -> None:
    def joined(**dem):
        return join_cubes(_index_like(), {"terrain": _dem_like(**dem)})

    same = joined()
    assert same.attrs["recipe_id"] == joined().attrs["recipe_id"]
    assert same.source_fingerprint.values.tolist() == joined().source_fingerprint.values.tolist()
    assert joined(recipe="other-dem").attrs["recipe_id"] != same.attrs["recipe_id"]
    changed = joined(fingerprint="z")
    assert changed.source_fingerprint.values.tolist() != same.source_fingerprint.values.tolist()
    base_changed = join_cubes(_index_like(fingerprint="q"), {"terrain": _dem_like()})
    assert base_changed.source_fingerprint.values.tolist() != same.source_fingerprint.values.tolist()


def test_execute_raster_joins_each_aoi_unit_and_checks_coverage() -> None:
    plan = SimpleNamespace()
    key = ("site", "part-0000")
    context = ProcessorContext(None, SimpleNamespace(node_id="j"), (), logging.getLogger("t"))
    first = RasterArtifact(plan, {key: _index_like()})
    second = RasterArtifact(plan, {key: _dem_like()})

    result = JoinProcessor().execute_raster(JoinNodeConfig(), {"a": first, "b": second}, context)

    assert result.plan is plan and list(result.datasets) == [key]
    assert result.datasets[key]["reflectance"].sizes["band"] == 5
    with pytest.raises(ValueError, match="different AOI units"):
        JoinProcessor().execute_raster(
            JoinNodeConfig(),
            {"a": first, "b": RasterArtifact(plan, {("other", "part-0000"): _dem_like()})},
            context,
        )
    with pytest.raises(TypeError, match="RasterArtifact"):
        JoinProcessor().execute_raster(JoinNodeConfig(), {"a": first, "b": object()}, context)


def test_pipeline_writes_one_cog_set_and_vrt_for_two_terrain_nodes(tmp_path) -> None:
    import json

    import rasterio

    path = _write_dem_project(tmp_path, "{type: dem.local, data_dir: dem}")
    (tmp_path / "aoi.geojson").write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "crs": {"type": "name", "properties": {"name": "EPSG:3338"}},
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"site": "site-1"},
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [
                                [[-100.0, 999900.0], [100.0, 999900.0], [100.0, 1000100.0],
                                 [-100.0, 1000100.0], [-100.0, 999900.0]]
                            ],
                        },
                    }
                ],
            }
        )
    )
    path.write_text(
        path.read_text()
        .replace(
            "  - {id: terrain, type: dem, source: dem, features: [slope, aspect]}\n",
            "  - {id: slope, type: dem, source: dem, features: [slope]}\n"
            "  - {id: aspect, type: dem, source: dem, features: [aspect]}\n"
            "  - {id: terrain, type: join, inputs: {base: slope, other: aspect}}\n",
        )
        .replace("format: parquet", "format: cog")
    )

    run_pipeline(PipelineConfig.from_file(path), logging.getLogger("join-cog-test"))

    unit = tmp_path / "output" / "site-1" / "part-0000" / "dem" / "dem"
    with rasterio.open(unit / "reflectance.r0000.c0000.tif") as tile:
        assert tile.descriptions == ("slope", "aspect")
        centre = tile.read()[:, tile.height // 2, tile.width // 2]
        np.testing.assert_allclose(centre, [45.0, 90.0], atol=0.01)
        expected = tile.read()
    for vrt_path in (unit / "reflectance.vrt", tmp_path / "output" / "dem" / "dem" / "reflectance.vrt"):
        with rasterio.open(vrt_path) as vrt:
            assert vrt.descriptions == ("slope", "aspect")
            np.testing.assert_array_equal(vrt.read(), expected)
