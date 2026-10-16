import logging
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from helpers import S1_SCENES, write_s1_project

from sentinel_py.enums import SourceType
from sentinel_py.pipeline import PipelineConfig
from sentinel_py.pipeline.config import PipelineConfigError
from sentinel_py.pipeline.processors import get_default_registry
from sentinel_py.pipeline.processors.snap_gpt import SnapGPTNodeConfig
from sentinel_py.pipeline.run import run_pipeline

LOGGER = logging.getLogger("snap-gpt-test")


def _run(path: Path):
    return run_pipeline(PipelineConfig.from_file(path), LOGGER)


def _dataset(result):
    return next(iter(result.node_results["sigma0"].datasets.values()))


def test_raster_node_builds_lazy_backscatter_per_scene(tmp_path: Path):
    result = _run(write_s1_project(tmp_path))
    dataset = _dataset(result)

    assert dict(dataset.sizes) == {"time": 2, "band": 2, "y": 20, "x": 20}
    assert list(dataset.band.values) == ["VV", "VH"]
    assert list(dataset.product_id.values) == list(S1_SCENES)
    assert set(dataset.orbit_type.values) == {"POEORB"}
    assert dataset.backscatter.attrs["nodata"] != dataset.backscatter.attrs["nodata"]
    values = dataset.backscatter.compute()
    assert float(values.sel(band="VV").mean()) == pytest.approx(0.1, rel=1e-5)
    assert float(values.sel(band="VH").mean()) == pytest.approx(0.05, rel=1e-5)
    assert int(values.isnull().sum()) == 0
    # One cached output and manifest per scene, in the node's work directory.
    manifests = [
        p
        for p in (tmp_path / "work" / "scenes").rglob("*.json")
        if not p.name.endswith(".args.json")  # the stub's own argument dump
    ]
    assert len(manifests) == 2


def test_raster_node_reuses_cached_scene_outputs(tmp_path: Path):
    project = write_s1_project(tmp_path)
    _dataset(_run(project)).backscatter.compute()
    outputs = {p: p.stat().st_mtime_ns for p in (tmp_path / "work").rglob("*.tif")}

    _dataset(_run(project)).backscatter.compute()
    assert {p: p.stat().st_mtime_ns for p in (tmp_path / "work").rglob("*.tif")} == outputs


def test_recipe_id_tracks_result_affecting_settings(tmp_path: Path):
    for name in "abcd":
        (tmp_path / name).mkdir()
    base = _dataset(_run(write_s1_project(tmp_path / "a"))).attrs["recipe_id"]
    datum = _dataset(
        _run(write_s1_project(tmp_path / "b", node_extra="    dem_vertical_datum: egm96"))
    ).attrs["recipe_id"]
    pols = _dataset(
        _run(write_s1_project(tmp_path / "c", node_extra="    polarizations: [VV]"))
    ).attrs["recipe_id"]
    again = _dataset(_run(write_s1_project(tmp_path / "d"))).attrs["recipe_id"]

    assert len({base, datum, pols}) == 3
    assert base == again  # paths and work dirs do not enter the recipe


def test_single_polarization_node_reads_one_band(tmp_path: Path):
    project = write_s1_project(tmp_path, node_extra="    polarizations: [VH]")
    values = _dataset(_run(project)).backscatter.compute()
    assert values.sizes["band"] == 1
    assert float(values.mean()) == pytest.approx(0.05, rel=1e-5)


def test_point_node_samples_one_row_per_scene(tmp_path: Path):
    result = _run(write_s1_project(tmp_path, point=True))
    rows = result.node_results["sigma0"].rows

    assert len(rows) == 2
    assert rows["VV"].tolist() == pytest.approx([0.1, 0.1], rel=1e-5)
    assert rows["VH"].tolist() == pytest.approx([0.05, 0.05], rel=1e-5)
    assert set(rows["granule_id"]) == set(S1_SCENES)
    assert set(rows["time_representation"]) == {"acquisition"}
    assert set(rows["orbit_type"]) == {"POEORB"}


def test_missing_staged_orbit_fails_fast_with_guidance(tmp_path: Path):
    project = write_s1_project(tmp_path, orbits=False)
    with pytest.raises(RuntimeError, match="asf download"):
        _run(project)


def test_uncovered_dem_names_the_scene(tmp_path: Path):
    project = write_s1_project(tmp_path, dem=False)
    with pytest.raises(RuntimeError, match="does not cover the processing region") as info:
        _run(project)
    message = str(info.value)
    assert "2 scene(s)" in message  # every uncovered scene is counted, not just one
    assert S1_SCENES[0] in message and "sentinel-py pgc missing" in message


def test_scene_without_requested_polarization_is_reported(tmp_path: Path):
    project = write_s1_project(tmp_path, node_extra="    polarizations: [HH]")
    with pytest.raises(RuntimeError, match="lack requested polarization"):
        _run(project)


def test_season_filter_selects_scenes(tmp_path: Path):
    project = write_s1_project(tmp_path)
    text = project.read_text().replace('speriod: "06-01"', 'speriod: "08-15"')
    project.write_text(text.replace('eperiod: "09-30"', 'eperiod: "08-31"'))
    assert _dataset(_run(project)).sizes["time"] == 1


def test_config_rejects_bad_nodes(tmp_path: Path):
    project = write_s1_project(tmp_path)
    original = project.read_text()
    cases = {
        "reserved variable": ("    work_dir: work", "    work_dir: work\n    parameters: {orbitType: x}"),
        "unused parameter": ("    work_dir: work", "    work_dir: work\n    parameters: {typo: 1}"),
        "wrong source type": ("    dem: dem\n", "    dem: s1\n"),
        "unknown key": ("    work_dir: work", "    work_dir: work\n    bogus: 1"),
    }
    for old, new in cases.values():
        project.write_text(original.replace(old, new, 1))
        with pytest.raises(PipelineConfigError):
            PipelineConfig.from_file(project)
    project.write_text(original)
    assert PipelineConfig.from_file(project).nodes[0].node_type == "snap_gpt"


def test_validate_requires_projected_metre_square_grid():
    processor = get_default_registry().get("snap_gpt")
    config = SnapGPTNodeConfig(source="s1", dem="dem")
    sources = {
        "s1": SimpleNamespace(source_type=SourceType.s1_grd_local),
        "dem": SimpleNamespace(source_type=SourceType.dem_local),
    }

    def check(**grid):
        processor.validate(
            config, node_id="n", inputs={}, sources=sources, outputs=(),
            output_grid=SimpleNamespace(
                **{"crs": "EPSG:3338", "resolution": (20, 20), "chunks": object(), **grid}
            ),
        )

    check()
    with pytest.raises(ValueError, match="projected metre"):
        check(crs="EPSG:4326")
    with pytest.raises(ValueError, match="square"):
        check(resolution=(20, 10))
    with pytest.raises(ValueError, match="canonical output grid"):
        check(chunks=None)


def test_processor_is_registered_and_versioned():
    processor = get_default_registry().get("snap_gpt")
    assert processor.version == 1 and processor.type_name == "snap_gpt"
    assert isinstance(pd.DataFrame(), pd.DataFrame)  # keeps pandas import honest


def test_backscatter_flows_through_composite_into_cog_tiles(tmp_path: Path):
    import rasterio

    project = write_s1_project(tmp_path)
    text = project.read_text().replace(
        "outputs:\n  result:\n    from: sigma0\n    path: output\n    format: xarray",
        "  - id: seasonal\n    type: composite\n    inputs: {data: sigma0}\n"
        "    period: all\n    aggregation: mean\n"
        "outputs:\n  result:\n    from: seasonal\n    path: output\n    format: cog",
    )
    assert "format: cog" in text
    project.write_text(text)

    result = _run(project)

    written = [r for r in result.output_results["result"].results]
    assert written and all(r.status == "written" for r in written)
    assert {r.variable for r in written} == {"backscatter"}
    tiles = sorted((tmp_path / "output").rglob("backscatter.r*.c*.tif"))
    assert tiles
    with rasterio.open(tiles[0]) as tile:
        assert tile.descriptions == ("VV", "VH")
        assert tile.dtypes == ("float32", "float32")
        assert tile.read(1).mean() == pytest.approx(0.1, rel=1e-5)
        assert tile.read(2).mean() == pytest.approx(0.05, rel=1e-5)


def test_backscatter_db_packed_to_int16_cog(tmp_path: Path):
    import rasterio

    project = write_s1_project(tmp_path)
    text = project.read_text().replace(
        "outputs:\n  result:\n    from: sigma0\n    path: output\n    format: xarray",
        "  - id: seasonal\n    type: composite\n    inputs: {data: sigma0}\n"
        "    period: all\n    aggregation: median\n"
        "  - id: sigma0_db\n    type: to_db\n    inputs: {data: seasonal}\n"
        "outputs:\n  result:\n    from: sigma0_db\n    path: output\n    format: cog\n"
        "    encoding: {dtype: int16, scale: 0.01}",
    )
    assert "to_db" in text
    project.write_text(text)

    result = _run(project)

    written = result.output_results["result"].results
    assert written and all(r.status == "written" for r in written)
    assert {r.variable for r in written} == {"backscatter_db"}
    tiles = sorted((tmp_path / "output").rglob("backscatter_db.r*.c*.tif"))
    assert tiles
    with rasterio.open(tiles[0]) as tile:
        assert tile.dtypes == ("int16", "int16")
        assert tile.descriptions == ("VV", "VH")
        assert tile.scales == (0.01, 0.01)
        assert tile.nodata == -32768
        vv, vh = tile.read(1) * 0.01, tile.read(2) * 0.01
        assert vv.mean() == pytest.approx(-10.0, abs=0.01)  # 0.1 linear
        assert vh.mean() == pytest.approx(-13.01, abs=0.01)  # 0.05 linear


# Missing data ---------------------------------------------------------------------


def _no_scene_project(tmp_path: Path, **kwargs) -> Path:
    """A project whose selected years (2019) have no downloaded scenes."""
    project = write_s1_project(tmp_path, **kwargs)
    project.write_text(project.read_text().replace("years: [2024]", "years: [2019]"))
    return project


def test_unit_without_scenes_becomes_an_empty_cube_with_a_warning(tmp_path, caplog):
    logger = logging.getLogger("snap-no-scenes")
    with caplog.at_level(logging.WARNING, logger=logger.name):
        result = run_pipeline(
            PipelineConfig.from_file(_no_scene_project(tmp_path)), logger
        )
    dataset = _dataset(result)

    assert dataset.sizes["time"] == 0 and dataset.sizes["band"] == 2
    assert dataset.backscatter.compute().shape == (0, 2, 20, 20)
    warnings_ = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings_) == 1
    assert "no Sentinel-1 scenes for 1 of 1 AOI unit(s): site-1" in warnings_[0]
    assert "Their output will be nodata" in warnings_[0]
    assert "years=(2019,)" in warnings_[0] and "flight_direction=BOTH" in warnings_[0]


def test_on_no_scenes_error_restores_the_hard_failure(tmp_path):
    project = _no_scene_project(tmp_path, node_extra="    on_no_scenes: error")
    with pytest.raises(RuntimeError, match="Found 0 Sentinel-1 scenes for AOI site-1"):
        _run(project)


def test_missing_scenes_flow_to_nodata_cog_output(tmp_path: Path):
    import rasterio

    project = _no_scene_project(tmp_path)
    project.write_text(
        project.read_text().replace(
            "outputs:\n  result:\n    from: sigma0\n    path: output\n    format: xarray",
            "  - id: seasonal\n    type: composite\n    inputs: {data: sigma0}\n"
            "    period: all\n    aggregation: median\n    empty_bins: fill\n"
            "  - id: sigma0_db\n    type: to_db\n    inputs: {data: seasonal}\n"
            "outputs:\n  result:\n    from: sigma0_db\n    path: output\n    format: cog\n"
            "    encoding: {dtype: int16, scale: 0.01}",
        )
    )

    result = _run(project)

    assert all(r.status == "written" for r in result.output_results["result"].results)
    tiles = sorted((tmp_path / "output").rglob("backscatter_db.r*.c*.tif"))
    assert tiles
    with rasterio.open(tiles[0]) as tile:
        assert tile.dtypes == ("int16", "int16")
        assert (tile.read() == -32768).all()  # every pixel is the nodata code
    assert not list((tmp_path / "work").rglob("*.json"))  # SNAP never ran


def test_site_with_and_site_without_scenes_in_one_run(tmp_path: Path):
    import json

    import rasterio

    project = write_s1_project(tmp_path)
    aoi = tmp_path / "aoi.geojson"
    data = json.loads(aoi.read_text())
    near = data["features"][0]
    ring = near["geometry"]["coordinates"][0]
    far = {  # 400 km away: no scene footprint reaches it
        "type": "Feature", "properties": {"site": "site-far"},
        "geometry": {"type": "Polygon", "coordinates": [[[x + 400_000, y] for x, y in ring]]},
    }
    data["features"].append(far)
    aoi.write_text(json.dumps(data))
    project.write_text(
        project.read_text().replace(
            "outputs:\n  result:\n    from: sigma0\n    path: output\n    format: xarray",
            "  - id: seasonal\n    type: composite\n    inputs: {data: sigma0}\n"
            "    period: all\n    aggregation: mean\n    empty_bins: fill\n"
            "outputs:\n  result:\n    from: seasonal\n    path: output\n    format: cog",
        )
    )

    _run(project)

    near_tile = next((tmp_path / "output" / "site-1").rglob("backscatter.r0000.c0000.tif"))
    far_tile = next((tmp_path / "output" / "site-far").rglob("backscatter.r0000.c0000.tif"))
    with rasterio.open(near_tile) as src:
        assert src.read(1).mean() == pytest.approx(0.1, rel=1e-5)
    with rasterio.open(far_tile) as src:
        assert src.nodata is not None and src.nodata != src.nodata  # NaN nodata
        assert not (src.read() == src.read()).any()  # every pixel is NaN


def test_points_without_scenes_give_a_typed_empty_table_then_nodata_rows(tmp_path: Path):
    project = _no_scene_project(tmp_path, point=True)
    project.write_text(
        project.read_text().replace(
            "outputs:\n  result:\n    from: sigma0\n    path: output\n    format: parquet",
            "  - id: seasonal\n    type: composite\n    inputs: {data: sigma0}\n"
            "    period: all\n    aggregation: median\n    empty_bins: fill\n"
            "outputs:\n  result:\n    from: seasonal\n    path: output\n    format: parquet",
        )
    )
    result = _run(project)

    scenes = result.node_results["sigma0"].rows
    assert scenes.empty and {"VV", "VH", "orbit_type", "units"} <= set(scenes.columns)
    composite = result.node_results["seasonal"].rows
    assert len(composite) == 1 and composite[["VV", "VH"]].isna().all().all()


def test_s1_points_flow_into_binned_climatology(tmp_path: Path):
    project = write_s1_project(tmp_path, point=True)
    project.write_text(
        project.read_text().replace(
            "outputs:\n  result:\n    from: sigma0\n    path: output\n    format: parquet",
            "  - id: seasonal\n    type: composite\n    inputs: {data: sigma0}\n"
            "    period: 15D\n    aggregation: median\n    empty_bins: fill\n"
            "  - id: sigma0_db\n    type: to_db\n    inputs: {data: seasonal}\n"
            "outputs:\n  result:\n    from: sigma0_db\n    path: output\n    format: parquet",
        )
    )

    rows = _run(project).node_results["sigma0_db"].rows

    # Season 06-01..09-30 in 15-day bins is nine bins; scenes fall in bins 5 and 6.
    assert len(rows) == 9
    valid = rows.dropna(subset=["VV"])
    assert valid["granule_id"].tolist() == ["bin_0005_2000-08-15", "bin_0006_2000-08-30"]
    assert valid["VV"].tolist() == pytest.approx([-10.0, -10.0], abs=1e-4)  # 0.1 linear
    assert valid["VH"].tolist() == pytest.approx([-13.0103, -13.0103], abs=1e-3)
    assert rows["VV"].isna().sum() == 7 and set(rows["units"].dropna()) == {"dB"}
