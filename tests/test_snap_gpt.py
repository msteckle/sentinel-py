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


def test_points_skip_scenes_with_no_overlap_and_warn(tmp_path: Path, monkeypatch, caplog):
    monkeypatch.setenv("STUB_GPT_NO_OVERLAP", S1_SCENES[0])
    logger = logging.getLogger("snap-no-overlap")
    with caplog.at_level(logging.WARNING, logger=logger.name):
        result = run_pipeline(
            PipelineConfig.from_file(write_s1_project(tmp_path, point=True)), logger
        )

    rows = result.node_results["sigma0"].rows
    assert rows["granule_id"].tolist() == [S1_SCENES[1]]
    messages = " ".join(r.getMessage() for r in caplog.records)
    assert "skipped 1 scene/plot pair" in messages and S1_SCENES[0] in messages


def test_points_where_every_scene_has_no_overlap_become_nodata(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("STUB_GPT_NO_OVERLAP", "S1A_IW_GRDH")  # matches both scenes
    project = write_s1_project(tmp_path, point=True)
    project.write_text(
        project.read_text().replace(
            "outputs:\n  result:\n    from: sigma0\n    path: output\n    format: parquet",
            "  - id: seasonal\n    type: composite\n    inputs: {data: sigma0}\n"
            "    period: all\n    aggregation: median\n    empty_bins: fill\n"
            "outputs:\n  result:\n    from: seasonal\n    path: output\n    format: parquet",
        )
    )
    result = _run(project)

    assert result.node_results["sigma0"].rows.empty
    composite = result.node_results["seasonal"].rows
    assert len(composite) == 1 and composite[["VV", "VH"]].isna().all().all()


def _planned_jobs(project: Path):
    """Return the config, node, prepared state and per-plot jobs of a point project."""
    from sentinel_py.pipeline.processors.base import ProcessorContext
    from sentinel_py.pipeline.processors.snap_gpt import SnapGPTProcessor

    config = PipelineConfig.from_file(project)
    node = config.nodes[0]
    processor = SnapGPTProcessor()
    context = ProcessorContext(config, node, (), LOGGER)
    state = processor._prepare(node.config, context)
    per_unit = [
        (unit, processor._jobs(unit, node.config, context, state))
        for unit in config.aoi_plan.units
    ]
    return config, node, state, per_unit


THREE_PLOTS = ((0.0, 0.0), (100.0, 100.0), (30_000.0, 0.0))


def test_group_cell_km_defaults_off_and_rejects_negative_values():
    assert SnapGPTNodeConfig(source="s1", dem="dem").group_cell_km == 0.0
    with pytest.raises(ValueError):
        SnapGPTNodeConfig(source="s1", dem="dem", group_cell_km=-1.0)


def test_grouping_with_tiny_cells_keeps_every_original_job_key(tmp_path: Path):
    from sentinel_py.pipeline.processors.snap_gpt import _group_unit_jobs

    config, node, state, per_unit = _planned_jobs(
        write_s1_project(tmp_path, point=True, point_offsets=THREE_PLOTS)
    )
    grouped = _group_unit_jobs(
        per_unit, config=node.config.model_copy(update={"group_cell_km": 0.001}),
        crs=config.output_grid.crs, dem_tiles=state["dem_tiles"],
    )
    original = [[job.key() for _, job in jobs] for _, jobs in per_unit]
    assert [[job.key() for _, job in jobs] for _, jobs in grouped] == original


def test_grouping_merges_plots_of_one_scene_into_a_covering_region(tmp_path: Path):
    from shapely import wkt

    from sentinel_py.pipeline.processors.snap_gpt import _group_unit_jobs

    config, node, state, per_unit = _planned_jobs(
        write_s1_project(tmp_path, point=True, point_offsets=THREE_PLOTS)
    )
    grouped = _group_unit_jobs(
        per_unit, config=node.config.model_copy(update={"group_cell_km": 1000.0}),
        crs=config.output_grid.crs, dem_tiles=state["dem_tiles"],
    )
    unique = {job.key(): job for _, jobs in grouped for _, job in jobs}
    assert len(unique) == len(S1_SCENES)  # one run per scene for all three plots
    for (_, original_jobs), (_, merged_jobs) in zip(per_unit, grouped, strict=True):
        for (_, original), (_, merged) in zip(original_jobs, merged_jobs, strict=True):
            assert merged.scene == original.scene
            west, south, east, north = wkt.loads(merged.region_wkt).bounds
            o_west, o_south, o_east, o_north = wkt.loads(original.region_wkt).bounds
            assert west <= o_west and south <= o_south and east >= o_east and north >= o_north
            assert merged.dem_bounds_wgs84[0] <= original.dem_bounds_wgs84[0]
            assert merged.dem_tiles  # the merged DEM extent still selects tiles


def test_grouped_points_run_gpt_once_per_scene_with_the_same_values(
    tmp_path: Path, monkeypatch
):
    def run(name: str, node_extra: str):
        folder = tmp_path / name
        folder.mkdir()
        log = folder / "gpt-runs.log"
        monkeypatch.setenv("STUB_GPT_LOG", str(log))
        project = write_s1_project(
            folder, point=True, point_offsets=THREE_PLOTS, node_extra=node_extra
        )
        rows = _run(project).node_results["sigma0"].rows
        runs = len(log.read_text().splitlines())
        return rows.sort_values(["aoi_id", "product_id"]).reset_index(drop=True), runs

    plain, plain_runs = run("plain", "")
    grouped, grouped_runs = run("grouped", "    group_cell_km: 1000")

    assert plain_runs == 3 * len(S1_SCENES) and grouped_runs == len(S1_SCENES)
    assert len(grouped) == len(plain) == 3 * len(S1_SCENES)
    assert not grouped[["VV", "VH"]].isna().any().any()  # every plot pixel was found
    assert grouped[["VV", "VH"]].to_numpy() == pytest.approx(plain[["VV", "VH"]].to_numpy())
    assert grouped["aoi_id"].tolist() == plain["aoi_id"].tolist()


def test_max_concurrent_defaults_and_is_validated():
    assert SnapGPTNodeConfig(source="s1", dem="dem").max_concurrent == 2
    with pytest.raises(ValueError):
        SnapGPTNodeConfig(source="s1", dem="dem", max_concurrent=0)


def test_oversubscription_is_warned_about(tmp_path: Path, caplog):
    project = write_s1_project(
        tmp_path, node_extra="    max_concurrent: 1000\n    memory_gb: 1024\n    threads: 1024"
    )
    logger = logging.getLogger("snap-oversub")
    with caplog.at_level(logging.WARNING, logger=logger.name):
        run_pipeline(PipelineConfig.from_file(project), logger)
    messages = " ".join(r.getMessage() for r in caplog.records if r.levelno == logging.WARNING)
    assert "exceeds the" in messages and "GB of RAM" in messages and "CPU cores" in messages


def test_point_runs_use_no_more_threads_than_max_concurrent(tmp_path: Path, monkeypatch):
    from sentinel_py.pipeline.processors import snap_gpt

    sizes = []

    class Recording(snap_gpt.ThreadPoolExecutor):
        def __init__(self, max_workers=None, *args, **kwargs):
            sizes.append(max_workers)
            super().__init__(max_workers, *args, **kwargs)

    monkeypatch.setattr(snap_gpt, "ThreadPoolExecutor", Recording)
    # execution.workers is 2 in the fixture, so max_concurrent 1 must win.
    _run(write_s1_project(tmp_path, point=True, node_extra="    max_concurrent: 1"))
    assert sizes == [1]


def test_point_scene_runs_report_progress_before_outputs_exist(tmp_path: Path):
    events = []
    config = PipelineConfig.from_file(write_s1_project(tmp_path, point=True))

    run_pipeline(
        config, LOGGER, node_progress_callback=lambda *event: events.append(event)
    )

    label = "SNAP scenes (sigma0)"
    assert events[0] == (label, 0, 2)  # the bar can open before any scene finishes
    assert events[-1] == (label, 2, 2)
    assert [done for _, done, _ in events] == sorted(done for _, done, _ in events)
    assert {name for name, _, _ in events} == {label}


def test_point_progress_counts_cached_scenes_immediately(tmp_path: Path):
    project = write_s1_project(tmp_path, point=True)
    _run(project)  # populate the per-scene cache
    events = []

    run_pipeline(
        PipelineConfig.from_file(project), LOGGER,
        node_progress_callback=lambda *event: events.append(event),
    )

    assert events[-1][1:] == (2, 2)


def test_cli_run_shows_the_eager_scene_bar_for_point_pipelines(tmp_path: Path):
    from helpers import runner

    from sentinel_py.cli.main import app

    result = runner.invoke(app, ["run", str(write_s1_project(tmp_path, point=True))])

    assert result.exit_code == 0, result.output
    assert "SNAP scenes (sigma0)" in result.output
