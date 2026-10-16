"""DEM coverage planning for ``snap_gpt`` nodes (no SNAP and no network needed)."""

from __future__ import annotations

from pathlib import Path

import pytest
from helpers import S1_SCENES, write_s1_project
from shapely.geometry import box

from sentinel_py.pipeline import PipelineConfig
from sentinel_py.pipeline.dem_coverage import find_dem_gaps


def _config(tmp_path: Path, **kwargs) -> PipelineConfig:
    return PipelineConfig.from_file(write_s1_project(tmp_path, **kwargs))


def test_fully_covered_pipeline_has_no_gaps(tmp_path: Path):
    assert find_dem_gaps(_config(tmp_path)) == []


def test_missing_tiles_report_every_scene_with_the_whole_region_required(tmp_path: Path):
    config = _config(tmp_path, dem=False)
    gaps = find_dem_gaps(config)

    assert [gap.scene for gap in gaps] == list(S1_SCENES)
    gap = gaps[0]
    assert gap.node_id == "sigma0" and gap.dem_source == "dem"
    assert gap.dem_dir == (tmp_path / "dem").resolve()
    assert gap.required.area == pytest.approx(box(*gap.region).area)
    assert not gap.margin.is_empty  # nothing covers the margin either


def test_gap_geometry_is_only_the_uncovered_part(tmp_path: Path):
    import rasterio

    config = _config(tmp_path)
    full = find_dem_gaps(config, include_margin=True)
    assert full == []
    # Replace the tile with one covering only the left half of the AOI region.
    tile = next((tmp_path / "dem").glob("*.tif"))
    with rasterio.open(tile) as src:
        profile, data, transform = src.profile, src.read(1), src.transform
    narrow = data[:, : data.shape[1] // 2]  # ends at the AOI's west edge
    profile.update(width=narrow.shape[1], transform=transform)
    with rasterio.open(tile, "w", **profile) as dst:
        dst.write(narrow, 1)
    gaps = find_dem_gaps(config, include_margin=False)

    assert gaps
    gap = gaps[0]
    region_area = box(*gap.region).area
    assert 0 < gap.required.area < region_area
    assert gap.margin.is_empty  # margin is excluded unless requested
    assert gap.required.bounds[2] == pytest.approx(gap.region[2], abs=1e-6)  # east side


def test_margin_only_gap_does_not_block_processing(tmp_path: Path):
    config = _config(tmp_path, node_extra="    dem_margin_m: 150000")

    assert find_dem_gaps(config, include_margin=False) == []
    gaps = find_dem_gaps(config, include_margin=True)
    assert gaps and all(gap.required.is_empty and not gap.margin.is_empty for gap in gaps)


def test_node_filter_and_non_snap_pipelines(tmp_path: Path):
    from helpers import write_s2_pipeline_yaml

    (tmp_path / "a").mkdir()
    config = _config(tmp_path / "a", dem=False)
    assert find_dem_gaps(config, ["other"]) == []
    assert len(find_dem_gaps(config, ["sigma0"])) == 2

    (tmp_path / "b").mkdir()
    s2 = PipelineConfig.from_file(write_s2_pipeline_yaml(tmp_path / "b"))
    assert find_dem_gaps(s2) == []
