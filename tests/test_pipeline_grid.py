from collections.abc import Mapping
from pathlib import Path
from typing import Any

import geopandas as gpd
import pytest
from affine import Affine
from pydantic import BaseModel, ConfigDict
from shapely.geometry import box

from sentinel_py.pipeline import PipelineConfig, PipelineConfigError
from sentinel_py.pipeline.grid import ChunkShape, plan_aoi_grid
from sentinel_py.pipeline.processors import ProcessorRegistry


def _write_aoi(
    path: Path,
    bounds: tuple[float, float, float, float],
    crs: str,
) -> Path:
    gpd.GeoDataFrame(geometry=[box(*bounds)], crs=crs).to_file(path, driver="GeoJSON")
    return path


def test_grid_snaps_aoi_outward_and_builds_north_up_affine(tmp_path: Path):
    aoi = _write_aoi(tmp_path / "aoi.geojson", (105, 205, 299, 401), "EPSG:3857")

    grid = plan_aoi_grid(
        aoi,
        crs="EPSG:3857",
        resolution=(20.0, 20.0),
        anchor=(0.0, 0.0),
        chunks=ChunkShape(y=4, x=6),
    )

    assert grid.crs == "EPSG:3857"
    assert grid.bounds == (100.0, 200.0, 300.0, 420.0)
    assert grid.width == 10
    assert grid.height == 11
    assert grid.shape == (11, 10)
    assert grid.affine == Affine(20, 0, 100, 0, -20, 420)


def test_grid_iterates_complete_and_edge_chunks_in_row_major_order(tmp_path: Path):
    aoi = _write_aoi(tmp_path / "aoi.geojson", (105, 205, 299, 401), "EPSG:3857")
    grid = plan_aoi_grid(
        aoi,
        crs="EPSG:3857",
        resolution=(20.0, 20.0),
        anchor=(0.0, 0.0),
        chunks=ChunkShape(y=4, x=6),
    )

    windows = list(grid.iter_windows())

    assert grid.chunk_grid_shape == (3, 2)
    assert len(windows) == 6
    assert (windows[0].row_offset, windows[0].column_offset) == (0, 0)
    assert (windows[0].height, windows[0].width) == (4, 6)
    assert windows[0].bounds == (100.0, 340.0, 220.0, 420.0)
    assert (windows[-1].row_offset, windows[-1].column_offset) == (8, 6)
    assert (windows[-1].height, windows[-1].width) == (3, 4)
    assert windows[-1].bounds == (220.0, 200.0, 300.0, 260.0)


def test_grid_honors_nonzero_pixel_anchor(tmp_path: Path):
    aoi = _write_aoi(tmp_path / "aoi.geojson", (106, 206, 124, 224), "EPSG:3857")

    grid = plan_aoi_grid(
        aoi,
        crs="EPSG:3857",
        resolution=(20.0, 20.0),
        anchor=(5.0, 5.0),
        chunks=ChunkShape(y=256, x=256),
    )

    assert grid.bounds == (105.0, 205.0, 125.0, 225.0)
    assert grid.shape == (1, 1)


def test_neighboring_aois_share_pixel_boundaries(tmp_path: Path):
    left = _write_aoi(tmp_path / "left.geojson", (0, 0, 100, 100), "EPSG:3857")
    right = _write_aoi(tmp_path / "right.geojson", (100, 0, 200, 100), "EPSG:3857")
    kwargs = {
        "crs": "EPSG:3857",
        "resolution": (30.0, 30.0),
        "anchor": (10.0, 10.0),
        "chunks": ChunkShape(y=256, x=256),
    }

    left_grid = plan_aoi_grid(left, **kwargs)
    right_grid = plan_aoi_grid(right, **kwargs)

    assert left_grid.bounds[2] == right_grid.bounds[0]
    for grid in (left_grid, right_grid):
        xmin, ymin, xmax, ymax = grid.bounds
        assert ((xmin - 10) / 30).is_integer()
        assert ((ymin - 10) / 30).is_integer()
        assert ((xmax - 10) / 30).is_integer()
        assert ((ymax - 10) / 30).is_integer()


def test_grid_transforms_aoi_before_snapping(tmp_path: Path):
    aoi = _write_aoi(
        tmp_path / "alaska.geojson", (-149.75, 68.5, -149.5, 68.75), "EPSG:4326"
    )
    grid = plan_aoi_grid(
        aoi,
        crs="EPSG:3338",
        resolution=(20.0, 20.0),
        anchor=(0.0, 0.0),
        chunks=ChunkShape(y=512, x=512),
    )
    transformed_bounds = gpd.read_file(aoi).to_crs("EPSG:3338").total_bounds

    assert grid.bounds[0] <= transformed_bounds[0]
    assert grid.bounds[1] <= transformed_bounds[1]
    assert grid.bounds[2] >= transformed_bounds[2]
    assert grid.bounds[3] >= transformed_bounds[3]
    assert all((value / 20).is_integer() for value in grid.bounds)


class _GridConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class _GridProcessor:
    type_name = "test.grid"
    config_model = _GridConfig

    def validate(self, config, **kwargs):
        return None

    def execute(self, config, inputs: Mapping[str, Any], context):
        return None


def _registry() -> ProcessorRegistry:
    registry = ProcessorRegistry()
    registry.register(_GridProcessor())
    return registry


def _canonical_pipeline(
    tmp_path: Path,
    output_grid: str,
    *,
    include_aoi: bool = True,
) -> Path:
    (tmp_path / "raw").mkdir(exist_ok=True)
    if include_aoi:
        _write_aoi(tmp_path / "aoi.geojson", (105, 205, 299, 401), "EPSG:3857")
    selection = "aoi: aoi.geojson" if include_aoi else "years: [2024]"
    path = tmp_path / "pipeline.yaml"
    path.write_text(
        f"""
version: 1
execution: {{method: local}}
selection: {{{selection}}}
output_grid:
{output_grid}
sources:
  s2: {{type: s2.l2a.local, data_dir: raw}}
nodes:
  - {{id: grid, type: test.grid}}
outputs:
  result: {{from: grid, path: output, format: test}}
"""
    )
    return path


def test_pipeline_builds_canonical_grid_from_yaml(tmp_path: Path):
    path = _canonical_pipeline(
        tmp_path,
        """  crs: EPSG:3857
  resolution: [20, 20]
  extent: aoi
  anchor: [0, 0]
  chunks: {y: 4, x: 6}""",
    )

    config = PipelineConfig.from_file(path, registry=_registry())

    assert config.output_grid.is_canonical
    assert config.output_grid.resolution == (20.0, 20.0)
    assert config.output_grid.anchor == (0.0, 0.0)
    assert config.output_grid.grid is not None
    assert config.output_grid.grid.bounds == (100.0, 200.0, 300.0, 420.0)
    assert config.output_grid.grid.chunks == ChunkShape(y=4, x=6)


@pytest.mark.parametrize(
    ("output_grid", "message"),
    [
        (
            """  crs: not-a-crs
  resolution: 20
  extent: aoi
  chunks: {y: 4, x: 4}""",
            "Invalid output grid CRS",
        ),
        (
            """  crs: EPSG:3857
  resolution: 0
  extent: aoi
  chunks: {y: 4, x: 4}""",
            "positive finite number",
        ),
        (
            """  crs: EPSG:3857
  resolution: 20
  extent: bounds
  chunks: {y: 4, x: 4}""",
            "extent must be 'aoi'",
        ),
        (
            """  crs: EPSG:3857
  resolution: 20
  extent: aoi
  anchor: [0]
  chunks: {y: 4, x: 4}""",
            "anchor must be a two-item list",
        ),
        (
            """  crs: EPSG:3857
  resolution: 20
  extent: aoi
  chunks: {y: 0, x: 4}""",
            "chunks.y must be a positive integer",
        ),
    ],
)
def test_pipeline_rejects_invalid_canonical_grid(
    tmp_path: Path,
    output_grid: str,
    message: str,
):
    path = _canonical_pipeline(tmp_path, output_grid)

    with pytest.raises(PipelineConfigError, match=message):
        PipelineConfig.from_file(path, registry=_registry())


def test_canonical_grid_requires_an_aoi(tmp_path: Path):
    path = _canonical_pipeline(
        tmp_path,
        """  crs: EPSG:3857
  resolution: 20
  extent: aoi
  chunks: {y: 4, x: 4}""",
        include_aoi=False,
    )

    with pytest.raises(PipelineConfigError, match="selection.aoi is required"):
        PipelineConfig.from_file(path, registry=_registry())
