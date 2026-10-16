from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from sentinel_py.pipeline.processors.dem import (
    DEMFeature,
    DEMNodeConfig,
    DEMProcessor,
)


def test_dem_features_are_registered_and_configured() -> None:
    assert set(("elevation", "aspect", "slope", "hillshade")) <= set(DEMFeature.registry)
    config = DEMNodeConfig(source="arcticdem", features=["ELEVATION", "slope"])
    assert config.features == ("elevation", "slope")
    assert config.algorithm == "Horn"


def test_dem_config_rejects_duplicate_or_unknown_features() -> None:
    with pytest.raises(ValueError, match="duplicates"):
        DEMNodeConfig(source="dem", features=["slope", "SLOPE"])
    with pytest.raises(ValueError, match="unsupported"):
        DEMNodeConfig(source="dem", features=["roughness"])


def test_flat_dem_has_zero_slope_and_nodata_aspect() -> None:
    elevation = np.ones((5, 5), dtype=np.float32)
    slope = DEMFeature.registry["slope"].calculate(
        elevation, x_resolution=10.0, y_resolution=10.0, algorithm="Horn"
    )
    aspect = DEMFeature.registry["aspect"].calculate(
        elevation, x_resolution=10.0, y_resolution=10.0, algorithm="Horn"
    )
    assert np.allclose(slope, 0)
    assert np.isnan(aspect).all()


def test_horn_slope_matches_planar_surface() -> None:
    # z increases 10 m for every 10 m eastward pixel: 45 degrees.
    elevation = np.tile(np.arange(5, dtype=np.float32) * 10, (5, 1))
    slope = DEMFeature.registry["slope"].calculate(
        elevation, x_resolution=10.0, y_resolution=10.0, algorithm="Horn"
    )
    aspect = DEMFeature.registry["aspect"].calculate(
        elevation, x_resolution=10.0, y_resolution=10.0, algorithm="Horn"
    )
    assert np.allclose(slope, 45.0)
    assert np.allclose(aspect, 90.0)


def test_builtin_registry_contains_dem_processor() -> None:
    from sentinel_py.pipeline.processors.base import get_default_registry

    assert isinstance(get_default_registry().get("dem"), DEMProcessor)


def test_select_dem_paths_keeps_only_tiles_intersecting_the_aoi_component(tmp_path):
    rasterio = pytest.importorskip("rasterio")
    from rasterio.transform import from_origin
    from shapely.geometry import box

    from sentinel_py.pipeline.grid import AOIUnit
    from sentinel_py.pipeline.readers.arcticdem import select_dem_paths
    from types import SimpleNamespace

    def tile(name: str, west: float) -> Path:
        path = tmp_path / name
        with rasterio.open(
            path,
            "w",
            driver="GTiff",
            width=10,
            height=10,
            count=1,
            dtype="float32",
            crs="EPSG:32606",
            transform=from_origin(west, 1000.0, 10.0, 10.0),
        ) as dataset:
            dataset.write(np.ones((1, 10, 10), dtype=np.float32))
        return path

    near = tile("near.tif", 0.0)
    far = tile("far.tif", 50_000.0)
    unit = AOIUnit(
        aoi_id="site",
        component_id="part-0000",
        kind="polygon",
        geometry=box(20.0, 920.0, 60.0, 980.0),
        geometry_wgs84=box(0, 0, 1, 1),
        grid=None,
    )

    selected = select_dem_paths(
        (near, far), unit, grid=SimpleNamespace(crs="EPSG:32606")
    )

    assert selected == (near,)


def _write_dem_project(tmp_path: Path, source_yaml: str) -> Path:
    """Write a point pipeline over one synthetic 45-degree planar DEM tile."""
    import json

    import rasterio
    from rasterio.transform import from_origin

    tile_dir = tmp_path / "dem"
    tile_dir.mkdir(exist_ok=True)
    columns = np.arange(1200, dtype=np.float32)
    with rasterio.open(
        tile_dir / "tile.tif",
        "w",
        driver="GTiff",
        width=1200,
        height=1200,
        count=1,
        dtype="float32",
        crs="EPSG:3338",
        transform=from_origin(-6000.0, 1006000.0, 10.0, 10.0),
    ) as dataset:
        dataset.write(np.tile(columns * 10.0, (1200, 1))[None])
    (tmp_path / "aoi.geojson").write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "crs": {"type": "name", "properties": {"name": "EPSG:3338"}},
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"site": "site-1"},
                        "geometry": {"type": "Point", "coordinates": [0.0, 1000000.0]},
                    }
                ],
            }
        )
    )
    path = tmp_path / "pipeline.yaml"
    path.write_text(
        f"""
version: 1
execution: {{method: local}}
selection: {{aoi: aoi.geojson, aoi_id_field: site}}
output_grid:
  crs: EPSG:3338
  resolution: 10
  extent: aoi
  anchor: [0, 0]
  chunks: {{y: 64, x: 64}}
sources:
  dem: {source_yaml}
nodes:
  - {{id: terrain, type: dem, source: dem, features: [slope, aspect]}}
outputs:
  result: {{from: terrain, path: output, format: parquet}}
"""
    )
    return path


def _run_dem_project(path: Path):
    import logging

    import pandas as pd

    from sentinel_py.pipeline import PipelineConfig
    from sentinel_py.pipeline.run import run_pipeline

    run_pipeline(PipelineConfig.from_file(path), logging.getLogger("dem-project-test"))
    return pd.read_parquet(path.parent / "output" / "samples.parquet")


def test_local_and_remote_sources_sample_identical_terrain(tmp_path, monkeypatch):
    pytest.importorskip("rasterio")
    from rasterio.warp import transform_bounds
    from shapely.geometry import box

    from sentinel_py.pipeline.readers import arcticdem

    local = _run_dem_project(
        _write_dem_project(tmp_path, "{type: dem.local, data_dir: dem}")
    )

    remote_dir = tmp_path / "remote"
    remote_dir.mkdir()
    footprint = box(*transform_bounds("EPSG:3338", "EPSG:4326", -6000, 994000, 6000, 1006000))
    tile = arcticdem.DEMTile(
        str(tmp_path / "dem" / "tile.tif"), "https://example.invalid/tile.tif#etag", footprint
    )
    # The catalog lookup is replaced; everything after it is the shared code path.
    monkeypatch.setattr(arcticdem, "remote_dem_tiles", lambda plan, **kwargs: (tile,))
    remote = _run_dem_project(_write_dem_project(remote_dir, "{type: dem.remote}"))

    assert np.isclose(local["slope"].iloc[0], 45.0, atol=0.01)
    assert np.isclose(local["aspect"].iloc[0], 90.0, atol=0.01)
    for column in ("slope", "aspect"):
        assert remote[column].iloc[0] == pytest.approx(local[column].iloc[0])
    assert remote["source_fingerprint"].iloc[0] != local["source_fingerprint"].iloc[0]


def test_dem_point_sampling_reports_progress_per_point(tmp_path):
    import logging

    from sentinel_py.pipeline import PipelineConfig
    from sentinel_py.pipeline.run import run_pipeline

    path = _write_dem_project(tmp_path, "{type: dem.local, data_dir: dem}")
    events = []

    run_pipeline(
        PipelineConfig.from_file(path),
        logging.getLogger("dem-progress-test"),
        node_progress_callback=lambda *event: events.append(event),
    )

    assert events[0] == ("Sampling terrain points (terrain)", 0, 1)
    assert events[-1] == ("Sampling terrain points (terrain)", 1, 1)
