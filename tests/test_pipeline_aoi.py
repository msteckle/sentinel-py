"""Tests for per-feature AOI planning."""

from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from shapely.geometry import MultiPolygon, Point, box

from sentinel_py.pipeline.aoi import load_aoi_plan, point_grid
from sentinel_py.pipeline.grid import ChunkShape
from sentinel_py.pipeline.artifacts import PointSampleArtifact
from sentinel_py.pipeline.writers.parquet import ParquetOutputWriter


def _write(path: Path, geometries, identifiers) -> Path:
    """Write a small WGS84 AOI fixture with stable IDs."""
    gpd.GeoDataFrame({"site": identifiers}, geometry=geometries, crs="EPSG:4326").to_file(path, driver="GeoJSON")
    return path


def _plan(path: Path):
    """Load a plan using a small projected output lattice."""
    return load_aoi_plan(path, id_field="site", crs="EPSG:3857", resolution=(20, 20), anchor=(0, 0), chunks=ChunkShape(y=4, x=4))


def test_distant_features_receive_independent_grids(tmp_path: Path) -> None:
    """Independent features must not form one world-spanning output grid."""
    plan = _plan(_write(tmp_path / "aoi.geojson", [box(0, 0, 0.001, 0.001), box(100, 40, 100.001, 40.001)], ["near", "far"]))
    assert plan.kind == "polygon"
    assert len(plan.units) == 2
    assert all(unit.grid is not None and unit.grid.width < 20 for unit in plan.units)


def test_multipart_feature_is_split_without_losing_group_id(tmp_path: Path) -> None:
    """Multipart components receive distinct grids below their shared AOI ID."""
    plan = _plan(_write(tmp_path / "aoi.geojson", [MultiPolygon([box(0, 0, .001, .001), box(20, 20, 20.001, 20.001)])], ["group"]))
    assert [(unit.aoi_id, unit.component_id) for unit in plan.units] == [("group", "part-0000"), ("group", "part-0001")]


def test_points_are_planned_without_raster_component_grids(tmp_path: Path) -> None:
    """Points retain identity and derive a one-pixel sampling grid on demand."""
    plan = _plan(_write(tmp_path / "points.geojson", [Point(0, 0), Point(1, 1)], ["a", "b"]))
    assert plan.kind == "point"
    assert all(unit.grid is None for unit in plan.units)
    grid = point_grid(plan.units[0], crs="EPSG:3857", resolution=(20, 20), anchor=(0, 0), chunks=ChunkShape(y=4, x=4))
    assert grid.shape == (1, 1)


def test_mixed_geometry_types_are_rejected(tmp_path: Path) -> None:
    """One pipeline cannot silently mix raster and point output semantics."""
    path = _write(tmp_path / "mixed.geojson", [Point(0, 0), box(0, 0, .001, .001)], ["point", "polygon"])
    with pytest.raises(ValueError, match="only points or only polygons"):
        _plan(path)


def test_point_parquet_writer_materializes_wide_rows(tmp_path: Path) -> None:
    """Point output stores normalized wide observation rows."""
    import dask
    from types import SimpleNamespace

    plan = _plan(_write(tmp_path / "points.geojson", [Point(0, 0)], ["site"]))
    artifact = PointSampleArtifact(pd.DataFrame([{
        "aoi_id": "site", "component_id": "part-0000", "point_index": 0,
        "longitude": 0.0, "latitude": 0.0, "time": "2024-01-01",
        "product_id": "product", "granule_id": "granule",
        "time_representation": "acquisition", "B02": 11, "B03": 22,
    }]), plan)
    write_plan = ParquetOutputWriter().build(artifact, SimpleNamespace(path=tmp_path / "out"), None)
    result = dask.compute(*write_plan.tasks)
    assert result[0].status == "written"
    frame = pd.read_parquet(result[0].output_path)
    assert frame.loc[0, ["aoi_id", "B02", "B03"]].tolist() == ["site", 11, 22]
