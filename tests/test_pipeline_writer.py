"""Output-writer registry and the point Parquet writer."""

from pathlib import Path
from types import SimpleNamespace

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Point

from sentinel_py.pipeline.artifacts import PointSampleArtifact
from sentinel_py.pipeline.grid import ChunkShape, load_aoi_plan
from sentinel_py.pipeline.writers import ArtifactOutputWriter, OutputWriterRegistry
from sentinel_py.pipeline.writers.parquet import ParquetOutputWriter


def _write(path: Path, geometries, identifiers) -> Path:
    """Write a small WGS84 AOI fixture with stable IDs."""
    gpd.GeoDataFrame(
        {"site": identifiers}, geometry=geometries, crs="EPSG:4326"
    ).to_file(path, driver="GeoJSON")
    return path


def _plan(path: Path):
    """Load a plan using a small projected output lattice."""
    return load_aoi_plan(
        path,
        id_field="site",
        crs="EPSG:3857",
        resolution=(20, 20),
        anchor=(0, 0),
        chunks=ChunkShape(y=4, x=4),
    )


def test_output_writer_registry_rejects_duplicate_formats():
    registry = OutputWriterRegistry()
    registry.register(ArtifactOutputWriter("example"))

    with pytest.raises(ValueError, match="already registered"):
        registry.register(ArtifactOutputWriter("example"))


def test_output_writer_registry_rejects_unknown_formats():
    registry = OutputWriterRegistry()

    with pytest.raises(KeyError, match="Unknown output format"):
        registry.get("absent")


def test_point_parquet_writer_materializes_wide_rows(tmp_path: Path) -> None:
    """Point output stores normalized wide observation rows."""
    import dask

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
