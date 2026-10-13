"""Small builders shared by CLI and download tests."""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
from shapely.geometry import box
from typer.testing import CliRunner

runner = CliRunner()

_POLYGON_AOI = """
{
  "type": "FeatureCollection",
  "features": [{
    "type": "Feature",
    "properties": {},
    "geometry": {
      "type": "Polygon",
      "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]
    }
  }]
}
"""


def write_polygon_aoi(path: Path) -> None:
    """Write a unit-square polygon GeoJSON AOI to ``path``."""
    path.write_text(_POLYGON_AOI)


def write_s5_config(path: Path) -> None:
    """Write a valid CDSE S3 (s5cmd) credentials file to ``path``."""
    path.write_text(
        "[default]\n"
        "aws_access_key_id = test-access-key\n"
        "aws_secret_access_key = test-secret-key\n"
        "host_base = eodata.dataspace.copernicus.eu\n"
    )


def write_earthdata_config(path: Path) -> None:
    """Write a netrc-format Earthdata credentials file to ``path``."""
    path.write_text(
        "machine urs.earthdata.nasa.gov\n"
        "    login test-user\n"
        "    password test-password\n"
    )


def write_s2_pipeline_yaml(tmp_path: Path, extra: str = "") -> Path:
    """Write a minimal valid ``s2.preprocess`` pipeline (and its AOI) under ``tmp_path``."""
    (tmp_path / "raw").mkdir()
    gpd.GeoDataFrame(
        {"site": ["site-1"]}, geometry=[box(500000, 999960, 500040, 1000000)], crs="EPSG:32606"
    ).to_file(tmp_path / "aoi.geojson", driver="GeoJSON")
    path = tmp_path / "pipeline.yaml"
    path.write_text(
        f"""
version: 1
execution:
  method: local
  workers: 2
selection:
  aoi: aoi.geojson
  aoi_id_field: site
  years: [2023, 2024]
  speriod: "06-01"
  eperiod: "08-31"
output_grid:
  crs: EPSG:32606
  resolution: 20
  extent: aoi
  anchor: [0, 0]
  chunks: {{y: 512, x: 512}}
sources:
  s2:
    type: s2.l2a.local
    data_dir: raw
nodes:
  - id: preprocess
    type: s2.preprocess
    source: s2
    bands: [B02, B08]
    mask_classes: [0, 1, 3, 8, 9, 10, 11]
outputs:
  result:
    from: preprocess
    path: output
    format: cog
{extra}
"""
    )
    return path
