"""Small builders shared by CLI and download tests."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import box
from typer.testing import CliRunner

from sentinel_py.cache import REGISTRY

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


DAY = 86400.0


def age_files(paths, days: float) -> None:
    """Make files look last used ``days`` ago."""
    stamp = time.time() - days * DAY
    for path in paths:
        os.utime(path, (stamp, stamp))


def make_query(
    cache_dir: Path, provider: str, key: str, *, days: float, rows: int = 2
) -> Path:
    """Create a keyed query directory like ``QueryCache`` does."""
    directory = cache_dir / provider / key
    directory.mkdir(parents=True)
    table_name = "scenes.parquet" if provider == "cdse" else "manifest.parquet"
    table = directory / table_name
    pd.DataFrame({"url": [f"u{i}" for i in range(rows)]}).to_parquet(table)
    (directory / "query_info.json").write_text(
        json.dumps({"num_products": rows, "years": [2024], "collection": "S1"})
    )
    age_files([table], days)
    return directory


def make_point_sample(cache_dir: Path, key: str, *, days: float) -> Path:
    """Create a cached point-sample table."""
    path = cache_dir / "pipeline" / "point-samples" / "s2.l2a.local" / f"{key}.parquet"
    path.parent.mkdir(parents=True)
    pd.DataFrame({"value": [1, 2, 3]}).to_parquet(path)
    age_files([path], days)
    return path


def make_assets_store(cache_dir: Path, *, batches: int = 2):
    """Create the CDSE asset store holding ``batches`` uncompacted parts."""
    store = REGISTRY["cdse_assets"].store(cache_dir / "cdse")
    for batch in range(batches):
        store.append(
            [
                {"safedir": f"S{batch}", "img_path_in_safedir": f"a{n}", "size": n}
                for n in range(3)
            ]
        )
    return store
