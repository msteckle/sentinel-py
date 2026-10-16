"""Small builders shared by CLI and download tests."""

from __future__ import annotations

import json
import os
import stat
import sys
import time
import zipfile
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


# Sentinel-1 / SNAP fixtures ----------------------------------------------------------

# lat,lon pairs around lon -150..-148, lat 67..69
S1_COORDS = "67,-150 69,-150 69,-148 67,-148"

S1_MANIFEST = """<?xml version="1.0" encoding="UTF-8"?>
<xfdu:XFDU xmlns:xfdu="urn:ccsds:schema:xfdu:1"
  xmlns:safe="http://www.esa.int/safe/sentinel-1.0"
  xmlns:s1="http://www.esa.int/safe/sentinel-1.0/sentinel-1"
  xmlns:s1sarl1="http://www.esa.int/safe/sentinel-1.0/sentinel-1/sar/level-1"
  xmlns:gml="http://www.opengis.net/gml">
 <metadataSection><metadataObject><metadataWrap><xmlData>
  <safe:orbitReference>
   <safe:relativeOrbitNumber type="start">102</safe:relativeOrbitNumber>
   <s1:orbitProperties><s1:pass>{direction}</s1:pass></s1:orbitProperties>
  </safe:orbitReference>
  <safe:frameSet><safe:frame>
   <safe:footPrint srsName="http://www.opengis.net/gml/srs/epsg.xml#4326">
    <gml:coordinates>{coords}</gml:coordinates>
   </safe:footPrint>
  </safe:frame></safe:frameSet>
  <s1sarl1:standAloneProductInformation>
   <s1sarl1:productType>GRD</s1sarl1:productType>
   <s1sarl1:transmitterReceiverPolarisation>VV</s1sarl1:transmitterReceiverPolarisation>
   <s1sarl1:transmitterReceiverPolarisation>VH</s1sarl1:transmitterReceiverPolarisation>
  </s1sarl1:standAloneProductInformation>
 </xmlData></metadataWrap></metadataObject></metadataSection>
</xfdu:XFDU>
"""


def write_s1_zip(
    directory: Path,
    scene: str,
    direction: str = "ASCENDING",
    coords: str = S1_COORDS,
) -> Path:
    path = directory / f"{scene}.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(
            f"{scene}.SAFE/manifest.safe",
            S1_MANIFEST.format(direction=direction, coords=coords),
        )
    return path


S1_SCENES = (
    "S1A_IW_GRDH_1SDV_20240820T164404_20240820T164429_055299_06BE02_F69F",
    "S1A_IW_GRDH_1SDV_20240901T164404_20240901T164429_055699_06C000_ABCD",
)
# Footprint (lat,lon pairs) around the synthetic AOI near 68.4N, 155.7W.
S1_SITE_COORDS = "68.0,-156.5 68.9,-156.5 68.9,-154.9 68.0,-154.9"

_STUB_GPT = '''#!{python}
"""Stand-in for SNAP gpt: verifies the offline layout and writes a tiny GeoTIFF."""
import glob, json, math, os, sys, time

import numpy as np
import rasterio
from pyproj import Transformer
from rasterio.transform import from_origin

args = sys.argv[1:]
if "--diag" in args:
    print("SNAP Release version 14.0.0")
    sys.exit(0)
if os.environ.get("STUB_GPT_FAIL"):
    print("stub gpt: simulated failure", file=sys.stderr)
    sys.exit(3)
params = {{}}
for arg in args:
    if arg.startswith("-P"):
        name, value = arg[2:].split("=", 1)
        params[name] = value
userdir = next(a.split("=", 1)[1] for a in args if a.startswith("-Dsnap.userdir="))
orbits = glob.glob(os.path.join(userdir, "auxdata/Orbits/Sentinel-1/*/*/*/*/*.EOF"))
assert orbits, "orbit file not linked into the SNAP user directory"
assert os.path.exists(os.path.join(userdir, "auxdata/dem/egm96/ww15mgh_b.zip"))
assert os.path.exists(params["demFile"]), "DEM clip missing"

started = time.time()
time.sleep(float(os.environ.get("STUB_GPT_SLEEP", "0")))
res = float(params["pixelSpacing"])
ox, oy = float(params["gridOriginX"]), float(params["gridOriginY"])
coords = params["geoRegion"].replace("POLYGON((", "").replace("))", "").split(",")
lonlat = [tuple(map(float, c.split())) for c in coords]
to_crs = Transformer.from_crs("EPSG:4326", params["mapProjection"], always_xy=True)
xs, ys = zip(*(to_crs.transform(lon, lat) for lon, lat in lonlat))
# SNAP aligns pixel centres to origin + k * res, so corners sit half a pixel away.
left = math.floor((min(xs) - (ox - res / 2)) / res) * res + (ox - res / 2)
top = math.ceil((max(ys) - (oy - res / 2)) / res) * res + (oy - res / 2)
width = int(math.ceil((max(xs) - left) / res))
height = int(math.ceil((top - min(ys)) / res))
bands = params["sourceBands"].split(",")
with rasterio.open(
    params["output"], "w", driver="GTiff", width=width, height=height,
    count=len(bands), dtype="float32", crs=params["mapProjection"],
    transform=from_origin(left, top, res, res),
) as dst:
    for index, band in enumerate(bands, start=1):
        # Distinct constant per polarization so tests can check band order.
        dst.write(np.full((height, width), {{"Sigma0_VV": 0.1, "Sigma0_VH": 0.05}}.get(band, 0.2), "float32"), index)
with open(params["output"] + ".args.json", "w") as handle:
    json.dump({{"argv": args, "params": params}}, handle)
if os.environ.get("STUB_GPT_LOG"):
    with open(os.environ["STUB_GPT_LOG"], "a") as handle:
        handle.write("%s %s\\n" % (started, time.time()))
'''


def write_stub_gpt(path: Path) -> Path:
    """Write an executable fake ``gpt`` that mimics SNAP's offline-facing contract."""
    path.write_text(_STUB_GPT.format(python=sys.executable))
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return path


def write_s1_project(
    tmp_path: Path,
    *,
    scenes: tuple[str, ...] = S1_SCENES,
    point: bool = False,
    dem: bool = True,
    orbits: bool = True,
    node_extra: str = "",
    source_extra: str = "",
) -> Path:
    """Write a synthetic S1 + ArcticDEM pipeline whose ``gpt`` is a test stub.

    Creates scene ZIPs with manifests, staged orbit files and EGM96 archive, one DEM
    tile, an AOI, and a ``snap_gpt`` pipeline YAML. Returns the pipeline path.
    """
    import numpy as np
    import rasterio
    from pyproj import Transformer
    from rasterio.transform import from_origin

    data = tmp_path / "s1"
    data.mkdir()
    for scene in scenes:
        write_s1_zip(data, scene, "DESCENDING", S1_SITE_COORDS)
    if orbits:
        rows = []
        for scene in scenes:
            name = f"S1A_OPER_AUX_POEORB_OPOD_20240919T070635_V{scene[17:25]}T000000_{scene[17:25]}T235959.EOF"
            orbit = data / "orbits" / "POEORB" / "S1A" / name
            orbit.parent.mkdir(parents=True, exist_ok=True)
            orbit.write_text("<?xml version='1.0'?><Earth_Explorer_File/>")
            rows.append(
                {
                    "scene": scene, "status": "staged", "orbit_type": "POEORB",
                    "orbit_file": name, "orbit_path": f"orbits/POEORB/S1A/{name}",
                }
            )
        state = data / ".sentinel-py" / "s1_orbits.parquet"
        state.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rows).to_parquet(state, index=False)
    egm = data / "auxdata" / "dem" / "egm96" / "ww15mgh_b.zip"
    egm.parent.mkdir(parents=True)
    with zipfile.ZipFile(egm, "w") as archive:
        archive.writestr("ww15mgh_b.grd", b"0")

    to_3338 = Transformer.from_crs("EPSG:4326", "EPSG:3338", always_xy=True)
    cx, cy = to_3338.transform(-155.7, 68.43)
    cx, cy = round(cx / 20) * 20, round(cy / 20) * 20
    dem_dir = tmp_path / "dem"
    dem_dir.mkdir()
    if dem:
        with rasterio.open(
            dem_dir / "tile.tif", "w", driver="GTiff", width=300, height=300, count=1,
            dtype="float32", crs="EPSG:3338", nodata=-9999.0,
            transform=from_origin(cx - 75000, cy + 75000, 500, 500),
        ) as dst:
            dst.write(np.full((300, 300), 120.0, "float32"), 1)
    geometry = (
        {"type": "Point", "coordinates": [cx, cy]}
        if point
        else {
            "type": "Polygon",
            "coordinates": [[[cx, cy], [cx + 400, cy], [cx + 400, cy + 400],
                             [cx, cy + 400], [cx, cy]]],
        }
    )
    (tmp_path / "aoi.geojson").write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "crs": {"type": "name", "properties": {"name": "EPSG:3338"}},
                "features": [
                    {"type": "Feature", "properties": {"site": "site-1"}, "geometry": geometry}
                ],
            }
        )
    )
    stub = write_stub_gpt(tmp_path / "gpt")
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
  years: [2024]
  speriod: "06-01"
  eperiod: "09-30"
output_grid:
  crs: EPSG:3338
  resolution: 20
  extent: aoi
  anchor: [0, 0]
  chunks: {{y: 64, x: 64}}
sources:
  s1:
    type: s1.grd.local
    data_dir: s1
{source_extra}
  dem:
    type: dem.local
    data_dir: dem
nodes:
  - id: sigma0
    type: snap_gpt
    source: s1
    dem: dem
    gpt: {stub}
    work_dir: work
{node_extra}
outputs:
  result:
    from: sigma0
    path: output
    format: {"parquet" if point else "xarray"}
"""
    )
    return path
