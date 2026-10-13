import zipfile
from pathlib import Path

import pandas as pd
import pytest
from shapely.geometry import Point

from sentinel_py.download.s1_orbits import ORBIT_STATE_RELPATH
from sentinel_py.pipeline.config import PipelineConfigError, S1SourceConfig
from sentinel_py.pipeline.readers.base import get_default_reader_registry
from sentinel_py.pipeline.readers.s1 import (
    attach_orbits,
    build_scene_catalog,
    read_scene_manifest,
    select_scenes,
)

MANIFEST = """<?xml version="1.0" encoding="UTF-8"?>
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
# lat,lon pairs around lon -150..-148, lat 67..69
COORDS = "67,-150 69,-150 69,-148 67,-148"


def _write_zip(
    directory: Path,
    scene: str,
    direction: str = "ASCENDING",
    coords: str = COORDS,
) -> Path:
    path = directory / f"{scene}.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(
            f"{scene}.SAFE/manifest.safe",
            MANIFEST.format(direction=direction, coords=coords),
        )
    return path


SCENE_A = "S1A_IW_GRDH_1SDV_20240601T164404_20240601T164429_054000_069000_AAAA"
SCENE_B = "S1A_IW_GRDH_1SDV_20240801T164404_20240801T164429_055000_06A000_BBBB"
SCENE_C = "S1A_IW_GRDH_1SDV_20240815T164404_20240815T164429_055100_06A100_CCCC"


def test_read_scene_manifest_extracts_catalog_fields(tmp_path: Path):
    path = _write_zip(tmp_path, SCENE_A)
    row = read_scene_manifest(path)
    assert row["platform"] == "S1A" and row["flight_direction"] == "ASCENDING"
    assert row["relative_orbit"] == 102 and row["polarizations"] == "VV,VH"
    assert row["start_time"].startswith("2024-06-01T16:44:04")
    # Footprint is stored lon/lat.
    assert row["footprint_wkt"].startswith("POLYGON ((-150 67,")


def test_catalog_caches_and_rereads_only_changed_archives(tmp_path: Path, monkeypatch):
    _write_zip(tmp_path, SCENE_A)
    first = build_scene_catalog(tmp_path)
    assert list(first["scene"]) == [SCENE_A]

    calls = []
    import sentinel_py.pipeline.readers.s1 as module

    original = module.read_scene_manifest
    monkeypatch.setattr(
        module, "read_scene_manifest", lambda p: calls.append(p) or original(p)
    )
    _write_zip(tmp_path, SCENE_B, direction="DESCENDING")
    second = build_scene_catalog(tmp_path)
    assert len(second) == 2
    assert [p.name for p in calls] == [f"{SCENE_B}.zip"]


def test_catalog_skips_unreadable_archives(tmp_path: Path):
    _write_zip(tmp_path, SCENE_A)
    (tmp_path / f"{SCENE_B}.zip").write_bytes(b"not a zip")
    catalog = build_scene_catalog(tmp_path)
    assert list(catalog["scene"]) == [SCENE_A]


def test_select_scenes_filters_time_space_and_direction(tmp_path: Path):
    _write_zip(tmp_path, SCENE_A, "ASCENDING")
    _write_zip(tmp_path, SCENE_B, "DESCENDING")
    _write_zip(tmp_path, SCENE_C, "DESCENDING", coords="10,10 11,10 11,11 10,11")
    catalog = build_scene_catalog(tmp_path)

    window = select_scenes(catalog, start="2024-07-01", end="2024-08-31")
    assert set(window["scene"]) == {SCENE_B, SCENE_C}
    spatial = select_scenes(catalog, geometry_wgs84=Point(-149, 68))
    assert set(spatial["scene"]) == {SCENE_A, SCENE_B}
    assert list(select_scenes(catalog, flight_direction="ASCENDING")["scene"]) == [SCENE_A]
    # Two descending scenes outnumber one ascending scene.
    predominant = select_scenes(catalog, flight_direction="PREDOMINANT")
    assert set(predominant["scene"]) == {SCENE_B, SCENE_C}
    with pytest.raises(ValueError, match="flight_direction"):
        select_scenes(catalog, flight_direction="SIDEWAYS")


def test_attach_orbits_joins_staged_files_and_ignores_missing_files(tmp_path: Path):
    _write_zip(tmp_path, SCENE_A)
    _write_zip(tmp_path, SCENE_B)
    orbit = tmp_path / "orbits" / "POEORB" / "S1A" / "orbit.EOF"
    orbit.parent.mkdir(parents=True)
    orbit.write_text("<?xml")
    state = pd.DataFrame(
        {
            "scene": [SCENE_A, SCENE_B],
            "status": ["staged", "staged"],
            "orbit_type": ["POEORB", "POEORB"],
            "orbit_file": ["orbit.EOF", "gone.EOF"],
            "orbit_path": ["orbits/POEORB/S1A/orbit.EOF", "orbits/POEORB/S1A/gone.EOF"],
        }
    )
    (tmp_path / ORBIT_STATE_RELPATH).parent.mkdir(parents=True)
    state.to_parquet(tmp_path / ORBIT_STATE_RELPATH, index=False)

    joined = attach_orbits(build_scene_catalog(tmp_path), tmp_path).set_index("scene")
    assert joined.loc[SCENE_A, "orbit_path"] == str(orbit)
    assert pd.isna(joined.loc[SCENE_B, "orbit_path"])


def test_s1_reader_parses_source_and_provides_scene_catalog(tmp_path: Path):
    _write_zip(tmp_path, SCENE_A)
    reader = get_default_reader_registry().get("s1.grd.local")
    source = reader.parse_config(
        "s1", {"type": "s1.grd.local", "data_dir": str(tmp_path)}, tmp_path
    )
    assert isinstance(source, S1SourceConfig)
    assert source.flight_direction == "BOTH" and source.orbit == "precise-or-restituted"
    assert list(reader.scene_catalog(source)["scene"]) == [SCENE_A]

    for bad in (
        {"type": "s1.grd.local", "data_dir": str(tmp_path), "orbit": "none"},
        {"type": "s1.grd.local", "data_dir": str(tmp_path), "flight_direction": "UP"},
        {"type": "s1.grd.local", "data_dir": str(tmp_path / "missing")},
        {"type": "s1.grd.local", "data_dir": str(tmp_path), "pattern": "*.zip"},
    ):
        with pytest.raises(PipelineConfigError):
            reader.parse_config("s1", bad, tmp_path)
    with pytest.raises(NotImplementedError):
        reader.read_raster()
