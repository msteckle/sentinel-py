from __future__ import annotations

import json
import struct
from pathlib import Path

import pytest
from shapely.geometry import box, mapping

from sentinel_py.enums import S2_BAND_IDS, S2Bands
from sentinel_py.s2.base import S2Granule, S2PreprocessConfig
from sentinel_py.s2.discover import discover_s2_granules, read_l2a_radiometry


def _metadata(path: Path, *, include_offsets: bool = True) -> Path:
    offsets = (
        "".join(
            f'<BOA_ADD_OFFSET band_id="{band_id}">-1000</BOA_ADD_OFFSET>'
            for band_id in range(13)
        )
        if include_offsets
        else ""
    )
    path.write_text(
        "<Level2A><BOA_QUANTIFICATION_VALUE>10000</BOA_QUANTIFICATION_VALUE>"
        f"{offsets}</Level2A>"
    )
    return path


def _local_safe(root: Path, acquired: str, *, ulx: int = 500000) -> Path:
    """Create a minimal local SAFE tree suitable for discovery tests."""
    safe = root / (f"S2A_MSIL2A_{acquired}_N0500_R001_T31NAA_{acquired}.SAFE")
    granule = safe / "GRANULE" / f"L2A_T31NAA_A000001_{acquired}"
    image_dir = granule / "IMG_DATA" / "R20m"
    image_dir.mkdir(parents=True)
    (safe / "MTD_MSIL2A.xml").write_text("<Level2A/>")
    (image_dir / f"T31NAA_{acquired}_B02_20m.jp2").touch()
    (image_dir / f"T31NAA_{acquired}_SCL_20m.jp2").touch()
    (granule / "MTD_TL.xml").write_text(
        "<Tile><HORIZONTAL_CS_CODE>EPSG:32631</HORIZONTAL_CS_CODE>"
        '<Size resolution="10"><NROWS>10</NROWS><NCOLS>10</NCOLS></Size>'
        f'<Geoposition resolution="10"><ULX>{ulx}</ULX><ULY>100</ULY>'
        "</Geoposition></Tile>"
    )
    return safe


def test_s2_metadata_ids_cover_the_canonical_band_enum():
    assert set(S2_BAND_IDS) == {band.value for band in S2Bands}
    assert S2_BAND_IDS["B01"] == "0"
    assert S2_BAND_IDS["B8A"] == "8"


def test_read_l2a_radiometry_uses_esa_band_ids(tmp_path: Path):
    offsets, quantification = read_l2a_radiometry(_metadata(tmp_path / "MTD.xml"))

    assert quantification == 10000
    assert offsets["B01"] == -1000
    assert offsets["B08"] == -1000
    assert offsets["B8A"] == -1000
    assert offsets["B12"] == -1000


def test_read_l2a_radiometry_treats_pre_offset_products_as_zero(tmp_path: Path):
    offsets, _ = read_l2a_radiometry(
        _metadata(tmp_path / "MTD.xml", include_offsets=False)
    )

    assert set(offsets.values()) == {0}


def test_discovery_filters_local_cache_by_aoi_year_and_season(tmp_path: Path):
    _local_safe(tmp_path, "20230615T120000")
    _local_safe(tmp_path, "20240115T120000")
    _local_safe(tmp_path, "20240615T120000", ulx=700000)
    aoi = tmp_path / "aoi.geojson"
    aoi.write_text(
        '{"type":"Feature","properties":{},"geometry":'
        + json.dumps(mapping(box(2.999, -0.001, 3.002, 0.002)))
        + "}"
    )

    granules = discover_s2_granules(
        tmp_path,
        ("B02",),
        20,
        aoi=aoi,
        years=(2023, 2024),
        speriod=(6, 1),
        eperiod=(8, 31),
    )

    assert [granule.acquisition_time for granule in granules] == ["20230615T120000"]
    assert granules[0].band_paths["B02"].name.endswith("_B02_20m.jp2")


def test_preprocessed_vrt_applies_offset_mask_and_cache(tmp_path: Path):
    gdal = pytest.importorskip("osgeo.gdal")
    from osgeo import osr

    from sentinel_py.s2.preprocess import (
        preprocess_s2_granule,
        validate_gdal_for_preprocessing,
    )

    try:
        validate_gdal_for_preprocessing()
    except RuntimeError as error:
        pytest.skip(str(error))
    gdal.UseExceptions()

    # Create two tiny georeferenced sources without depending on NumPy.
    spatial_reference = osr.SpatialReference()
    spatial_reference.ImportFromEPSG(32606)
    sources = {}
    for name, size, resolution, values in (
        ("B02", 2, 20, [0, 1100, 2000, 3000]),
        ("B08", 4, 10, [1400] * 16),
        ("SCL", 2, 20, [4, 4, 9, 4]),
    ):
        path = tmp_path / f"{name}.tif"
        dataset = gdal.GetDriverByName("GTiff").Create(
            str(path), size, size, 1, gdal.GDT_UInt16
        )
        dataset.SetGeoTransform((500000, resolution, 0, 1000000, 0, -resolution))
        dataset.SetSpatialRef(spatial_reference)
        dataset.GetRasterBand(1).WriteRaster(
            0,
            0,
            size,
            size,
            struct.pack(f"<{len(values)}H", *values),
            buf_type=gdal.GDT_UInt16,
        )
        dataset = None
        sources[name] = path

    metadata = _metadata(tmp_path / "MTD_MSIL2A.xml")
    granule = S2Granule(
        product_id="S2A_MSIL2A_20230601T000000_N0500_R001_T06ABC_20230601T000000",
        granule_id="L2A_T06ABC_A000001_20230601T000000",
        tile_id="T06ABC",
        acquisition_time="20230601T000000",
        safe_dir=tmp_path,
        product_metadata=metadata,
        band_paths={"B02": sources["B02"], "B08": sources["B08"]},
        scl_path=sources["SCL"],
    )
    config = S2PreprocessConfig(
        bands=("B02", "B08"), mask_classes=(0, 1, 3, 8, 9, 10, 11)
    )

    first = preprocess_s2_granule(granule, config, tmp_path / "preprocessed")
    second = preprocess_s2_granule(
        granule, config, tmp_path / "preprocessed", first.state_row
    )

    assert first.status == "preprocessed"
    assert second.status == "skipped"
    dataset = gdal.Open(str(first.output_path))
    assert dataset.GetProjectionRef().find("32606") >= 0
    assert dataset.GetRasterBand(1).ReadAsArray().tolist() == [
        [65535, 100],
        [65535, 2000],
    ]
    assert dataset.GetRasterBand(2).ReadAsArray().tolist() == [
        [400, 400],
        [65535, 400],
    ]
    assert dataset.GetRasterBand(1).GetScale() == pytest.approx(0.0001)
    assert dataset.GetRasterBand(1).GetUnitType() == "reflectance"
    assert "gdal_streamed_alg" in first.output_path.read_text()
