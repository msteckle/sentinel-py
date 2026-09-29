from __future__ import annotations

import struct
from pathlib import Path

import pytest

from sentinel_py.enums import S2_BAND_IDS, S2Bands
from sentinel_py.s2.base import S2Granule, S2PreprocessConfig
from sentinel_py.s2.discover import read_l2a_radiometry


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
        dataset.SetGeoTransform(
            (500000, resolution, 0, 1000000, 0, -resolution)
        )
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
