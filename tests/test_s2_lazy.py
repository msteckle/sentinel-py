from __future__ import annotations

import logging
from pathlib import Path

import geopandas as gpd
import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import box

from sentinel_py.pipeline import load_pipeline
from sentinel_py.pipeline.run import run_pipeline


def _write_raster(path: Path, values: np.ndarray, resolution: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=values.shape[1],
        height=values.shape[0],
        count=1,
        dtype=values.dtype,
        crs="EPSG:32606",
        transform=from_origin(500000, 1000000, resolution, resolution),
    ) as dataset:
        dataset.write(values, 1)


def _synthetic_pipeline(tmp_path: Path) -> tuple[Path, Path]:
    acquired = "20240615T120000"
    safe = (
        tmp_path
        / "raw"
        / f"S2A_MSIL2A_{acquired}_N0500_R001_T06ABC_{acquired}.SAFE"
    )
    granule = safe / "GRANULE" / f"L2A_T06ABC_A000001_{acquired}"
    offsets = "".join(
        f'<BOA_ADD_OFFSET band_id="{band_id}">-1000</BOA_ADD_OFFSET>'
        for band_id in range(13)
    )
    safe.mkdir(parents=True)
    (safe / "MTD_MSIL2A.xml").write_text(
        "<Level2A><BOA_QUANTIFICATION_VALUE>10000</BOA_QUANTIFICATION_VALUE>"
        f"{offsets}"
        "<Special_Values><SPECIAL_VALUE_TEXT>NODATA</SPECIAL_VALUE_TEXT>"
        "<SPECIAL_VALUE_INDEX>0</SPECIAL_VALUE_INDEX></Special_Values>"
        "<Special_Values><SPECIAL_VALUE_TEXT>SATURATED</SPECIAL_VALUE_TEXT>"
        "<SPECIAL_VALUE_INDEX>65535</SPECIAL_VALUE_INDEX></Special_Values>"
        "</Level2A>"
    )
    (granule / "MTD_TL.xml").parent.mkdir(parents=True)
    (granule / "MTD_TL.xml").write_text(
        "<Tile><HORIZONTAL_CS_CODE>EPSG:32606</HORIZONTAL_CS_CODE>"
        '<Size resolution="10"><NROWS>4</NROWS><NCOLS>4</NCOLS></Size>'
        '<Geoposition resolution="10"><ULX>500000</ULX>'
        "<ULY>1000000</ULY></Geoposition></Tile>"
    )
    _write_raster(
        granule / "IMG_DATA" / "R10m" / f"T06ABC_{acquired}_B02_10m.jp2",
        np.array(
            [
                [0, 1100, 2000, 2200],
                [1300, 1500, 2400, 2600],
                [65535, 3000, 4000, 4200],
                [3200, 3400, 4400, 4600],
            ],
            dtype=np.uint16,
        ),
        10,
    )
    _write_raster(
        granule / "IMG_DATA" / "R20m" / f"T06ABC_{acquired}_B03_20m.jp2",
        np.array([[1000, 1100], [0, 65535]], dtype=np.uint16),
        20,
    )
    _write_raster(
        granule / "IMG_DATA" / "R20m" / f"T06ABC_{acquired}_SCL_20m.jp2",
        np.array([[4, 4], [4, 9]], dtype=np.uint16),
        20,
    )
    aoi = tmp_path / "aoi.geojson"
    gpd.GeoDataFrame(
        geometry=[box(500000, 999960, 500040, 1000000)], crs="EPSG:32606"
    ).to_file(aoi, driver="GeoJSON")
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text(
        """
version: 1
execution: {method: local}
selection:
  aoi: aoi.geojson
  years: [2024]
  speriod: "06-01"
  eperiod: "08-31"
output_grid:
  crs: EPSG:32606
  resolution: 20
  extent: aoi
  anchor: [0, 0]
  chunks: {y: 1, x: 1}
sources:
  s2: {type: s2.l2a.local, data_dir: raw}
nodes:
  - id: preprocess
    type: s2.preprocess
    source: s2
    bands: [B02, B03]
outputs:
  result: {from: preprocess, path: unused, format: xarray}
"""
    )
    return pipeline, granule


def test_lazy_s2_processor_defers_reads_and_preserves_grid_masks_and_dtypes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    da = pytest.importorskip("dask.array")
    from sentinel_py.s2 import lazy

    pipeline_path, _ = _synthetic_pipeline(tmp_path)
    calls = []
    original = lazy._read_scene_chunk

    def counted_reader(scene, grid, window):
        calls.append((scene.granule.granule_id, window.row_index, window.column_index))
        return original(scene, grid, window)

    monkeypatch.setattr(lazy, "_read_scene_chunk", counted_reader)
    logger = logging.getLogger("lazy-s2-test")
    logger.addHandler(logging.NullHandler())

    result = run_pipeline(load_pipeline(pipeline_path), logger)
    dataset = result.outputs["result"]

    assert isinstance(dataset.reflectance.data, da.Array)
    assert isinstance(dataset.scl.data, da.Array)
    assert calls == []
    assert not (tmp_path / "unused").exists()
    assert dataset.reflectance.dtype == np.dtype("uint16")
    assert dataset.scl.dtype == np.dtype("uint8")
    assert dataset.reflectance.chunks == ((1,), (2,), (1, 1), (1, 1))
    assert dataset.x.values.tolist() == [500010.0, 500030.0]
    assert dataset.y.values.tolist() == [999990.0, 999970.0]
    assert dataset.attrs["crs"] == "EPSG:32606"

    computed = dataset.compute(scheduler="single-threaded")

    assert len(calls) == 4
    np.testing.assert_array_equal(
        computed.reflectance.values,
        np.array(
            [[[[300, 1300], [2200, 65535]], [[0, 100], [65535, 65535]]]],
            dtype=np.uint16,
        ),
    )
    np.testing.assert_array_equal(
        computed.scl.values,
        np.array([[[4, 4], [4, 9]]], dtype=np.uint8),
    )


def test_local_dask_cog_outputs_share_reads_and_resume(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    from sentinel_py.pipeline.writers.cog import cog_state_path
    from sentinel_py.s2 import lazy

    pipeline_path, _ = _synthetic_pipeline(tmp_path)
    pipeline_path.write_text(
        pipeline_path.read_text()
        .replace(
            "execution: {method: local}",
            "execution: {method: local, workers: 2, threads_per_worker: 2}",
        )
        .replace(
            "  result: {from: preprocess, path: unused, format: xarray}",
            "  first: {from: preprocess, path: first, format: cog}\n"
            "  second: {from: preprocess, path: second, format: cog}",
        )
    )
    reads = tmp_path / "reads.txt"
    original = lazy._read_scene_chunk

    def counted_reader(scene, grid, window):
        with reads.open("a") as stream:
            stream.write(f"{window.row_index},{window.column_index}\n")
        return original(scene, grid, window)

    monkeypatch.setattr(lazy, "_read_scene_chunk", counted_reader)
    logger = logging.getLogger("local-dask-cog-test")
    logger.addHandler(logging.NullHandler())

    first = run_pipeline(load_pipeline(pipeline_path), logger)

    assert len(reads.read_text().splitlines()) == 4
    for output_id in ("first", "second"):
        output_result = first.output_results[output_id]
        assert len(output_result.results) == 8
        assert {item.status for item in output_result.results} == {"written"}
        assert cog_state_path(tmp_path / output_id).is_file()
        assert len(list((tmp_path / output_id).rglob("*.tif"))) == 8
        assert not list((tmp_path / output_id).rglob("*.tmp.tif"))

    sample = next((tmp_path / "first").rglob("reflectance*.tif"))
    with rasterio.open(sample) as raster:
        image_structure = raster.tags(ns="IMAGE_STRUCTURE")
        assert image_structure["LAYOUT"] == "COG"
        assert image_structure["COMPRESSION"] == "DEFLATE"
        assert raster.descriptions[0] == "B02"

    reads.unlink()
    second = run_pipeline(load_pipeline(pipeline_path), logger)

    assert not reads.exists()
    for output_result in second.output_results.values():
        assert {item.status for item in output_result.results} == {"skipped"}
