from __future__ import annotations

import logging
import struct
from pathlib import Path
from typing import Any, Mapping

import pytest
from pydantic import BaseModel, ConfigDict

from sentinel_py.pipeline import ProcessorRegistry, load_pipeline
from sentinel_py.pipeline.run import run_pipeline


class _ValueConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    value: int


class _ValueProcessor:
    type_name = "test.value"
    config_model = _ValueConfig

    def __init__(self, calls: list[str]):
        self.calls = calls

    def validate(self, config, **kwargs):
        return None

    def execute(self, config, inputs: Mapping[str, Any], context):
        self.calls.append(context.node.node_id)
        return config.value + sum(inputs.values())


def test_pipeline_executes_registered_processors_in_dependency_order(tmp_path: Path):
    (tmp_path / "raw").mkdir()
    pipeline = tmp_path / "graph.yaml"
    pipeline.write_text(
        """
version: 1
execution: {method: local}
selection: {years: [2024]}
output_grid: {crs: native, resolution_m: 20}
sources:
  s2: {type: s2.l2a.local, data_dir: raw}
nodes:
  - {id: result, type: test.value, inputs: {data: source}, value: 2}
  - {id: source, type: test.value, value: 1}
outputs:
  total: {from: result, path: output, format: test}
"""
    )
    calls: list[str] = []
    registry = ProcessorRegistry()
    registry.register(_ValueProcessor(calls))
    logger = logging.getLogger("generic-pipeline-test")
    logger.addHandler(logging.NullHandler())

    result = run_pipeline(load_pipeline(pipeline, registry=registry), logger)

    assert calls == ["source", "result"]
    assert result.node_results == {"source": 1, "result": 3}
    assert result.outputs == {"total": 3}


def test_pipeline_runs_preprocess_node_end_to_end(tmp_path: Path):
    gdal = pytest.importorskip("osgeo.gdal")
    from osgeo import osr

    from sentinel_py.s2.preprocess import validate_gdal_for_preprocessing

    try:
        validate_gdal_for_preprocessing()
    except RuntimeError as error:
        pytest.skip(str(error))

    raw = tmp_path / "raw"
    acquired = "20240615T120000"
    safe = raw / f"S2A_MSIL2A_{acquired}_N0500_R001_T06ABC_{acquired}.SAFE"
    granule = safe / "GRANULE" / f"L2A_T06ABC_A000001_{acquired}"
    image_dir = granule / "IMG_DATA" / "R20m"
    image_dir.mkdir(parents=True)
    offsets = "".join(
        f'<BOA_ADD_OFFSET band_id="{band_id}">-1000</BOA_ADD_OFFSET>'
        for band_id in range(13)
    )
    (safe / "MTD_MSIL2A.xml").write_text(
        "<Level2A><BOA_QUANTIFICATION_VALUE>10000</BOA_QUANTIFICATION_VALUE>"
        f"{offsets}"
        "<Special_Values><SPECIAL_VALUE_TEXT>NODATA</SPECIAL_VALUE_TEXT>"
        "<SPECIAL_VALUE_INDEX>0</SPECIAL_VALUE_INDEX></Special_Values>"
        "<Special_Values><SPECIAL_VALUE_TEXT>SATURATED</SPECIAL_VALUE_TEXT>"
        "<SPECIAL_VALUE_INDEX>65535</SPECIAL_VALUE_INDEX></Special_Values>"
        "</Level2A>"
    )
    spatial_reference = osr.SpatialReference()
    spatial_reference.ImportFromEPSG(32606)
    for name, values in (("B02", [0, 1100, 2000, 3000]), ("SCL", [4, 4, 9, 4])):
        path = image_dir / f"T06ABC_{acquired}_{name}_20m.jp2"
        dataset = gdal.GetDriverByName("GTiff").Create(
            str(path), 2, 2, 1, gdal.GDT_UInt16
        )
        dataset.SetGeoTransform((500000, 20, 0, 1000000, 0, -20))
        dataset.SetSpatialRef(spatial_reference)
        dataset.GetRasterBand(1).WriteRaster(
            0, 0, 2, 2, struct.pack("<4H", *values), buf_type=gdal.GDT_UInt16
        )
        dataset = None

    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text(
        """
version: 1
execution: {method: local, workers: 1}
selection: {years: [2024], speriod: "06-01", eperiod: "08-31"}
output_grid: {crs: native, resolution_m: 20}
sources:
  s2: {type: s2.l2a.local, data_dir: raw}
nodes:
  - {id: preprocess, type: s2.preprocess, source: s2, bands: [B02]}
outputs:
  result: {from: preprocess, path: output, format: vrt}
"""
    )
    logger = logging.getLogger("pipeline-end-to-end-test")
    logger.addHandler(logging.NullHandler())

    result = run_pipeline(load_pipeline(pipeline), logger)

    assert [item.status for item in result.results] == ["preprocessed"]
    assert result.state_path.is_file()
    output = gdal.Open(str(result.results[0].output_path))
    assert output.GetRasterBand(1).ReadAsArray().tolist() == [
        [65535, 100],
        [65535, 2000],
    ]
