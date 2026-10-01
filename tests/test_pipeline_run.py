from __future__ import annotations

import logging
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import geopandas as gpd
import numpy as np
import rasterio
from pydantic import BaseModel, ConfigDict
from rasterio.transform import from_origin
from shapely.geometry import box

from sentinel_py.pipeline import PipelineConfig
from sentinel_py.pipeline.processors import ProcessorRegistry
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
  total: {from: result, path: output, format: xarray}
"""
    )
    calls: list[str] = []
    registry = ProcessorRegistry()
    registry.register(_ValueProcessor(calls))
    logger = logging.getLogger("generic-pipeline-test")
    logger.addHandler(logging.NullHandler())

    result = run_pipeline(PipelineConfig.from_file(pipeline, registry=registry), logger)

    assert calls == ["source", "result"]
    assert result.node_results == {"source": 1, "result": 3}
    assert result.outputs == {"total": 3}


def test_pipeline_runs_preprocess_node_end_to_end(tmp_path: Path):
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
    (granule / "MTD_TL.xml").write_text(
        "<Tile><HORIZONTAL_CS_CODE>EPSG:32606</HORIZONTAL_CS_CODE>"
        '<Size resolution="20"><NROWS>2</NROWS><NCOLS>2</NCOLS></Size>'
        '<Geoposition resolution="20"><ULX>500000</ULX>'
        "<ULY>1000000</ULY></Geoposition></Tile>"
    )
    for name, values in (("B02", [0, 1100, 2000, 3000]), ("SCL", [4, 4, 9, 4])):
        path = image_dir / f"T06ABC_{acquired}_{name}_20m.jp2"
        with rasterio.open(
            path,
            "w",
            driver="GTiff",
            width=2,
            height=2,
            count=1,
            dtype="uint16",
            crs="EPSG:32606",
            transform=from_origin(500000, 1000000, 20, 20),
        ) as dataset:
            dataset.write(np.asarray(values, dtype=np.uint16).reshape(2, 2), 1)

    gpd.GeoDataFrame(
        geometry=[box(500000, 999960, 500040, 1000000)], crs="EPSG:32606"
    ).to_file(tmp_path / "aoi.geojson", driver="GeoJSON")

    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text(
        """
version: 1
execution: {method: local, workers: 1}
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
  chunks: {y: 2, x: 2}
sources:
  s2: {type: s2.l2a.local, data_dir: raw}
nodes:
  - {id: preprocess, type: s2.preprocess, source: s2, bands: [B02]}
outputs:
  result: {from: preprocess, path: output, format: cog}
"""
    )
    logger = logging.getLogger("pipeline-end-to-end-test")
    logger.addHandler(logging.NullHandler())

    result = run_pipeline(PipelineConfig.from_file(pipeline), logger)

    worker_results = result.output_results["result"].results
    assert [item.status for item in worker_results] == ["written", "written"]
    reflectance = next(
        item for item in worker_results if item.variable == "reflectance"
    )
    with rasterio.open(reflectance.output_path) as output:
        assert output.read(1).tolist() == [[65535, 100], [65535, 2000]]
