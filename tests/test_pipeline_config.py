from collections.abc import Mapping
from pathlib import Path
from typing import Any

import geopandas as gpd
import pytest
from pydantic import BaseModel, ConfigDict
from shapely.geometry import box

from sentinel_py.pipeline import PipelineConfig, PipelineConfigError


class _DummyConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    value: int = 0


class _DummyProcessor:
    type_name = "test.value"
    config_model = _DummyConfig

    def validate(self, config, **kwargs):
        return None

    def execute(self, config, inputs: Mapping[str, Any], context):
        return config.value + sum(inputs.values())


def _pipeline_yaml(tmp_path: Path, extra: str = "") -> Path:
    (tmp_path / "raw").mkdir()
    gpd.GeoDataFrame(
        geometry=[box(500000, 999960, 500040, 1000000)], crs="EPSG:32606"
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


def test_load_pipeline_resolves_paths_and_node(tmp_path: Path):
    config = PipelineConfig.from_file(_pipeline_yaml(tmp_path))

    assert config.execution.method == "local"
    assert config.execution.workers == 2
    assert config.selection.years == (2023, 2024)
    assert config.source.data_dir == (tmp_path / "raw").resolve()
    assert config.output.path == (tmp_path / "output").resolve()
    assert [node.node_id for node in config.ordered_nodes] == ["preprocess"]
    assert config.outputs["result"].node == "preprocess"


def test_s2_preprocessor_accepts_canonical_cog_grid(tmp_path: Path):
    path = _pipeline_yaml(tmp_path)

    config = PipelineConfig.from_file(path)

    assert config.output_grid.grid is not None
    assert config.output.format == "cog"


def test_s2_preprocessor_rejects_retired_native_grid(tmp_path: Path):
    path = _pipeline_yaml(tmp_path)
    path.write_text(
        path.read_text().replace(
            "  crs: EPSG:32606\n"
            "  resolution: 20\n"
            "  extent: aoi\n"
            "  anchor: [0, 0]\n"
            "  chunks: {y: 512, x: 512}",
            "  crs: native\n  resolution_m: 20",
        )
    )

    with pytest.raises(PipelineConfigError, match="requires a canonical output grid"):
        PipelineConfig.from_file(path)


def test_pipeline_rejects_unknown_keys(tmp_path: Path):
    path = _pipeline_yaml(tmp_path, "unexpected: true")

    with pytest.raises(PipelineConfigError, match="Unsupported key"):
        PipelineConfig.from_file(path)


def test_pipeline_accepts_legacy_output_node_alias(tmp_path: Path):
    path = _pipeline_yaml(tmp_path)
    path.write_text(path.read_text().replace("from: preprocess", "node: preprocess"))

    config = PipelineConfig.from_file(path)

    assert config.outputs["result"].node == "preprocess"


def test_pipeline_rejects_backend_settings_that_would_be_ignored(tmp_path: Path):
    path = _pipeline_yaml(tmp_path)
    path.write_text(
        path.read_text().replace(
            "workers: 2", "workers: 2\n  scheduler_address: tcp://scheduler:8786"
        )
    )

    with pytest.raises(PipelineConfigError, match="Unsupported local execution"):
        PipelineConfig.from_file(path)


def test_pipeline_accepts_complete_slurm_execution(tmp_path: Path):
    path = _pipeline_yaml(tmp_path)
    path.write_text(
        path.read_text().replace(
            "method: local\n  workers: 2",
            "method: slurm\n"
            "  jobs: 4\n"
            "  cores_per_job: 8\n"
            "  processes_per_job: 8\n"
            "  memory_per_job: 64GiB\n"
            '  walltime: "04:00:00"\n'
            "  queue: regular\n"
            "  account: project",
        )
    )

    config = PipelineConfig.from_file(path)

    assert config.execution.method == "slurm"
    assert config.execution.jobs == 4
    assert config.execution.processes_per_job == 8


def _graph_pipeline(tmp_path: Path, nodes: str, output_node: str = "final") -> Path:
    (tmp_path / "raw").mkdir(exist_ok=True)
    path = tmp_path / "graph.yaml"
    path.write_text(
        f"""
version: 1
execution: {{method: local}}
selection: {{years: [2024]}}
output_grid: {{crs: native, resolution_m: 20}}
sources:
  s2: {{type: s2.l2a.local, data_dir: raw}}
nodes:
{nodes}
outputs:
  result: {{from: {output_node}, path: output, format: test}}
"""
    )
    return path


def test_pipeline_topologically_orders_named_node_inputs(tmp_path: Path):
    path = _graph_pipeline(
        tmp_path,
        """  - id: final
    type: test.value
    inputs: {data: middle}
    value: 3
  - id: root
    type: test.value
    value: 1
  - id: middle
    type: test.value
    inputs: {data: root}
    value: 2""",
    )

    config = PipelineConfig.from_file(path)

    assert [node.node_id for node in config.nodes] == ["final", "root", "middle"]
    assert [node.node_id for node in config.ordered_nodes] == [
        "root",
        "middle",
        "final",
    ]
    assert config.nodes[0].inputs == {"data": "middle"}


def test_pipeline_rejects_duplicate_node_ids(tmp_path: Path):
    path = _graph_pipeline(
        tmp_path,
        """  - {id: repeated, type: test.value}
  - {id: repeated, type: test.value}""",
        output_node="repeated",
    )

    with pytest.raises(PipelineConfigError, match="Duplicate node id: repeated"):
        PipelineConfig.from_file(path)


def test_pipeline_rejects_missing_node_input_reference(tmp_path: Path):
    path = _graph_pipeline(
        tmp_path,
        """  - id: final
    type: test.value
    inputs: {data: absent}""",
    )

    with pytest.raises(PipelineConfigError, match="references missing node 'absent'"):
        PipelineConfig.from_file(path)


def test_pipeline_rejects_node_cycles(tmp_path: Path):
    path = _graph_pipeline(
        tmp_path,
        """  - id: first
    type: test.value
    inputs: {data: second}
  - id: second
    type: test.value
    inputs: {data: first}""",
        output_node="first",
    )

    with pytest.raises(
        PipelineConfigError, match="cycle detected: first -> second -> first"
    ):
        PipelineConfig.from_file(path)


def test_processor_pydantic_model_rejects_unknown_configuration(tmp_path: Path):
    path = _graph_pipeline(
        tmp_path,
        """  - id: final
    type: test.value
    unexpected: true""",
    )

    with pytest.raises(PipelineConfigError, match="Invalid configuration") as error:
        PipelineConfig.from_file(path)
    assert "unexpected" in str(error.value)
