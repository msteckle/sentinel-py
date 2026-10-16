"""The ``join`` processor merges point samples of several nodes into one table."""

from __future__ import annotations

import logging
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from sentinel_py.pipeline import PipelineConfig
from sentinel_py.pipeline.artifacts import PointSampleArtifact
from sentinel_py.pipeline.processors.base import ProcessorContext, get_default_registry
from sentinel_py.pipeline.processors.join import JoinNodeConfig, JoinProcessor, join_point_rows
from sentinel_py.pipeline.run import run_pipeline
from test_dem import _write_dem_project


def _rows(values: dict[str, list], *, representation: str, times: list[str]) -> pd.DataFrame:
    """Point rows for the points ``p-0`` and ``p-1`` of site ``s``, one per time."""
    count = len(times) * 2
    return pd.DataFrame(
        {
            "aoi_id": ["s"] * count,
            "component_id": [f"point-{index % 2}" for index in range(count)],
            "point_index": [index % 2 for index in range(count)],
            "longitude": [0.0] * count,
            "latitude": [0.0] * count,
            "time": [time for time in times for _ in range(2)],
            "product_id": ["p"] * count,
            "granule_id": ["g"] * count,
            "time_representation": [representation] * count,
            "source_fingerprint": ["f"] * count,
            **values,
        }
    )


def test_static_inputs_broadcast_and_seasonal_inputs_match_on_time() -> None:
    spectral = _rows({"B02": [1, 2, 3, 4]}, representation="composite", times=["06-01", "07-01"])
    terrain = _rows({"slope": [10.0, 20.0]}, representation="static", times=["1970-01-01"])
    radar = _rows({"VV": [-5.0, -6.0]}, representation="composite", times=["07-01"])

    joined = join_point_rows(spectral, {"terrain": terrain, "radar": radar})

    assert joined["B02"].tolist() == [1, 2, 3, 4]  # the base rows keep their order
    assert joined["slope"].tolist() == [10.0, 20.0, 10.0, 20.0]
    # The radar input only has the 07-01 bin, so the 06-01 rows are missing.
    assert np.isnan(joined["VV"].iloc[:2]).all()
    assert joined["VV"].iloc[2:].tolist() == [-5.0, -6.0]
    assert joined["time"].tolist() == ["06-01", "06-01", "07-01", "07-01"]


def test_join_rejects_ambiguous_rows_and_repeated_columns() -> None:
    base = _rows({"B02": [1, 2]}, representation="composite", times=["all"])
    seasonal = _rows({"VV": [1, 2, 3, 4]}, representation="static", times=["a", "b"])
    with pytest.raises(ValueError, match="several rows per aoi_id and component_id"):
        join_point_rows(base, {"radar": seasonal})
    clash = _rows({"B02": [3, 4]}, representation="composite", times=["all"])
    with pytest.raises(ValueError, match="repeats column.*B02"):
        join_point_rows(base, {"other": clash})


def test_join_validation_and_polygon_rejection() -> None:
    processor = get_default_registry().get("join")
    assert isinstance(processor, JoinProcessor)
    kwargs = {"node_id": "n", "sources": {}, "outputs": (), "output_grid": None}
    with pytest.raises(ValueError, match="at least two"):
        processor.validate(JoinNodeConfig(), inputs={"a": "x"}, **kwargs)
    processor.validate(JoinNodeConfig(), inputs={"a": "x", "b": "y"}, **kwargs)
    with pytest.raises(ValueError, match="point samples"):
        processor.execute_raster(
            JoinNodeConfig(), {}, ProcessorContext(None, None, (), logging.getLogger("t"))
        )


def test_join_type_checks_inputs() -> None:
    context = ProcessorContext(
        None, SimpleNamespace(node_id="j"), (), logging.getLogger("t")
    )
    rows = _rows({"B02": [1, 2]}, representation="composite", times=["all"])
    with pytest.raises(TypeError, match="PointSampleArtifact"):
        JoinProcessor().execute_points(
            JoinNodeConfig(),
            {"a": PointSampleArtifact(rows, SimpleNamespace()), "b": object()},
            context,
        )


def test_pipeline_writes_one_parquet_for_two_terrain_nodes(tmp_path) -> None:
    path = _write_dem_project(tmp_path, "{type: dem.local, data_dir: dem}")
    path.write_text(
        path.read_text()
        .replace(
            "  - {id: terrain, type: dem, source: dem, features: [slope, aspect]}\n",
            "  - {id: slope, type: dem, source: dem, features: [slope]}\n"
            "  - {id: aspect, type: dem, source: dem, features: [aspect]}\n"
            "  - {id: terrain, type: join, inputs: {base: slope, other: aspect}}\n",
        )
    )

    run_pipeline(PipelineConfig.from_file(path), logging.getLogger("join-test"))
    rows = pd.read_parquet(tmp_path / "output" / "samples.parquet")

    assert len(rows) == 1
    assert rows["slope"].iloc[0] == pytest.approx(45.0, abs=0.01)
    assert rows["aspect"].iloc[0] == pytest.approx(90.0, abs=0.01)


def test_remote_example_pipeline_validates(tmp_path) -> None:
    import json
    import shutil
    from pathlib import Path

    example = Path(__file__).parents[1] / "examples" / "pavc_random_sample_remote_processing.yaml"
    (tmp_path / "examples").mkdir()
    shutil.copy(example, tmp_path / "examples" / example.name)
    aoi = tmp_path / "data" / "aois" / "pavc_random_sample.geojson"
    aoi.parent.mkdir(parents=True)
    aoi.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"site_visit_id": "a"},
                        "geometry": {"type": "Point", "coordinates": [-150.0, 68.0]},
                    }
                ],
            }
        )
    )

    config = PipelineConfig.from_file(tmp_path / "examples" / example.name)

    assert {source.source_type for source in config.sources.values()} == {
        "s2.l2a.remote",
        "dem.remote",
    }
    assert [node.node_id for node in config.ordered_nodes][-1] == "samples"
