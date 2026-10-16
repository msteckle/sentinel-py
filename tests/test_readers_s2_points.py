"""Characterization of Sentinel-2 point sampling through the pipeline."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd

from sentinel_py.pipeline import PipelineConfig
from sentinel_py.pipeline.run import run_pipeline
from test_s2_lazy import _synthetic_pipeline


def _point_project(tmp_path: Path, workers: int = 1) -> Path:
    """The synthetic one-granule archive sampled at two points (one cloud-masked)."""
    pipeline, _ = _synthetic_pipeline(tmp_path)
    (tmp_path / "aoi.geojson").write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "crs": {"type": "name", "properties": {"name": "EPSG:32606"}},
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"site": name},
                        "geometry": {"type": "Point", "coordinates": coordinates},
                    }
                    for name, coordinates in (
                        ("clear", [500010.0, 999990.0]),
                        ("cloudy", [500030.0, 999970.0]),
                    )
                ],
            }
        )
    )
    pipeline.write_text(
        pipeline.read_text()
        .replace("execution: {method: local}", f"execution: {{method: local, workers: {workers}}}")
        .replace("path: unused, format: xarray", f"path: {tmp_path / 'points'}, format: parquet")
    )
    return pipeline


def _sample(pipeline: Path) -> pd.DataFrame:
    run_pipeline(PipelineConfig.from_file(pipeline), logging.getLogger("s2-points-test"))
    rows = pd.read_parquet(pipeline.parent / "points" / "samples.parquet")
    return rows.sort_values("aoi_id").reset_index(drop=True)


def test_points_yield_one_row_per_scene_and_point_with_masked_nodata(tmp_path) -> None:
    rows = _sample(_point_project(tmp_path))

    assert rows["aoi_id"].tolist() == ["clear", "cloudy"]
    assert rows["time"].unique().tolist() == ["20240615T120000"]
    assert rows["time_representation"].unique().tolist() == ["acquisition"]
    assert rows["quantification_value"].tolist() == [10000, 10000]
    # The clear pixel keeps its offset-corrected DN; the cloudy one is SCL class 9.
    assert rows["B02"].tolist() == [300, 65535]
    assert rows["B03"].tolist() == [0, 65535]
    assert rows["source_fingerprint"].nunique() == 1  # one granule, one fingerprint


def test_point_rows_do_not_depend_on_the_worker_count(tmp_path) -> None:
    one = _sample(_point_project(tmp_path / "one", workers=1))
    two = _sample(_point_project(tmp_path / "two", workers=2))

    # Provenance fingerprints hash absolute file paths, so they differ between trees.
    columns = [column for column in one.columns if column != "source_fingerprint"]
    pd.testing.assert_frame_equal(one[columns], two[columns])


def test_points_are_split_into_batches_without_changing_rows(tmp_path, monkeypatch) -> None:
    from sentinel_py.pipeline.readers import s2

    whole = _sample(_point_project(tmp_path / "whole"))
    monkeypatch.setattr(s2, "POINTS_PER_TASK", 1)
    events = []
    project = _point_project(tmp_path / "split")
    run_pipeline(
        PipelineConfig.from_file(project),
        logging.getLogger("s2-points-test"),
        node_progress_callback=lambda *event: events.append(event[1:]),
    )
    split = (
        pd.read_parquet(project.parent / "points" / "samples.parquet")
        .sort_values("aoi_id")
        .reset_index(drop=True)
    )

    columns = [column for column in whole.columns if column != "source_fingerprint"]
    pd.testing.assert_frame_equal(whole[columns], split[columns])
    # Progress counts scene/point pairs: starts at 0 and ends complete.
    assert events[0] == (0, 2) and events[-1] == (2, 2)
