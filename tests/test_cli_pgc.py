"""PGC ``query`` and ``download`` CLI commands."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from helpers import runner, write_polygon_aoi
from sentinel_py.cli.main import app
from sentinel_py.download.base import DownloadSummary


def _manifest(path: Path) -> None:
    pd.DataFrame(
        {
            "url": ["https://example.test/10m/12_34.tif"],
            "filename": ["12_34.tif"],
            "expected_size": [4],
        }
    ).to_parquet(path, index=False)


def test_pgc_query_caches_manifest_and_reuses_it(tmp_path: Path, monkeypatch):
    aoi = tmp_path / "aoi.geojson"
    write_polygon_aoi(aoi)
    calls = []

    def fake_query(geometries, **kwargs):
        calls.append(len(geometries))
        return pd.DataFrame({"url": ["https://example.test/a.tif"], "filename": ["a.tif"]})

    monkeypatch.setattr("sentinel_py.cli.pgc.query.query_arcticdem", fake_query)
    args = ["pgc", "query", "--aoi", str(aoi), "--cache-dir", str(tmp_path / "cache")]

    first = runner.invoke(app, args)
    second = runner.invoke(app, args)

    assert first.exit_code == 0, first.output
    assert "Cached PGC query" in first.output
    assert "Found 1 unique ArcticDEM tile(s)." in first.output
    assert second.exit_code == 0, second.output
    assert "Loaded cached PGC query" in second.output
    assert calls == [1]  # the second invocation never queried the catalog


def test_pgc_download_runs_a_manifest_and_prints_the_summary(
    tmp_path: Path, monkeypatch
):
    manifest = tmp_path / "manifest.parquet"
    _manifest(manifest)
    seen = {}

    def fake_download(products, outdir, **kwargs):
        seen["rows"] = len(products)
        seen["processes"] = kwargs["processes"]
        return DownloadSummary(1, 0, 0, ["Results: 1 downloaded"])

    monkeypatch.setattr("sentinel_py.cli.pgc.download.download_arcticdem", fake_download)

    result = runner.invoke(
        app,
        [
            "pgc", "download",
            "--outdir", str(tmp_path / "out"),
            "--query", str(manifest),
            "--processes", "2",
            "--yes",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Summary:" in result.output and "Results: 1 downloaded" in result.output
    assert seen == {"rows": 1, "processes": 2}


def test_pgc_download_exits_nonzero_when_a_tile_fails(tmp_path: Path, monkeypatch):
    manifest = tmp_path / "manifest.parquet"
    _manifest(manifest)
    monkeypatch.setattr(
        "sentinel_py.cli.pgc.download.download_arcticdem",
        lambda *args, **kwargs: DownloadSummary(0, 0, 1, ["Results: 1 failed"]),
    )

    result = runner.invoke(
        app,
        [
            "pgc", "download",
            "--outdir", str(tmp_path / "out"),
            "--query", str(manifest),
            "--yes",
        ],
    )

    assert result.exit_code == 1


def test_pgc_download_without_a_cached_query_explains_what_to_run(tmp_path: Path):
    result = runner.invoke(
        app,
        [
            "pgc", "download",
            "--outdir", str(tmp_path / "out"),
            "--cache-dir", str(tmp_path / "empty-cache"),
            "--yes",
        ],
    )

    assert result.exit_code != 0
    assert "No cached PGC query manifest" in result.output


# `pgc missing` ---------------------------------------------------------------------


def _missing(args):
    from helpers import runner

    from sentinel_py.cli.main import app

    return runner.invoke(app, ["pgc", "missing", *args])


def test_pgc_missing_writes_a_usable_aoi_and_exits_nonzero(tmp_path):
    import geopandas as gpd
    from helpers import write_s1_project

    from sentinel_py.cli.options import load_aoi_geometries

    pipeline = write_s1_project(tmp_path, dem=False)
    result = _missing(["--pipeline", str(pipeline)])

    assert result.exit_code == 1, result.output
    destination = tmp_path / "pipeline_missing_dem.geojson"
    assert destination.is_file()
    frame = gpd.read_file(destination)
    assert set(frame["kind"]) == {"required", "margin"}
    required = frame[frame["kind"] == "required"].iloc[0]
    assert required["scenes"] == 2 and required["area_km2"] > 0
    assert frame.crs.to_string() == "EPSG:4326"
    assert "blocks processing" in result.output
    assert f"sentinel-py pgc query --aoi {destination}" in result.output
    assert f"sentinel-py pgc download --outdir {(tmp_path / 'dem').resolve()}" in result.output
    # The written file is exactly what `pgc query --aoi` consumes.
    assert load_aoi_geometries(destination, "EPSG:4326")


def test_pgc_missing_options_and_covered_pipeline(tmp_path):
    import geopandas as gpd
    from helpers import write_s1_project

    pipeline = write_s1_project(tmp_path, dem=False)
    custom = tmp_path / "out" / "gaps.geojson"
    result = _missing(
        ["--pipeline", str(pipeline), "--output", str(custom), "--no-margin"]
    )
    assert result.exit_code == 1
    assert set(gpd.read_file(custom)["kind"]) == {"required"}

    covered = tmp_path / "covered"
    covered.mkdir()
    ok = _missing(["--pipeline", str(write_s1_project(covered))])
    assert ok.exit_code == 0
    assert "covered by the local DEM tiles" in ok.output
    assert not (covered / "pipeline_missing_dem.geojson").exists()


def test_pgc_missing_margin_only_gaps(tmp_path):
    from helpers import write_s1_project

    pipeline = write_s1_project(tmp_path, node_extra="    dem_margin_m: 150000")
    assert _missing(["--pipeline", str(pipeline)]).exit_code == 1
    assert "DEM margin only" in _missing(["--pipeline", str(pipeline)]).output
    assert _missing(["--pipeline", str(pipeline), "--no-margin"]).exit_code == 0


def test_pgc_missing_handles_other_pipelines_and_bad_input(tmp_path):
    from helpers import write_s2_pipeline_yaml

    s2 = write_s2_pipeline_yaml(tmp_path)
    result = _missing(["--pipeline", str(s2)])
    assert result.exit_code == 0 and "no snap_gpt nodes" in result.output

    bad = tmp_path / "bad.yaml"
    bad.write_text("version: 1\n")
    failed = _missing(["--pipeline", str(bad)])
    assert failed.exit_code == 2 and "Invalid pipeline" in failed.output
    assert _missing(["--pipeline", str(tmp_path / "missing.yaml")]).exit_code == 2
