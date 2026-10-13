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
