"""ASF ``query`` and ``download`` CLI commands."""

import json
import os
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
import shapely

from helpers import runner, write_earthdata_config
from sentinel_py.cli.main import app
from sentinel_py.download.base import DownloadSummary


def test_asf_query_help_lists_sourced_sentinel1_options():
    result = runner.invoke(
        app,
        ["asf", "query", "--help"],
        terminal_width=160,
    )
    normalized_help = " ".join(result.stdout.replace("│", " ").split())
    uppercase_help = normalized_help.upper()

    assert result.exit_code == 0
    assert "GRD_HD" in uppercase_help
    assert "GRD_HS" in uppercase_help
    assert "GRD_MD" in uppercase_help
    assert "GRD_MS" in uppercase_help
    assert "GRD_FD" in uppercase_help
    assert "SLC" in uppercase_help
    assert "RAW" in uppercase_help
    assert "OCN" in uppercase_help
    assert "IW" in uppercase_help
    assert "EW" in uppercase_help
    assert "S1-S6" in uppercase_help
    assert "VV+VH" in uppercase_help
    assert "HH+HV" in uppercase_help
    assert "ASF Search API keyword" in normalized_help
    assert "ESA" in normalized_help
    assert "Sentinel-1 Mission" in normalized_help


def test_asf_query_help_groups_options_into_named_panels():
    result = runner.invoke(app, ["asf", "query", "--help"])

    assert result.exit_code == 0
    arguments = result.stdout.index("Required Arguments")
    other_options = result.stdout.index("Optional Query Configurations")
    utils = result.stdout.index("Utils")
    assert arguments < other_options < utils

    arguments_panel = result.stdout[arguments:other_options]
    other_options_panel = result.stdout[other_options:utils]
    utils_panel = result.stdout[utils:]

    for option in ("--aoi", "--years"):
        assert option in arguments_panel
    for option in (
        "--speriod",
        "--eperiod",
        "--product-levels",
        "--beam-mode",
        "--flight-direction",
        "--polarization",
        "--relative-orbit",
        "--max-results",
    ):
        assert option in other_options_panel
    for option in ("--cache-dir", "--crs", "--log", "--verbose"):
        assert option in utils_panel


def test_asf_query_parses_enum_options_before_querying(tmp_path: Path, monkeypatch):
    aoi = tmp_path / "aoi.geojson"
    gpd.GeoDataFrame(
        geometry=[shapely.box(-150.0, 68.0, -149.5, 68.5)],
        crs="EPSG:4326",
    ).to_file(aoi, driver="GeoJSON")
    calls = []

    def fake_query_asf(**kwargs):
        calls.append(kwargs)
        return pd.DataFrame()

    monkeypatch.setattr("sentinel_py.cli.asf.query.query_asf", fake_query_asf)

    result = runner.invoke(
        app,
        [
            "asf",
            "query",
            "--aoi",
            str(aoi),
            "--years",
            "2024",
            "--product-levels",
            "slc",
            "--beam-mode",
            "ew",
            "--flight-direction",
            "ascending",
            "--polarization",
            "hh+hv",
            "--cache-dir",
            str(tmp_path / ".asf-cache"),
            "--log",
            str(tmp_path / "asf-query"),
            "--verbose",
        ],
    )

    assert result.exit_code == 0, result.stderr
    assert len(calls) == 1
    assert calls[0]["product_levels"] == ["SLC"]
    assert calls[0]["beam_mode"] == "EW"
    assert calls[0]["flight_direction"] == "ASCENDING"
    assert calls[0]["polarization"] == "HH+HV"
    assert type(calls[0]["product_levels"][0]) is str
    assert type(calls[0]["beam_mode"]) is str
    assert type(calls[0]["flight_direction"]) is str
    assert type(calls[0]["polarization"]) is str
    assert calls[0]["logger"].name == "asf_query_logger"
    log_files = list(tmp_path.glob("asf-query_*.log"))
    assert len(log_files) == 1
    log_text = log_files[0].read_text()
    assert ":asf_query_logger:" in log_text
    assert "ASF query complete: 0 unique products" in log_text


def test_asf_query_cli_caches_manifest(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    aoi = tmp_path / "aoi.geojson"
    aoi.write_text(
        """
        {
          "type": "FeatureCollection",
          "features": [{
            "type": "Feature",
            "properties": {},
            "geometry": {
              "type": "Polygon",
              "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]
            }
          }]
        }
        """
    )
    expected = pd.DataFrame(
        [
            {
                "granule": "S1-ascending-1.zip",
                "url": "https://example.test/S1-ascending-1.zip",
                "beamMode": "IW",
                "flightDirection": "ASCENDING",
                "polarization": "VV+VH",
                "relativeOrbit": 12,
                "startTime": "2024-01-01T00:00:00Z",
                "stopTime": "2024-01-01T00:01:00Z",
                "sizeMB": 100.0,
            },
            {
                "granule": "S1-ascending-2.zip",
                "url": "https://example.test/S1-ascending-2.zip",
                "beamMode": "IW",
                "flightDirection": "ASCENDING",
                "polarization": "VV+VH",
                "relativeOrbit": 12,
                "startTime": "2024-01-02T00:00:00Z",
                "stopTime": "2024-01-02T00:01:00Z",
                "sizeMB": 100.0,
            },
            {
                "granule": "S1-descending.zip",
                "url": "https://example.test/S1-descending.zip",
                "beamMode": "IW",
                "flightDirection": "DESCENDING",
                "polarization": "VV+VH",
                "relativeOrbit": 12,
                "startTime": "2024-01-03T00:00:00Z",
                "stopTime": "2024-01-03T00:01:00Z",
                "sizeMB": 100.0,
            },
        ]
    )
    query_args = {}
    query_calls = 0

    def fake_query_asf(**kwargs):
        nonlocal query_calls
        query_calls += 1
        query_args.update(kwargs)
        return expected

    monkeypatch.setattr("sentinel_py.cli.asf.query.query_asf", fake_query_asf)

    result = runner.invoke(
        app,
        [
            "asf",
            "query",
            "--aoi",
            str(aoi),
            "--years",
            "2024",
            "--speriod",
            "06-01",
            "--eperiod",
            "08-31",
            "--flight-direction",
            "predominant",
            "--cache-dir",
            str(tmp_path / ".asf-cache"),
        ],
    )

    assert result.exit_code == 0, result.stderr
    cached_manifests = list((tmp_path / ".asf-cache").glob("*/manifest.parquet"))
    assert len(cached_manifests) == 1
    written = pd.read_parquet(cached_manifests[0])
    assert written["flightDirection"].unique().tolist() == ["ASCENDING"]
    assert len(written) == 2
    assert query_args["beam_mode"] == "IW"
    assert query_args["product_levels"] == ["GRD_HD"]
    assert query_args["flight_direction"] is None
    assert query_args["polarization"] == "VV+VH"
    assert query_args["date_start"] == "2024-06-01"
    assert query_args["date_end"] == "2024-08-31"
    assert query_args["season"] == [152, 243]
    assert "ASCENDING=2, DESCENDING=1; selected ASCENDING" in result.stdout
    assert "Found 2 unique ASF product" in result.stdout
    assert (tmp_path / ".asf-cache").is_dir()

    cached_result = runner.invoke(
        app,
        [
            "asf",
            "query",
            "--aoi",
            str(aoi),
            "--years",
            "2024",
            "--speriod",
            "06-01",
            "--eperiod",
            "08-31",
            "--flight-direction",
            "predominant",
            "--cache-dir",
            str(tmp_path / ".asf-cache"),
        ],
    )

    assert cached_result.exit_code == 0, cached_result.stderr
    assert query_calls == 1
    assert "Loaded cached ASF query" in cached_result.stdout


def test_asf_query_cli_defaults_to_both_directions(tmp_path: Path, monkeypatch):
    aoi = tmp_path / "aoi.geojson"
    gpd.GeoDataFrame(
        geometry=[shapely.box(-150.0, 68.0, -149.5, 68.5)],
        crs="EPSG:4326",
    ).to_file(aoi, driver="GeoJSON")
    expected = pd.DataFrame(
        [
            {
                "granule": "ascending.zip",
                "url": "https://example.test/ascending.zip",
                "flightDirection": "ASCENDING",
            },
            {
                "granule": "descending.zip",
                "url": "https://example.test/descending.zip",
                "flightDirection": "DESCENDING",
            },
        ]
    )
    calls = []

    def fake_query_asf(**kwargs):
        calls.append(kwargs)
        return expected

    monkeypatch.setattr("sentinel_py.cli.asf.query.query_asf", fake_query_asf)

    result = runner.invoke(
        app,
        [
            "asf",
            "query",
            "--aoi",
            str(aoi),
            "--years",
            "2024",
            "--cache-dir",
            str(tmp_path / ".asf-cache"),
        ],
    )

    assert result.exit_code == 0, result.stderr
    assert calls[0]["flight_direction"] is None
    assert calls[0]["product_levels"] == ["GRD_HD"]
    manifest = pd.read_parquet(
        next((tmp_path / ".asf-cache").glob("*/manifest.parquet"))
    )
    assert set(manifest["flightDirection"]) == {"ASCENDING", "DESCENDING"}
    assert "Found 2 unique ASF product" in result.stdout


def test_asf_query_warns_when_max_results_may_truncate_window(
    tmp_path: Path,
    monkeypatch,
):
    aoi = tmp_path / "aoi.geojson"
    gpd.GeoDataFrame(
        geometry=[shapely.box(-150.0, 68.0, -149.5, 68.5)],
        crs="EPSG:4326",
    ).to_file(aoi, driver="GeoJSON")
    monkeypatch.setattr(
        "sentinel_py.cli.asf.query.query_asf",
        lambda **kwargs: pd.DataFrame(
            [
                {
                    "granule": "one.zip",
                    "url": "https://example.test/one.zip",
                    "flightDirection": "ASCENDING",
                }
            ]
        ),
    )

    args = [
        "asf",
        "query",
        "--aoi",
        str(aoi),
        "--years",
        "2024",
        "--max-results",
        "1",
        "--cache-dir",
        str(tmp_path / ".asf-cache"),
    ]
    result = runner.invoke(app, args)

    assert result.exit_code == 0
    assert "cached manifest may be truncated" in result.stderr

    cached_result = runner.invoke(app, args)
    assert cached_result.exit_code == 0
    assert "cached manifest may be truncated" in cached_result.stderr


def test_asf_query_cli_uses_one_seasonal_range_and_reads_projected_shapefile(
    tmp_path: Path,
    monkeypatch,
):
    aoi = tmp_path / "aoi.shp"
    gpd.GeoDataFrame(
        geometry=[
            shapely.box(-150.0, 68.0, -149.5, 68.5),
        ],
        crs="EPSG:4326",
    ).to_crs("EPSG:3338").to_file(aoi)
    cache_dir = tmp_path / ".asf-cache"
    calls: list[dict] = []

    def fake_query_asf(**kwargs):
        calls.append(kwargs)
        return pd.DataFrame(
            [
                {
                    "granule": "S1-seasonal.zip",
                    "url": "https://example.test/S1-seasonal.zip",
                    "flightDirection": "ASCENDING",
                }
            ]
        )

    monkeypatch.setattr("sentinel_py.cli.asf.query.query_asf", fake_query_asf)

    result = runner.invoke(
        app,
        [
            "asf",
            "query",
            "--aoi",
            str(aoi),
            "--years",
            "2023, 2024",
            "--speriod",
            "06-01",
            "--eperiod",
            "08-31",
            "--cache-dir",
            str(cache_dir),
        ],
    )

    assert result.exit_code == 0, result.stderr
    assert len(calls) == 1
    assert calls[0]["date_start"] == "2023-06-01"
    assert calls[0]["date_end"] == "2024-08-31"
    assert calls[0]["season"] == [152, 243]
    cached_manifest = next(cache_dir.glob("*/manifest.parquet"))
    assert len(pd.read_parquet(cached_manifest)) == 1
    minx, miny, maxx, maxy = shapely.from_wkt(calls[0]["aoi_wkt"]).bounds
    assert minx == pytest.approx(-150.0)
    assert miny == pytest.approx(68.0)
    assert maxx == pytest.approx(-149.5)
    assert maxy == pytest.approx(68.5)


def test_asf_query_cli_preserves_multipoint_for_cmr_point_or(
    tmp_path: Path,
    monkeypatch,
):
    aoi = tmp_path / "points.geojson"
    gpd.GeoDataFrame(
        geometry=[shapely.MultiPoint([(-150.0, 68.0), (20.0, -30.0)])],
        crs="EPSG:4326",
    ).to_file(aoi, driver="GeoJSON")
    cache_dir = tmp_path / ".asf-cache"
    calls: list[dict] = []

    def fake_query_asf(**kwargs):
        calls.append(kwargs)
        return pd.DataFrame(
            [
                {
                    "granule": "shared.zip",
                    "url": "https://example.test/shared.zip",
                    "flightDirection": "ASCENDING",
                }
            ]
        )

    monkeypatch.setattr("sentinel_py.cli.asf.query.query_asf", fake_query_asf)

    result = runner.invoke(
        app,
        [
            "asf",
            "query",
            "--aoi",
            str(aoi),
            "--years",
            "2023,2024",
            "--cache-dir",
            str(cache_dir),
        ],
    )

    assert result.exit_code == 0, result.stderr
    assert len(calls) == 1
    assert calls[0]["season"] == [1, 365]
    assert all(
        shapely.from_wkt(call["aoi_wkt"]).geom_type == "MultiPoint" for call in calls
    )
    cached_manifest = next(cache_dir.glob("*/manifest.parquet"))
    assert len(pd.read_parquet(cached_manifest)) == 1
    query_info = json.loads(next(cache_dir.glob("*/query_info.json")).read_text())
    assert query_info["spatial_query_strategy"] == "cmr_point_or_v1"


def test_asf_download_cli_reads_cached_query_and_config(tmp_path: Path, monkeypatch):
    cache_dir = tmp_path / ".asf-cache"
    manifest = cache_dir / "query" / "manifest.parquet"
    manifest.parent.mkdir(parents=True)
    pd.DataFrame(
        {
            "url": [
                "https://example.test/one.zip",
                "https://example.test/one.zip",
            ]
        }
    ).to_parquet(manifest, index=False)
    config = tmp_path / "earthdata.netrc"
    write_earthdata_config(config)
    calls = {}

    def fake_download_asf(**kwargs):
        calls.update(kwargs)
        return DownloadSummary(
            1,
            0,
            0,
            [
                "Download started: 2026-07-24 10:00:00",
                "Download ended:   2026-07-24 10:00:01",
                "Elapsed time:     1.0 seconds for 1 files",
                "Results:          1 downloaded, 0 skipped, 0 failed",
                "Download status:  /tmp/asf_downloads.parquet",
            ],
        )

    monkeypatch.setattr(
        "sentinel_py.cli.asf.download.download_asf",
        fake_download_asf,
    )

    result = runner.invoke(
        app,
        [
            "asf",
            "download",
            "--outdir",
            str(tmp_path / "downloads"),
            "--cache-dir",
            str(cache_dir),
            "--config",
            str(config),
            "--processes",
            "2",
            "--retries",
            "5",
            "--yes",
            "--log",
            str(tmp_path / "asf-download"),
            "--verbose",
        ],
    )

    assert result.exit_code == 0, result.stderr
    assert calls["products"]["url"].tolist() == ["https://example.test/one.zip"]
    assert calls["out_dir"] == tmp_path / "downloads"
    assert calls["config_file"] == config
    assert calls["processes"] == 2
    assert calls["retries"] == 5
    assert type(calls["processes"]) is int
    assert type(calls["retries"]) is int
    assert calls["logger"].name == "asf_download_logger"
    log_files = list(tmp_path.glob("asf-download_*.log"))
    assert len(log_files) == 1
    log_text = log_files[0].read_text()
    assert ":asf_download_logger:" in log_text
    assert "Downloading 1 unique ASF products" in log_text
    assert f"Cached query: {manifest}" in result.stdout
    assert result.stdout.count(str(manifest)) == 1
    assert "Found 1 scenes: 1 ZIP file" in result.stdout
    assert "Downloading 1 unique ASF product" not in result.stdout
    assert "Summary:" in result.stdout
    assert "Download started:" in result.stdout
    assert "Download ended:" in result.stdout
    assert "Elapsed time:" in result.stdout
    assert "Results:          1 downloaded, 0 skipped, 0 failed" in result.stdout
    assert "Download status:  /tmp/asf_downloads.parquet" in result.stdout


def test_asf_download_cli_uses_latest_cached_query(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    cache_dir = Path(".asf-cache")
    older = cache_dir / "older" / "manifest.parquet"
    latest = cache_dir / "latest" / "manifest.parquet"
    older.parent.mkdir(parents=True)
    latest.parent.mkdir(parents=True)
    pd.DataFrame({"url": ["https://example.test/older.zip"]}).to_parquet(
        older, index=False
    )
    pd.DataFrame({"url": ["https://example.test/latest.zip"]}).to_parquet(
        latest, index=False
    )
    os.utime(older, (1, 1))
    os.utime(latest, (2, 2))

    config = tmp_path / "earthdata.netrc"
    write_earthdata_config(config)
    calls = {}

    def fake_download_asf(**kwargs):
        calls.update(kwargs)
        return DownloadSummary(1, 0, 0, ["download complete"])

    monkeypatch.setattr(
        "sentinel_py.cli.asf.download.download_asf",
        fake_download_asf,
    )

    result = runner.invoke(
        app,
        [
            "asf",
            "download",
            "--outdir",
            str(tmp_path / "downloads"),
            "--config",
            str(config),
            "--cache-dir",
            str(cache_dir),
            "--yes",
        ],
    )

    assert result.exit_code == 0, result.stderr
    assert calls["products"]["url"].tolist() == ["https://example.test/latest.zip"]
    assert f"Cached query: {latest}" in result.stdout


def test_asf_download_cli_uses_explicit_query_manifest(
    tmp_path: Path,
    monkeypatch,
):
    cache_dir = tmp_path / ".asf-cache"
    cached = cache_dir / "latest" / "manifest.parquet"
    cached.parent.mkdir(parents=True)
    pd.DataFrame({"url": ["https://example.test/cached.zip"]}).to_parquet(
        cached, index=False
    )
    explicit = tmp_path / "selected.parquet"
    pd.DataFrame({"url": ["https://example.test/selected.zip"]}).to_parquet(
        explicit, index=False
    )
    config = tmp_path / "earthdata.netrc"
    write_earthdata_config(config)
    calls = {}

    def fake_download_asf(**kwargs):
        calls.update(kwargs)
        return DownloadSummary(1, 0, 0, ["download complete"])

    monkeypatch.setattr(
        "sentinel_py.cli.asf.download.download_asf",
        fake_download_asf,
    )

    result = runner.invoke(
        app,
        [
            "asf",
            "download",
            "--outdir",
            str(tmp_path / "downloads"),
            "--cache-dir",
            str(cache_dir),
            "--query",
            str(explicit),
            "--config",
            str(config),
            "--yes",
        ],
    )

    assert result.exit_code == 0, result.stderr
    assert calls["products"]["url"].tolist() == ["https://example.test/selected.zip"]
    assert f"Cached query: {explicit}" in result.stdout


def test_asf_download_cli_exits_nonzero_when_a_product_fails(
    tmp_path: Path,
    monkeypatch,
):
    manifest = tmp_path / "manifest.parquet"
    pd.DataFrame({"url": ["https://example.test/failed.zip"]}).to_parquet(
        manifest, index=False
    )
    config = tmp_path / "earthdata.netrc"
    write_earthdata_config(config)
    monkeypatch.setattr(
        "sentinel_py.cli.asf.download.download_asf",
        lambda **kwargs: DownloadSummary(
            0,
            0,
            1,
            ["Results:          0 downloaded, 0 skipped, 1 failed"],
        ),
    )

    result = runner.invoke(
        app,
        [
            "asf",
            "download",
            "--outdir",
            str(tmp_path / "downloads"),
            "--query",
            str(manifest),
            "--config",
            str(config),
            "--yes",
        ],
    )

    assert result.exit_code == 1
    assert "1 failed ASF product" in result.stderr


def test_asf_download_cli_reports_storage_and_can_cancel(tmp_path: Path, monkeypatch):
    manifest = tmp_path / "manifest.parquet"
    pd.DataFrame(
        {
            "url": [
                "https://example.test/already.zip",
                "https://example.test/missing.zip",
            ],
            "expected_size": [100, 200],
        }
    ).to_parquet(manifest, index=False)
    config = tmp_path / "earthdata.netrc"
    write_earthdata_config(config)
    outdir = tmp_path / "downloads"
    outdir.mkdir()
    (outdir / "already.zip").write_bytes(b"x" * 100)

    called = False

    def fake_download_asf(**kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(
        "sentinel_py.cli.asf.download.download_asf",
        fake_download_asf,
    )

    result = runner.invoke(
        app,
        [
            "asf",
            "download",
            "--outdir",
            str(outdir),
            "--query",
            str(manifest),
            "--config",
            str(config),
        ],
        input="n\n",
    )

    assert result.exit_code == 0, result.stderr
    assert not called
    assert f"Cached query: {manifest}" in result.stdout
    assert "Known dataset size: 300 B" in result.stdout
    assert "Known additional storage needed: 200 B" in result.stdout
    assert "Continue with download? [y/N]: n" in result.stdout
    assert "Download cancelled." in result.stdout


def test_asf_download_cli_explains_invalid_config(tmp_path: Path):
    cache_dir = tmp_path / ".asf-cache"
    manifest = cache_dir / "query" / "manifest.parquet"
    manifest.parent.mkdir(parents=True)
    pd.DataFrame({"url": ["https://example.test/one.zip"]}).to_parquet(
        manifest, index=False
    )
    config = tmp_path / "invalid.netrc"
    config.write_text("machine example.test login nobody password nothing\n")

    result = runner.invoke(
        app,
        [
            "asf",
            "download",
            "--outdir",
            str(tmp_path / "downloads"),
            "--cache-dir",
            str(cache_dir),
            "--config",
            str(config),
        ],
    )

    assert result.exit_code != 0
    assert "Invalid Earthdata credentials file" in result.stderr
    assert "machine urs.earthdata.nasa.gov" in result.stderr
    assert "chmod 600" in result.stderr
