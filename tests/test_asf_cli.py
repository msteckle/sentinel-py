import hashlib
import os
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
import requests
import shapely
from typer.testing import CliRunner

import sentinel_py.download.asf as asf_download_module
from sentinel_py.cli.main import app
from sentinel_py.download.asf import (
    ASF_MANIFEST_COLUMNS,
    ASFDownloadSummary,
    download_asf,
    get_predominant_flightdir,
    query_asf,
)

runner = CliRunner()


def _write_earthdata_config(path: Path) -> None:
    path.write_text(
        "machine urs.earthdata.nasa.gov\n"
        "    login test-user\n"
        "    password test-password\n"
    )


class EmptyASFResults:
    def geojson(self):
        return {"type": "FeatureCollection", "features": []}


class OneASFResult:
    def geojson(self):
        return {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {
                        "fileName": "one.zip",
                        "url": "https://example.test/one.zip",
                        "flightDirection": "ASCENDING",
                        "bytes": 1048576,
                        "md5sum": "abc123",
                    },
                }
            ],
        }


def test_query_asf_handles_no_results(monkeypatch):
    monkeypatch.setattr(
        "sentinel_py.download.asf.asf.search",
        lambda **kwargs: EmptyASFResults(),
    )

    manifest = query_asf(
        aoi_wkt="POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))",
        date_start="2024-01-01",
        date_end="2024-01-02",
        product_levels=["SLC"],
    )

    assert manifest.empty
    assert manifest.columns.tolist() == ASF_MANIFEST_COLUMNS


def test_query_asf_preserves_exact_size_and_checksum(monkeypatch):
    monkeypatch.setattr(
        "sentinel_py.download.asf.asf.search",
        lambda **kwargs: OneASFResult(),
    )

    manifest = query_asf(
        aoi_wkt="POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))",
        date_start="2024-01-01",
        date_end="2024-01-02",
        product_levels=["SLC"],
    )

    assert manifest.iloc[0]["expected_size"] == 1048576
    assert manifest.iloc[0]["sizeMB"] == 1.0
    assert manifest.iloc[0]["md5sum"] == "abc123"


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        (
            {"product_levels": ["RTC_HIGH_RES"]},
            "Unsupported Sentinel-1 ASF product level",
        ),
        (
            {"product_levels": ["GRD_HD"], "beam_mode": "FBS"},
            "Unsupported Sentinel-1 ASF beam mode",
        ),
        (
            {"product_levels": ["GRD_HD"], "polarization": "quadrature"},
            "Unsupported Sentinel-1 ASF polarization",
        ),
    ],
)
def test_query_asf_rejects_options_from_other_missions(kwargs, message):
    with pytest.raises(ValueError, match=message):
        query_asf(
            aoi_wkt="POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))",
            date_start="2024-01-01",
            date_end="2024-01-02",
            **kwargs,
        )


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
        ],
    )

    assert cached_result.exit_code == 0, cached_result.stderr
    assert query_calls == 1
    assert "Loaded cached ASF query" in cached_result.stdout


def test_predominant_flight_direction_tie_selects_ascending():
    manifest = pd.DataFrame({"flightDirection": ["DESCENDING", "ASCENDING"]})

    assert get_predominant_flightdir(manifest) == "ASCENDING"


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


def test_asf_query_cli_queries_each_year_and_reads_projected_shapefile(
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
        year = kwargs["date_start"][:4]
        return pd.DataFrame(
            [
                {
                    "granule": f"S1-{year}.zip",
                    "url": f"https://example.test/S1-{year}.zip",
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
    assert [(call["date_start"], call["date_end"]) for call in calls] == [
        ("2023-06-01", "2023-08-31"),
        ("2024-06-01", "2024-08-31"),
    ]
    cached_manifest = next(cache_dir.glob("*/manifest.parquet"))
    assert len(pd.read_parquet(cached_manifest)) == 2
    minx, miny, maxx, maxy = shapely.from_wkt(calls[0]["aoi_wkt"]).bounds
    assert minx == pytest.approx(-150.0)
    assert miny == pytest.approx(68.0)
    assert maxx == pytest.approx(-149.5)
    assert maxy == pytest.approx(68.5)


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
    _write_earthdata_config(config)
    calls = {}

    def fake_download_asf(**kwargs):
        calls.update(kwargs)
        return ASFDownloadSummary(
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
    _write_earthdata_config(config)
    calls = {}

    def fake_download_asf(**kwargs):
        calls.update(kwargs)
        return ASFDownloadSummary(1, 0, 0, ["download complete"])

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
    _write_earthdata_config(config)
    calls = {}

    def fake_download_asf(**kwargs):
        calls.update(kwargs)
        return ASFDownloadSummary(1, 0, 0, ["download complete"])

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
    _write_earthdata_config(config)
    monkeypatch.setattr(
        "sentinel_py.cli.asf.download.download_asf",
        lambda **kwargs: ASFDownloadSummary(
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
        ],
    )

    assert result.exit_code == 1
    assert "1 failed ASF product" in result.stderr


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


def test_download_asf_validates_size_persists_state_and_reports_progress(
    tmp_path: Path,
    monkeypatch,
    capsys,
):
    monkeypatch.setenv("COLUMNS", "200")
    monkeypatch.setattr(
        "sentinel_py.download.asf._build_asf_session",
        lambda *args: None,
    )
    products = pd.DataFrame(
        [
            {
                "granule": "one.zip",
                "url": "https://example.test/one.zip",
                "expected_size": 4,
                "sizeMB": 4 / (1024 * 1024),
                "md5sum": None,
            }
        ]
    )
    outdir = tmp_path / "downloads"
    download_calls = 0

    def fake_download_url(*, path, filename, **kwargs):
        nonlocal download_calls
        download_calls += 1
        (Path(path) / filename).write_bytes(b"data")

    monkeypatch.setattr(
        "sentinel_py.download.asf.asf_download_url",
        fake_download_url,
    )

    config = tmp_path / "unused.netrc"
    messages = download_asf(products, outdir, config_file=config, processes=1)

    target = outdir / "one.zip"
    state_file = outdir / ".sentinel-py" / "asf_downloads.parquet"
    assert target.read_bytes() == b"data"
    assert not (outdir / ".one.zip.part").exists()
    assert download_calls == 1
    assert "1 downloaded, 0 skipped, 0 failed" in messages[-2]
    state = pd.read_parquet(state_file)
    assert state.iloc[0]["status"] == "complete"
    assert state.iloc[0]["local_actual_size"] == 4
    progress_output = capsys.readouterr().out
    assert "Checking & downloading ASF products" in progress_output
    assert "downloaded 1 · skipped 0 · failed 0 · cache updated 1" in progress_output

    messages = download_asf(products, outdir, config_file=config, processes=1)

    assert download_calls == 1
    assert "0 downloaded, 1 skipped, 0 failed" in messages[-2]

    target.write_bytes(b"bad")
    messages = download_asf(products, outdir, config_file=config, processes=1)

    assert download_calls == 2
    assert target.read_bytes() == b"data"
    assert "1 downloaded, 0 skipped, 0 failed" in messages[-2]


def test_download_asf_validates_md5_for_new_and_existing_files(
    tmp_path: Path,
    monkeypatch,
):
    monkeypatch.setattr(
        "sentinel_py.download.asf._build_asf_session",
        lambda *args: None,
    )
    expected_bytes = b"correct"
    products = pd.DataFrame(
        [
            {
                "granule": "one.zip",
                "url": "https://example.test/one.zip",
                "expected_size": len(expected_bytes),
                "md5sum": hashlib.md5(expected_bytes).hexdigest(),
            }
        ]
    )
    outdir = tmp_path / "downloads"
    download_calls = 0

    def fake_download(*, path, filename, **kwargs):
        nonlocal download_calls
        download_calls += 1
        (Path(path) / filename).write_bytes(expected_bytes)

    monkeypatch.setattr(
        "sentinel_py.download.asf.asf_download_url",
        fake_download,
    )

    summary = download_asf(
        products,
        outdir,
        config_file=tmp_path / "unused.netrc",
        processes=1,
    )
    assert summary.failed == 0
    assert download_calls == 1
    state = pd.read_parquet(outdir / ".sentinel-py" / "asf_downloads.parquet")
    assert state.iloc[0]["local_mtime_ns"] == (outdir / "one.zip").stat().st_mtime_ns
    assert state.iloc[0]["fingerprint_version"] == 1

    # Simulate the short-lived legacy state format that rounded nanosecond mtimes
    # through float64. It should migrate without re-reading the ZIP.
    state["local_mtime_ns"] = state["local_mtime_ns"].astype(float)
    state = state.drop(columns=["fingerprint_version"])
    state.to_parquet(
        outdir / ".sentinel-py" / "asf_downloads.parquet",
        index=False,
    )

    original_file_md5 = asf_download_module._file_md5
    hash_calls = 0

    def counting_file_md5(path):
        nonlocal hash_calls
        hash_calls += 1
        return original_file_md5(path)

    monkeypatch.setattr(
        "sentinel_py.download.asf._file_md5",
        counting_file_md5,
    )
    summary = download_asf(
        products,
        outdir,
        config_file=tmp_path / "unused.netrc",
        processes=1,
    )
    assert summary.skipped == 1
    assert hash_calls == 0
    migrated_state = pd.read_parquet(outdir / ".sentinel-py" / "asf_downloads.parquet")
    assert (
        migrated_state.iloc[0]["local_mtime_ns"]
        == (outdir / "one.zip").stat().st_mtime_ns
    )
    assert migrated_state.iloc[0]["fingerprint_version"] == 1

    (outdir / "one.zip").write_bytes(b"badfile")
    summary = download_asf(
        products,
        outdir,
        config_file=tmp_path / "unused.netrc",
        processes=1,
    )
    assert summary.failed == 0
    assert download_calls == 2
    assert hash_calls == 2
    assert (outdir / "one.zip").read_bytes() == expected_bytes


def test_download_asf_retries_md5_mismatch_and_reports_failure(
    tmp_path: Path,
    monkeypatch,
):
    monkeypatch.setattr(
        "sentinel_py.download.asf._build_asf_session",
        lambda *args: None,
    )
    products = pd.DataFrame(
        [
            {
                "granule": "one.zip",
                "url": "https://example.test/one.zip",
                "expected_size": 4,
                "md5sum": hashlib.md5(b"good").hexdigest(),
            }
        ]
    )
    calls = 0

    def fake_download(*, path, filename, **kwargs):
        nonlocal calls
        calls += 1
        (Path(path) / filename).write_bytes(b"evil")

    monkeypatch.setattr(
        "sentinel_py.download.asf.asf_download_url",
        fake_download,
    )
    monkeypatch.setattr("sentinel_py.download.asf.time.sleep", lambda delay: None)

    summary = download_asf(
        products,
        tmp_path / "downloads",
        config_file=tmp_path / "unused.netrc",
        processes=1,
        retries=1,
    )

    assert calls == 2
    assert summary.failed == 1
    assert not (tmp_path / "downloads" / "one.zip").exists()
    state = pd.read_parquet(
        tmp_path / "downloads" / ".sentinel-py" / "asf_downloads.parquet"
    )
    assert "MD5 mismatch" in state.iloc[0]["error"]


def test_download_asf_batches_state_writes(tmp_path: Path, monkeypatch):
    outdir = tmp_path / "downloads"
    outdir.mkdir()
    products = []
    for index in range(101):
        filename = f"{index:03d}.zip"
        (outdir / filename).write_bytes(b"x")
        products.append(
            {
                "granule": filename,
                "url": f"https://example.test/{filename}",
                "expected_size": 1,
            }
        )

    original_write = asf_download_module.write_parquet_atomic
    write_calls = 0

    def counting_write(*args, **kwargs):
        nonlocal write_calls
        write_calls += 1
        return original_write(*args, **kwargs)

    monkeypatch.setattr(
        "sentinel_py.download.asf.write_parquet_atomic",
        counting_write,
    )

    summary = download_asf(
        pd.DataFrame(products),
        outdir,
        config_file=tmp_path / "unused.netrc",
        processes=8,
    )

    assert summary.skipped == 101
    assert write_calls == 2
    state = pd.read_parquet(outdir / ".sentinel-py" / "asf_downloads.parquet")
    assert len(state) == 101


def test_download_asf_does_not_replace_target_and_cleans_partial_file(
    tmp_path: Path,
    monkeypatch,
):
    monkeypatch.setattr(
        "sentinel_py.download.asf._build_asf_session",
        lambda *args: None,
    )
    products = pd.DataFrame(
        [
            {
                "granule": "one.zip",
                "url": "https://example.test/one.zip",
                "expected_size": 4,
            }
        ]
    )
    outdir = tmp_path / "downloads"
    outdir.mkdir()
    target = outdir / "one.zip"
    target.write_bytes(b"old")

    def fake_short_download(*, path, filename, **kwargs):
        (Path(path) / filename).write_bytes(b"xx")

    monkeypatch.setattr(
        "sentinel_py.download.asf.asf_download_url",
        fake_short_download,
    )

    messages = download_asf(
        products,
        outdir,
        config_file=tmp_path / "unused.netrc",
        processes=1,
    )

    assert target.read_bytes() == b"old"
    assert not (outdir / ".one.zip.part").exists()
    assert "0 downloaded, 0 skipped, 1 failed" in messages[-2]
    state = pd.read_parquet(outdir / ".sentinel-py" / "asf_downloads.parquet")
    assert state.iloc[0]["status"] == "failed"


def test_download_asf_retries_transient_failure_then_succeeds(
    tmp_path: Path,
    monkeypatch,
):
    monkeypatch.setattr(
        "sentinel_py.download.asf._build_asf_session",
        lambda *args: None,
    )
    products = pd.DataFrame(
        [
            {
                "granule": "one.zip",
                "url": "https://example.test/one.zip",
                "expected_size": 4,
            }
        ]
    )
    calls = 0
    sleeps = []

    def fake_download(*, path, filename, **kwargs):
        nonlocal calls
        calls += 1
        if calls < 3:
            raise requests.exceptions.ConnectionError("temporary DNS failure")
        (Path(path) / filename).write_bytes(b"data")

    monkeypatch.setattr(
        "sentinel_py.download.asf.asf_download_url",
        fake_download,
    )
    monkeypatch.setattr("sentinel_py.download.asf.time.sleep", sleeps.append)

    messages = download_asf(
        products,
        tmp_path / "downloads",
        config_file=tmp_path / "unused.netrc",
        processes=1,
        retries=3,
        retry_backoff=0.5,
    )

    assert calls == 3
    assert sleeps == [0.5, 1.0]
    assert (tmp_path / "downloads" / "one.zip").read_bytes() == b"data"
    assert "1 downloaded, 0 skipped, 0 failed" in messages[-2]


def test_download_asf_cleans_partial_file_on_keyboard_interrupt(
    tmp_path: Path,
    monkeypatch,
):
    monkeypatch.setattr(
        "sentinel_py.download.asf._build_asf_session",
        lambda *args: None,
    )
    products = pd.DataFrame(
        [
            {
                "granule": "one.zip",
                "url": "https://example.test/one.zip",
                "expected_size": 4,
            }
        ]
    )
    outdir = tmp_path / "downloads"
    outdir.mkdir()

    def fake_interrupted_download(*, path, filename, **kwargs):
        (Path(path) / filename).write_bytes(b"xx")
        raise KeyboardInterrupt

    monkeypatch.setattr(
        "sentinel_py.download.asf.asf_download_url",
        fake_interrupted_download,
    )

    with pytest.raises(KeyboardInterrupt):
        download_asf(
            products,
            outdir,
            config_file=tmp_path / "unused.netrc",
            processes=1,
        )

    assert not (outdir / ".one.zip.part").exists()
    assert not (outdir / "one.zip").exists()
