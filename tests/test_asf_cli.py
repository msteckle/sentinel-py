import os
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
import requests
import shapely
from typer.testing import CliRunner

from sentinel_py.cli.main import app
from sentinel_py.download.asf import (
    ASF_MANIFEST_COLUMNS,
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
        ],
    )

    assert result.exit_code == 0, result.stderr
    cached_manifests = list((tmp_path / ".asf-cache").glob("*/manifest.parquet"))
    assert len(cached_manifests) == 1
    written = pd.read_parquet(cached_manifests[0])
    assert written["flightDirection"].unique().tolist() == ["ASCENDING"]
    assert len(written) == 2
    assert query_args["beam_mode"] == "IW"
    assert query_args["product_levels"] == [
        "GRD_HD",
        "GRD_FD",
        "GRD_HS",
        "GRD_MD",
        "GRD_MS",
    ]
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
        ],
    )

    assert cached_result.exit_code == 0, cached_result.stderr
    assert query_calls == 1
    assert "Loaded cached ASF query" in cached_result.stdout


def test_predominant_flight_direction_tie_selects_ascending():
    manifest = pd.DataFrame(
        {"flightDirection": ["DESCENDING", "ASCENDING"]}
    )

    assert get_predominant_flightdir(manifest) == "ASCENDING"


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
    assert [
        (call["date_start"], call["date_end"]) for call in calls
    ] == [
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
        return ["download complete"]

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
        ],
    )

    assert result.exit_code == 0, result.stderr
    assert calls["products"]["url"].tolist() == [
        "https://example.test/one.zip"
    ]
    assert calls["config_file"] == config
    assert calls["processes"] == 2
    assert calls["retries"] == 5
    assert "download complete" in result.stdout


def test_asf_download_cli_uses_latest_cached_query(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    cache_dir = Path(".asf-cache")
    older = cache_dir / "older" / "manifest.parquet"
    latest = cache_dir / "latest" / "manifest.parquet"
    older.parent.mkdir(parents=True)
    latest.parent.mkdir(parents=True)
    pd.DataFrame(
        {"url": ["https://example.test/older.zip"]}
    ).to_parquet(older, index=False)
    pd.DataFrame(
        {"url": ["https://example.test/latest.zip"]}
    ).to_parquet(latest, index=False)
    os.utime(older, (1, 1))
    os.utime(latest, (2, 2))

    config = tmp_path / "earthdata.netrc"
    _write_earthdata_config(config)
    calls = {}

    def fake_download_asf(**kwargs):
        calls.update(kwargs)
        return ["download complete"]

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
    assert calls["products"]["url"].tolist() == [
        "https://example.test/latest.zip"
    ]
    assert f"Using most recent cached ASF query: {latest}" in result.stdout


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
    assert "downloaded 1 · skipped 0 · failed 0" in progress_output

    messages = download_asf(products, outdir, config_file=config, processes=1)

    assert download_calls == 1
    assert "0 downloaded, 1 skipped, 0 failed" in messages[-2]

    target.write_bytes(b"bad")
    messages = download_asf(products, outdir, config_file=config, processes=1)

    assert download_calls == 2
    assert target.read_bytes() == b"data"
    assert "1 downloaded, 0 skipped, 0 failed" in messages[-2]


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
    state = pd.read_parquet(
        outdir / ".sentinel-py" / "asf_downloads.parquet"
    )
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
