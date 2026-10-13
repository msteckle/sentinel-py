"""CDSE ``query`` and ``download`` CLI commands."""

from pathlib import Path

import pandas as pd
import pytest
import typer

from helpers import runner, write_polygon_aoi, write_s5_config
from sentinel_py.cache import DEFAULT_CDSE_CACHE_DIR
from sentinel_py.cli.cdse.download import _validate_s5_config
from sentinel_py.cli.main import app
from sentinel_py.download import cdse
from sentinel_py.download.preflight import DownloadStorageSummary


def test_cdse_query_uses_hidden_default_cache(tmp_path: Path, monkeypatch):
    aoi = tmp_path / "aoi.geojson"
    write_polygon_aoi(aoi)
    calls = {}

    def fake_query_cdse(**kwargs):
        calls.update(kwargs)

    monkeypatch.setattr(
        "sentinel_py.download.cdse.query_cdse",
        fake_query_cdse,
    )

    result = runner.invoke(
        app,
        [
            "cdse",
            "query",
            "--aoi",
            str(aoi),
            "--crs",
            "EPSG:4326",
            "--years",
            "2024",
        ],
    )

    assert result.exit_code == 0, result.stderr
    assert calls["cache_dir"] == DEFAULT_CDSE_CACHE_DIR


def test_cdse_query_parses_supplied_parameters_before_querying(
    tmp_path: Path,
    monkeypatch,
):
    aoi = tmp_path / "aoi.geojson"
    write_polygon_aoi(aoi)
    calls = {}

    def fake_query_cdse(**kwargs):
        calls.update(kwargs)

    monkeypatch.setattr(
        "sentinel_py.download.cdse.query_cdse",
        fake_query_cdse,
    )

    result = runner.invoke(
        app,
        [
            "cdse",
            "query",
            "--aoi",
            str(aoi),
            "--crs",
            "EPSG:3413",
            "--years",
            "2024, 2023 2024",
            "--speriod",
            "06-01",
            "--eperiod",
            "08-31",
            "--product",
            "s2msi1c",
            "--orbit",
            "descending",
            "--cloud-thresh",
            "12.5",
            "--rel-orbit-num",
            "143",
            "--ops-mode",
            "ins-nobs",
            "--platform-serial-id",
            "c",
            "--top",
            "250",
            "--no-count",
            "--cache-dir",
            str(tmp_path / ".cdse-cache"),
        ],
    )

    assert result.exit_code == 0, result.stderr
    assert calls["collection"] == "SENTINEL-2"
    assert calls["product"] == "S2MSI1C"
    assert calls["years"] == [2023, 2024]
    assert calls["speriod"].isoformat() == "2000-06-01"
    assert calls["eperiod"].isoformat() == "2000-08-31"
    assert calls["aoi"] == aoi
    assert calls["crs"] == "EPSG:3413"
    assert calls["orbit"] == "DESCENDING"
    assert calls["cloud_thresh"] == 12.5
    assert calls["rel_orbit_num"] == 143
    assert calls["ops_mode"] == "INS-NOBS"
    assert calls["platform_serial_id"] == "C"
    assert calls["top"] == 250
    assert calls["count"] is False
    for key in ("collection", "product", "orbit", "ops_mode", "platform_serial_id"):
        assert type(calls[key]) is str


def test_cdse_query_help_lists_sourced_sentinel2_options():
    result = runner.invoke(app, ["cdse", "query", "--help"])
    normalized_help = " ".join(result.stdout.replace("│", " ").split())

    assert result.exit_code == 0
    assert "S2MSI1C" in normalized_help
    assert "S2MSI2A" in normalized_help
    assert "surface" in normalized_help
    assert "SCL; recommended" in normalized_help
    assert "INS-NOBS" in normalized_help
    assert "INS-RAW" in normalized_help
    assert "INS-VIC" in normalized_help
    assert "1-143" in normalized_help
    assert "or C." in normalized_help
    assert "ESA Sentinel-2" in normalized_help
    assert "Source: CDSE" in normalized_help
    assert "SENTINEL-1" not in normalized_help
    assert "--burst-mode" not in normalized_help


def test_cdse_query_rejects_sentinel2_relative_orbit_above_143(
    tmp_path: Path,
):
    aoi = tmp_path / "aoi.geojson"
    write_polygon_aoi(aoi)

    result = runner.invoke(
        app,
        [
            "cdse",
            "query",
            "--aoi",
            str(aoi),
            "--crs",
            "EPSG:4326",
            "--years",
            "2024",
            "--rel-orbit-num",
            "144",
        ],
    )

    assert result.exit_code != 0
    assert "143" in result.stderr


def test_cdse_download_discovers_query_in_hidden_default_cache(
    tmp_path: Path,
    monkeypatch,
):
    config = tmp_path / ".s5cfg"
    write_s5_config(config)
    query_cache = tmp_path / "scenes.parquet"
    pd.DataFrame(
        {
            "Name": ["S2A_ONE.SAFE", "S2A_TWO.SAFE"],
            "S3Path": ["/eodata/one", "/eodata/two"],
        }
    ).to_parquet(query_cache, index=False)
    calls = {}

    def fake_find_latest(cache_dir: Path):
        calls["searched_cache_dir"] = cache_dir
        return query_cache

    def fake_resolve_and_download(**kwargs):
        calls.update(kwargs)

    def fake_prepare_cdse_download(**kwargs):
        return DownloadStorageSummary(2, 300, 300)

    monkeypatch.setattr(
        "sentinel_py.download.cdse.find_latest_scenes_cache",
        fake_find_latest,
    )
    monkeypatch.setattr(
        "sentinel_py.download.cdse.resolve_and_download",
        fake_resolve_and_download,
    )
    monkeypatch.setattr(
        "sentinel_py.download.cdse.prepare_cdse_download",
        fake_prepare_cdse_download,
    )

    result = runner.invoke(
        app,
        [
            "cdse",
            "download",
            "--mission",
            "S2",
            "--bands",
            "B04",
            "--outdir",
            str(tmp_path),
            "--res",
            "20",
            "--config",
            str(config),
            "--yes",
        ],
    )

    assert result.exit_code == 0, result.stderr
    assert calls["searched_cache_dir"] == DEFAULT_CDSE_CACHE_DIR
    assert calls["scenes_cache"] == query_cache
    assert f"Cached query: {query_cache}" in result.stdout
    assert "Found 2 scenes: 1 requested image per scene" in result.stdout
    assert "Summary:" in result.stdout
    assert "Download started:" in result.stdout
    assert "Download ended:" in result.stdout
    assert "Elapsed time:" in result.stdout
    assert "Results:          0 downloaded, 0 skipped, 0 failed" in result.stdout
    assert (
        f"Download status:  {tmp_path / '.sentinel-py' / 'cdse_downloads.parquet'}"
        in result.stdout
    )


def test_cdse_download_parses_supplied_parameters_before_downloading(
    tmp_path: Path,
    monkeypatch,
):
    config = tmp_path / ".s5cfg"
    write_s5_config(config)
    query_cache = tmp_path / "scenes.parquet"
    pd.DataFrame(
        {
            "Name": ["S2A_ONE.SAFE"],
            "S3Path": ["/eodata/one"],
        }
    ).to_parquet(query_cache, index=False)
    calls = {}

    def fake_resolve_and_download(**kwargs):
        calls.update(kwargs)
        return []

    def fake_prepare_cdse_download(**kwargs):
        return DownloadStorageSummary(4, 400, 400)

    monkeypatch.setattr(
        "sentinel_py.download.cdse.resolve_and_download",
        fake_resolve_and_download,
    )
    monkeypatch.setattr(
        "sentinel_py.download.cdse.prepare_cdse_download",
        fake_prepare_cdse_download,
    )

    result = runner.invoke(
        app,
        [
            "cdse",
            "download",
            "--mission",
            "s2",
            "--bands",
            "b02,B8A scl AOT",
            "--outdir",
            str(tmp_path / "downloads"),
            "--res",
            "20",
            "--config",
            str(config),
            "--query",
            str(query_cache),
            "--parallel-scenes",
            "3",
            "--parallel-bands",
            "2",
            "--yes",
        ],
    )

    assert result.exit_code == 0, result.stderr
    assert calls["scenes_cache"] == query_cache
    assert calls["mission"] == "S2"
    assert calls["bands"] == ["B02", "B8A", "SCL", "AOT"]
    assert calls["resolution"] == 20
    assert calls["output_dir"] == tmp_path / "downloads"
    assert calls["config_file"] == str(config)
    assert calls["parallel_scenes"] == 3
    assert calls["parallel_bands"] == 2
    assert type(calls["mission"]) is str
    assert all(type(band) is str for band in calls["bands"])
    assert type(calls["resolution"]) is int


def test_cdse_download_can_skip_storage_estimate(tmp_path: Path, monkeypatch):
    config = tmp_path / ".s5cfg"
    write_s5_config(config)
    query_cache = tmp_path / "scenes.parquet"
    pd.DataFrame(
        {"Name": ["S2A_ONE.SAFE"], "S3Path": ["/eodata/one"]}
    ).to_parquet(query_cache, index=False)
    prepared = []
    downloaded = {}

    monkeypatch.setattr(
        "sentinel_py.download.cdse.prepare_cdse_download",
        lambda **kwargs: prepared.append(kwargs),
    )
    monkeypatch.setattr(
        "sentinel_py.download.cdse.resolve_and_download",
        lambda **kwargs: downloaded.update(kwargs),
    )

    result = runner.invoke(
        app,
        [
            "cdse", "download", "--mission", "S2", "--bands", "B04",
            "--outdir", str(tmp_path), "--res", "20", "--config", str(config),
            "--query", str(query_cache), "--skip-storage-estimate", "--yes",
        ],
    )

    assert result.exit_code == 0, result.stderr
    assert prepared == []
    assert downloaded["scenes_cache"] == query_cache
    assert "Skipping the storage estimate" in result.stdout


def test_cdse_download_missing_config_shows_setup_instructions(tmp_path: Path):
    config = tmp_path / ".s5cfg"

    result = runner.invoke(
        app,
        [
            "cdse",
            "download",
            "--mission",
            "S2",
            "--bands",
            "B04",
            "--outdir",
            str(tmp_path),
            "--res",
            "20",
            "--config",
            str(config),
            "--cache-dir",
            str(tmp_path),
        ],
    )

    assert result.exit_code != 0
    assert "Invalid CDSE S3 configuration" in result.stderr
    assert "was not" in result.stderr
    assert "found." in result.stderr
    assert "[default]" in result.stderr
    assert "aws_access_key_id" in result.stderr
    assert "aws_secret_access_key" in result.stderr
    assert "eodata-s3keysmanager.dataspace.copernicus.eu" in result.stderr
    assert "chmod 600" in result.stderr


def test_validate_s5_config_reports_missing_required_keys(tmp_path: Path):
    config = tmp_path / ".s5cfg"
    config.write_text("[default]\naws_access_key_id = test-access-key\n")

    with pytest.raises(typer.BadParameter) as error:
        _validate_s5_config(config)

    message = str(error.value)
    assert "aws_secret_access_key" in message
    assert "host_base" in message


def test_validate_s5_config_accepts_required_shape(tmp_path: Path):
    config = tmp_path / ".s5cfg"
    config.write_text(
        "[default]\n"
        "aws_access_key_id = test-access-key\n"
        "aws_secret_access_key = test-secret-key\n"
        "aws_region = default\n"
        "host_base = eodata.dataspace.copernicus.eu\n"
        "use_https = true\n"
    )

    _validate_s5_config(config)


def test_cdse_download_rejects_non_sentinel2_mission(tmp_path: Path):
    config = tmp_path / ".s5cfg"
    config.write_text(
        "[default]\n"
        "aws_access_key_id = test-access-key\n"
        "aws_secret_access_key = test-secret-key\n"
        "host_base = eodata.dataspace.copernicus.eu\n"
    )

    result = runner.invoke(
        app,
        [
            "cdse",
            "download",
            "--mission",
            "S1",
            "--bands",
            "VV",
            "--outdir",
            str(tmp_path),
            "--res",
            "10",
            "--config",
            str(config),
        ],
    )

    assert result.exit_code != 0
    assert "currently supports only Sentinel-2" in result.stderr


def test_download_cli_exits_nonzero_for_required_asset_failure(
    tmp_path: Path,
    monkeypatch,
):
    config = tmp_path / ".s5cfg"
    query = tmp_path / "scenes.parquet"
    write_s5_config(config)
    pd.DataFrame(
        {
            "Name": ["S2A_TEST.SAFE"],
            "S3Path": ["/eodata/test"],
        }
    ).to_parquet(query, index=False)

    monkeypatch.setattr(
        cdse,
        "resolve_and_download",
        lambda **kwargs: [
            cdse.DownloadResult(
                scene_name="S2A_TEST.SAFE",
                failed=["MISSING:MTD_MSIL2A"],
            )
        ],
    )
    monkeypatch.setattr(
        cdse,
        "prepare_cdse_download",
        lambda **kwargs: cdse.DownloadStorageSummary(3, 300, 300),
    )

    result = runner.invoke(
        app,
        [
            "cdse",
            "download",
            "--mission",
            "S2",
            "--bands",
            "B04",
            "--outdir",
            str(tmp_path),
            "--res",
            "20",
            "--config",
            str(config),
            "--query",
            str(query),
            "--yes",
        ],
    )

    assert result.exit_code == 1
    assert "failed required asset" in result.stderr
