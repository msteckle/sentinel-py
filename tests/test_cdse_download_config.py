from pathlib import Path

import pytest
import typer
from typer.testing import CliRunner

from sentinel_py.cli.cdse.download import _validate_s5_config
from sentinel_py.cli.main import app

runner = CliRunner()


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
