from pathlib import Path
import json

from typer.testing import CliRunner

from sentinel_py.cli.main import app  # or from sentinel_py.cli import app

runner = CliRunner()


def test_create_aoi_cli_success(tmp_path: Path):
    out = tmp_path / "aoi.geojson"

    result = runner.invoke(
        app,
        [
            "create-aoi",
            "--xmin", "-150",
            "--xmax", "-148",
            "--ymin", "68",
            "--ymax", "70",
            "--out-file", str(out),
        ],
    )

    assert result.exit_code == 0, result.stdout
    assert out.exists()


def test_create_aoi_cli_invalid_bounds(tmp_path: Path):
    out = tmp_path / "aoi.geojson"

    result = runner.invoke(
        app,
        [
            "create-aoi",
            "--xmin", "10",
            "--xmax", "5",   # invalid
            "--ymin", "0",
            "--ymax", "1",
            "--out-file", str(out),
        ],
    )

    assert result.exit_code == 1
    assert "xmin must be less than xmax" in result.stderr
    assert not out.exists()
