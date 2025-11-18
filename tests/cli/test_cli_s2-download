from pathlib import Path
import json

import pytest
from typer.testing import CliRunner

from sentinel_py.cli.main import app

runner = CliRunner()


@pytest.fixture
def fake_download(monkeypatch):
    calls = {}

    def _fake_download_s2_seasonal_scenes(**kwargs):
        calls["kwargs"] = kwargs

    # Patch the name imported into main.py
    import sentinel_py.cli.main as cli_main
    monkeypatch.setattr(
        cli_main, "download_s2_seasonal_scenes", _fake_download_s2_seasonal_scenes
    )

    return calls


def test_download_calls_worker(tmp_path: Path, fake_download):
    aoi = tmp_path / "aoi.geojson"
    aoi.write_text(
        json.dumps({"type": "FeatureCollection", "features": []})
    )
    out = tmp_path / "out"

    result = runner.invoke(
        app,
        [
            "s2", "download",
            "--aoi", str(aoi),
            "--output", str(out),
            "--start-year", "2019",
            "--end-year", "2019",
        ],
    )

    assert result.exit_code == 0, result.stdout

    # Our fake function was called
    assert "kwargs" in fake_download
    kwargs = fake_download["kwargs"]

    assert kwargs["aoi"] == aoi
    assert kwargs["output_root"] == out
    assert kwargs["start_year"] == 2019
    assert kwargs["end_year"] == 2019
    # spot-check a default
    assert kwargs["start_month"] == 6
    assert kwargs["end_month"] == 8


def test_date_end_before_start(tmp_path: Path):
    aoi = tmp_path / "aoi.geojson"
    aoi.write_text(
        json.dumps({"type": "FeatureCollection", "features": []})
    )
    out = tmp_path / "out"

    result = runner.invoke(
        app,
        [
            "s2", "download",
            "--aoi", str(aoi),
            "--output", str(out),
            "--start-year", "2020",
            "--end-year", "2019",  # invalid (end < start)
        ],
    )

    assert result.exit_code == 1
    assert "end date must be on or after start date" in result.stderr
