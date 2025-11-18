# tests/test_cli.py
import json
from pathlib import Path

import geopandas as gpd
import pytest
from typer.testing import CliRunner

from sentinel_py.cli.main import app

runner = CliRunner()


@pytest.fixture
def dummy_download(monkeypatch):
    """Monkeypatch download_s2_seasonal_scenes to avoid real downloads."""

    called = {}

    def fake_download_s2_seasonal_scenes(**kwargs):
        # Just record what we were called with, or perform light checks
        called["kwargs"] = kwargs

    import sentinel_py.s2.workflows.download_s2 as dl

    monkeypatch.setattr(
        dl, "download_s2_seasonal_scenes", fake_download_s2_seasonal_scenes
    )

    return called


def test_s2_download_seasonally_calls_worker(tmp_path: Path, dummy_download):
    # Make a tiny AOI file for the CLI
    aoi_file = tmp_path / "aoi.geojson"
    aoi_file.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [],
            }
        )
    )

    output_dir = tmp_path / "out"

    result = runner.invoke(
        app,
        [
            "s2",
            "download-seasonally",
            "--aoi",
            str(aoi_file),
            "--output",
            str(output_dir),
            "--start-year",
            "2019",
            "--end-year",
            "2019",
        ],
    )

    assert result.exit_code == 0, result.stdout

    # Ensure our fake function was called
    assert "kwargs" in dummy_download
    kwargs = dummy_download["kwargs"]

    # Check that CLI arguments were forwarded properly
    assert kwargs["aoi"] == aoi_file
    assert kwargs["output_root"] == output_dir
    assert kwargs["start_year"] == 2019
    assert kwargs["end_year"] == 2019

    # Defaults
    assert kwargs["start_month"] == 6
    assert kwargs["end_month"] == 8
    assert kwargs["max_scenes"] is None
