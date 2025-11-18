from pathlib import Path

import geopandas as gpd
import pytest
from typer.testing import CliRunner

from sentinel_py.cli.main import app

runner = CliRunner()


def test_creates_geojson(tmp_path: Path):
    out = tmp_path / "aoi.geojson"

    result = runner.invoke(
        app,
        [
            "aoi",
            "--xmin", "-150",
            "--xmax", "-148",
            "--ymin", "68",
            "--ymax", "70",
            "--out-file", str(out),
        ],
    )

    assert result.exit_code == 0, result.stdout
    assert out.exists()

    # optional: sanity-check bounds
    gdf = gpd.read_file(out)
    assert len(gdf) == 1
    minx, miny, maxx, maxy = gdf.total_bounds
    assert pytest.approx(minx) == -150
    assert pytest.approx(maxx) == -148
    assert pytest.approx(miny) == 68
    assert pytest.approx(maxy) == 70


def test_invalid_bounds(tmp_path: Path):
    out = tmp_path / "aoi.geojson"

    result = runner.invoke(
        app,
        [
            "aoi",
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