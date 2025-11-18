from pathlib import Path

import geopandas as gpd
import pytest
from typer.testing import CliRunner

from sentinel_py.cli.main import app

runner = CliRunner()


def grid_from_aoi(tmp_path: Path):
    aoi_file = tmp_path / "aoi.geojson"
    grid_file = tmp_path / "grid.geojson"

    # First create an AOI via CLI
    result_aoi = runner.invoke(
        app,
        [
            "aoi",
            "--xmin", "-150",
            "--xmax", "-149.5",
            "--ymin", "68",
            "--ymax", "68.5",
            "--out-file", str(aoi_file),
        ],
    )
    assert result_aoi.exit_code == 0, result_aoi.stdout
    assert aoi_file.exists()

    # Now create a grid on it
    result_grid = runner.invoke(
        app,
        [
            "grid",
            "--aoi-file", str(aoi_file),
            "--dx-deg", "0.25",
            "--dy-deg", "0.25",
            "--out-file", str(grid_file),
        ],
    )
    assert result_grid.exit_code == 0, result_grid.stdout
    assert grid_file.exists()

    gdf_grid = gpd.read_file(grid_file)
    assert len(gdf_grid) >= 1


def test_negative_dx(tmp_path: Path):
    # Minimal fake AOI
    aoi_file = tmp_path / "aoi.geojson"
    aoi_file.write_text(
        """
        { "type": "FeatureCollection", "features": [] }
        """
    )
    grid_file = tmp_path / "grid.geojson"

    result = runner.invoke(
        app,
        [
            "grid",
            "--aoi-file", str(aoi_file),
            "--dx-deg", "-0.25",   # invalid
            "--dy-deg", "0.25",
            "--out-file", str(grid_file),
        ],
    )

    assert result.exit_code == 1
    assert "dx_deg and dy_deg must be positive" in result.stderr
    assert not grid_file.exists()
