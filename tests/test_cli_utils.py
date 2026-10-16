"""AOI utility commands: ``bbox2geojson``, ``aoi2bbox``, ``csv2geojson``, ``grid``."""

from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd
from shapely.geometry import Point, box

from helpers import runner, write_polygon_aoi
from sentinel_py.cli.main import app


def _features(path: Path) -> list[dict]:
    return json.loads(path.read_text())["features"]


def test_bbox2geojson_writes_a_polygon_for_the_bounds(tmp_path: Path):
    output = tmp_path / "bbox.geojson"

    result = runner.invoke(
        app, ["bbox2geojson", "--bounds", "0", "1", "2", "3", "--output", str(output)]
    )

    assert result.exit_code == 0, result.output
    (feature,) = _features(output)
    coordinates = feature["geometry"]["coordinates"][0]
    xs = {point[0] for point in coordinates}
    ys = {point[1] for point in coordinates}
    assert xs == {0.0, 2.0} and ys == {1.0, 3.0}


def test_aoi2bbox_encloses_all_features(tmp_path: Path):
    aoi = tmp_path / "sheds.geojson"
    gpd.GeoDataFrame(
        {"name": ["a", "b"]},
        geometry=[box(0, 0, 1, 1), box(2, -1, 3, 4)],
        crs="EPSG:4326",
    ).to_file(aoi, driver="GeoJSON")
    output = tmp_path / "bbox.geojson"

    result = runner.invoke(
        app, ["aoi2bbox", "--aoi", str(aoi), "--output", str(output)]
    )

    assert result.exit_code == 0, result.output
    (feature,) = _features(output)
    coordinates = feature["geometry"]["coordinates"][0]
    assert {p[0] for p in coordinates} == {0.0, 3.0}
    assert {p[1] for p in coordinates} == {-1.0, 4.0}


def test_aoi2bbox_rejects_degenerate_extent(tmp_path: Path):
    aoi = tmp_path / "point.geojson"
    gpd.GeoDataFrame(geometry=[Point(1, 1)], crs="EPSG:4326").to_file(
        aoi, driver="GeoJSON"
    )
    output = tmp_path / "bbox.geojson"

    result = runner.invoke(
        app, ["aoi2bbox", "--aoi", str(aoi), "--output", str(output)]
    )

    assert result.exit_code == 2
    assert not output.exists()


def test_csv2geojson_writes_points_from_lon_lat_columns(tmp_path: Path):
    csv = tmp_path / "sites.csv"
    csv.write_text("name,lon,lat\na,10.0,60.0\nb,11.5,61.5\n")
    output = tmp_path / "points.geojson"

    result = runner.invoke(
        app,
        [
            "csv2geojson",
            "--csv", str(csv),
            "--lon", "lon",
            "--lat", "lat",
            "--output", str(output),
        ],
    )

    assert result.exit_code == 0, result.output
    features = _features(output)
    assert [f["geometry"]["coordinates"] for f in features] == [
        [10.0, 60.0],
        [11.5, 61.5],
    ]


def test_grid_covers_the_aoi_with_cells_of_the_requested_size(tmp_path: Path):
    aoi = tmp_path / "aoi.geojson"
    write_polygon_aoi(aoi)
    output = tmp_path / "grid.geojson"

    result = runner.invoke(
        app,
        ["grid", "--aoi", str(aoi), "--px", "0.5", "0.5", "--output", str(output)],
    )

    assert result.exit_code == 0, result.output
    assert len(_features(output)) == 4


def test_grid_rejects_cell_sizes_outside_the_supported_range(tmp_path: Path):
    aoi = tmp_path / "aoi.geojson"
    write_polygon_aoi(aoi)

    result = runner.invoke(
        app,
        [
            "grid",
            "--aoi", str(aoi),
            "--px", "0.000001", "0.000001",
            "--output", str(tmp_path / "grid.geojson"),
        ],
    )

    assert result.exit_code != 0
    assert "Cell size values must be between" in result.output
