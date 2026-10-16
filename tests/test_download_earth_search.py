"""Earth Search STAC query: item rows, search batching, and the query function."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
import shapely
from shapely.geometry import Point, box

from sentinel_py.download import earth_search


def _item(item_id: str = "S2A_21WVQ_20240615_0_L2A") -> SimpleNamespace:
    """A STAC item shaped like an Earth Search ``sentinel-2-l2a`` item."""
    red = SimpleNamespace(
        href="https://example.invalid/B04.tif",
        extra_fields={"raster:bands": [{"scale": 0.0001, "offset": -0.1, "nodata": 0}]},
    )
    scl = SimpleNamespace(href="https://example.invalid/SCL.tif", extra_fields={})
    jp2 = SimpleNamespace(href="s3://elsewhere/B04.jp2", extra_fields={})
    return SimpleNamespace(
        id=item_id,
        collection_id="sentinel-2-l2a",
        datetime=None,
        geometry={
            "type": "Polygon",
            "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
        },
        properties={
            "datetime": "2024-06-15T12:00:00Z",
            "s2:product_uri": "S2A_MSIL2A_20240615T120000_N0510.SAFE",
            "grid:code": "MGRS-21WVQ",
            "s2:processing_baseline": "05.10",
        },
        assets={"red": red, "scl": scl, "red-jp2": jp2},
    )


def test_item_row_keeps_cog_assets_by_band_name() -> None:
    row = earth_search._item_row(_item())

    assert row["item_id"] == "S2A_21WVQ_20240615_0_L2A"
    assert row["product_id"] == "S2A_MSIL2A_20240615T120000_N0510"
    assert row["tile_id"] == "T21WVQ"
    assert row["processing_baseline"] == "05.10"
    assert json.loads(row["assets"]) == {
        "B04": {
            "href": "https://example.invalid/B04.tif",
            "scale": 0.0001,
            "offset": -0.1,
            "nodata": 0,
        },
        "SCL": {
            "href": "https://example.invalid/SCL.tif",
            "scale": None,
            "offset": None,
            "nodata": None,
        },
    }


def test_search_batches_split_by_vertex_count_and_keep_every_geometry(monkeypatch):
    monkeypatch.setattr(earth_search, "MAX_SEARCH_VERTICES", 3)
    points = [Point(x, 0) for x in range(7)]

    batches = earth_search._search_batches(points)

    parts = [shapely.get_parts(batch) for batch in batches]
    assert [len(part) for part in parts] == [3, 3, 1]
    assert {(g.x, g.y) for part in parts for g in part} == {(p.x, p.y) for p in points}


def test_query_searches_each_year_season_and_deduplicates(monkeypatch) -> None:
    calls = []

    class Results:
        def items(self):
            return [_item("a"), _item("b")]

    class Catalog:
        def search(self, **kwargs):
            calls.append(kwargs)
            return Results()

    import pystac_client

    monkeypatch.setattr(pystac_client.Client, "open", staticmethod(lambda url: Catalog()))

    scenes = earth_search.query_earth_search(
        [box(0, 0, 1, 1)],
        collection="sentinel-2-l2a",
        years=(2023, 2024),
        speriod=(6, 1),
        eperiod=(8, 31),
    )

    assert sorted(call["datetime"] for call in calls) == [
        "2023-06-01/2023-08-31",
        "2024-06-01/2024-08-31",
    ]
    assert all(call["collections"] == ["sentinel-2-l2a"] for call in calls)
    # Each search returns the same two items; scenes are unique by item id.
    assert scenes["item_id"].tolist() == ["a", "b"]
    assert list(scenes.columns) == earth_search.EARTH_SEARCH_COLUMNS


def test_query_requires_a_geometry() -> None:
    with pytest.raises(ValueError, match="at least one non-empty geometry"):
        earth_search.query_earth_search(
            [], years=None, speriod=(1, 1), eperiod=(12, 31)
        )
