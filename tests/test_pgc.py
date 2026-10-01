from __future__ import annotations

import sys
import types
from pathlib import Path

import pandas as pd
import pytest
from shapely.geometry import box, mapping

from sentinel_py.download import pgc


class FakeAsset:
    href = "https://example.test/10m/12_34.tif"
    media_type = "image/tiff; application=geotiff; profile=cloud-optimized"
    extra_fields = {"file:size": 4}


class FakeItem:
    id = "12_34_10m_v4.1"
    bbox = [12, 34, 13, 35]
    geometry = mapping(box(12, 34, 13, 35))
    properties = {"pgc:tile": "12_34"}
    assets = {"dem": FakeAsset()}


class FakeSearch:
    def items(self):
        return iter([FakeItem()])


class FakeCatalog:
    def __init__(self):
        self.calls = []

    def search(self, **kwargs):
        self.calls.append(kwargs)
        return FakeSearch()


def install_fake_pystac(monkeypatch):
    catalog = FakeCatalog()
    module = types.SimpleNamespace(Client=types.SimpleNamespace(open=lambda _: catalog))
    monkeypatch.setitem(sys.modules, "pystac_client", module)
    return catalog


def test_query_arcticdem_uses_tutorial_collection_and_bbox(monkeypatch):
    catalog = install_fake_pystac(monkeypatch)

    manifest = pgc.query_arcticdem((12, 34, 13, 35))

    assert manifest.loc[0, "tile_id"] == "12_34"
    assert manifest.loc[0, "expected_size"] == 4
    assert catalog.calls == [
        {
            "collections": [pgc.PGC_COLLECTION],
            "bbox": (12.0, 34.0, 13.0, 35.0),
            "max_items": None,
        }
    ]


def test_query_arcticdem_queries_disjoint_points_independently(monkeypatch):
    catalog = install_fake_pystac(monkeypatch)
    FakeItem.geometry = mapping(box(0, 0, 1, 1))

    manifest = pgc.query_arcticdem(
        [box(12.5, 34.5, 12.5, 34.5).centroid, box(20.5, 40.5, 20.5, 40.5).centroid]
    )

    assert len(catalog.calls) == 2
    assert {call["bbox"] for call in catalog.calls} == {
        (12.5, 34.5, 12.5, 34.5),
        (20.5, 40.5, 20.5, 40.5),
    }
    assert manifest.empty


def test_atomic_aoi_geometries_flattens_multipart_and_deduplicates():
    geometries = pgc.atomic_aoi_geometries(
        [box(0, 0, 1, 1), box(0, 0, 1, 1), box(5, 5, 6, 6).boundary]
    )

    assert len(geometries) == 2


def test_query_arcticdem_rejects_missing_dem_asset(monkeypatch):
    install_fake_pystac(monkeypatch)
    original_assets = FakeItem.assets
    FakeItem.assets = {}

    with pytest.raises(ValueError, match="no 'dem' asset"):
        pgc.query_arcticdem((12, 34, 13, 35))

    FakeItem.assets = original_assets


def test_select_cached_tiles_joins_download_state(tmp_path: Path):
    manifest = pd.DataFrame(
        [
            {
                "item_id": "a",
                "tile_id": "12_34",
                "url": "https://example.test/a.tif",
                "geometry_wkt": box(12, 34, 13, 35).wkt,
            },
            {
                "item_id": "b",
                "tile_id": "20_40",
                "url": "https://example.test/b.tif",
                "geometry_wkt": box(20, 40, 21, 41).wkt,
            },
        ]
    )
    state_file = tmp_path / "pgc_downloads.parquet"
    pd.DataFrame(
        [
            {
                "url": "https://example.test/a.tif",
                "path": "/tmp/a.tif",
                "status": "complete",
                "last_action": "downloaded",
                "error": None,
            }
        ]
    ).to_parquet(state_file)

    selected = pgc.select_cached_tiles(
        manifest,
        box(12.5, 34.5, 12.75, 34.75),
        state_file=state_file,
    )

    assert selected["tile_id"].tolist() == ["12_34"]
    assert selected.loc[0, "status"] == "complete"
    assert selected.loc[0, "download_status"] == "missing"


def test_download_arcticdem_skips_verified_existing_file(tmp_path: Path, monkeypatch):
    target = tmp_path / "tile.tif"
    target.write_bytes(b"data")
    products = pd.DataFrame(
        [
            {
                "item_id": "a",
                "tile_id": "12_34",
                "url": "https://example.test/tile.tif",
                "filename": "tile.tif",
                "expected_size": 4,
            }
        ]
    )

    def fail_get(*args, **kwargs):
        raise AssertionError("verified local tile should not be downloaded")

    monkeypatch.setattr(pgc.requests, "get", fail_get)
    summary = pgc.download_arcticdem(products, tmp_path, processes=1)

    assert summary.downloaded == 0
    assert summary.skipped == 1
    assert (tmp_path / ".sentinel-py" / "pgc_downloads.parquet").exists()
