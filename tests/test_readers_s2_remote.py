"""Remote Sentinel-2 granules: records, finder, and local/remote read equivalence."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
from shapely.geometry import Point, box

from sentinel_py.enums import SourceType
from sentinel_py.pipeline import PipelineConfig
from sentinel_py.pipeline.config import PipelineConfigError
from sentinel_py.pipeline.readers import get_default_reader_registry
from sentinel_py.pipeline.readers import s2_remote
from sentinel_py.pipeline.run import run_pipeline
from test_s2_lazy import _synthetic_pipeline


def _assets(band_hrefs: dict[str, str]) -> str:
    """Asset JSON of a scene: every band with baseline-04+ radiometry, plus the SCL."""
    assets = {
        band: {"href": href, "scale": 0.0001, "offset": -0.1, "nodata": 0}
        for band, href in band_hrefs.items()
    }
    assets["SCL"] = {"href": band_hrefs["SCL"], "scale": None, "offset": None, "nodata": None}
    return json.dumps(assets)


def _scene(item_id: str, when: str, footprint, hrefs: dict[str, str]) -> dict:
    return {
        "item_id": item_id,
        "collection": "sentinel-2-l2a",
        "datetime": when,
        "product_id": f"PRODUCT_{item_id}",
        "tile_id": "T06ABC",
        "processing_baseline": "05.10",
        "geometry_wkt": footprint.wkt,
        "assets": _assets({**hrefs, "SCL": hrefs.get("SCL", "scl.tif")}),
    }


HREFS = {"B02": "https://example.invalid/B02.tif", "SCL": "https://example.invalid/SCL.tif"}


def test_granule_record_carries_cog_urls_radiometry_and_identity() -> None:
    scenes = pd.DataFrame([_scene("item-1", "2024-06-15T12:00:00Z", box(0, 0, 1, 1), HREFS)])

    granule = s2_remote.granule_from_row(
        next(scenes.itertuples()), ("B02",), "sentinel-2-l2a"
    )

    assert granule.band_paths == {"B02": HREFS["B02"]}
    assert granule.scl_path == HREFS["SCL"]
    assert granule.acquisition_time == "20240615T120000"
    assert granule.safe_dir is None and granule.product_metadata is None
    assert granule.radiometry.offsets == {"B02": -1000}
    assert granule.radiometry.special_values == (0, 65535)
    assert granule.radiometry.quantification_value == 10000
    # Identity changes with the collection, so the two archives never share caches.
    other = s2_remote.granule_from_row(next(scenes.itertuples()), ("B02",), "sentinel-2-c1-l2a")
    assert other.fingerprint != granule.fingerprint


def test_finder_queries_once_filters_by_season_and_footprint(tmp_path, monkeypatch) -> None:
    calls = []
    scenes = pd.DataFrame(
        [
            _scene("summer", "2024-07-01T10:00:00Z", box(0, 0, 1, 1), HREFS),
            _scene("winter", "2024-01-01T10:00:00Z", box(0, 0, 1, 1), HREFS),
            _scene("elsewhere", "2024-07-02T10:00:00Z", box(5, 5, 6, 6), HREFS),
        ]
    )

    def fake_query(geometries, **kwargs):
        calls.append(kwargs)
        return scenes

    monkeypatch.setattr("sentinel_py.download.earth_search.query_earth_search", fake_query)
    unit = SimpleNamespace(geometry_wgs84=Point(0.5, 0.5))
    plan = SimpleNamespace(units=(unit,))
    finder = s2_remote.RemoteGranuleFinder(
        plan, bands=("B02",), collection="sentinel-2-l2a", years=(2024,),
        speriod=(6, 1), eperiod=(8, 31), cache_dir=tmp_path,
    )

    assert [g.granule_id for g in finder(unit)] == ["summer"]
    assert [g.granule_id for g in finder(None)] == ["summer", "elsewhere"]
    # A second finder over the same query reuses the cached scene table.
    again = s2_remote.RemoteGranuleFinder(
        plan, bands=("B02",), collection="sentinel-2-l2a", years=(2024,),
        speriod=(6, 1), eperiod=(8, 31), cache_dir=tmp_path,
    )
    assert again.fingerprint == finder.fingerprint
    assert len(calls) == 1


def test_remote_source_config_accepts_only_known_collections(tmp_path: Path) -> None:
    reader = get_default_reader_registry().get(SourceType.s2_l2a_remote)

    default = reader.parse_config("s2", {"type": "s2.l2a.remote"}, tmp_path)
    c1 = reader.parse_config(
        "s2", {"type": "s2.l2a.remote", "collection": "sentinel-2-c1-l2a"}, tmp_path
    )

    assert default.data_dir is None and default.collection == "sentinel-2-l2a"
    assert c1.collection == "sentinel-2-c1-l2a"
    with pytest.raises(PipelineConfigError, match="collection must be one of"):
        reader.parse_config("s2", {"type": "s2.l2a.remote", "collection": "x"}, tmp_path)
    with pytest.raises(PipelineConfigError, match="data_dir"):
        reader.parse_config("s2", {"type": "s2.l2a.remote", "data_dir": "x"}, tmp_path)


def test_remote_and_local_sources_produce_identical_scenes(tmp_path, monkeypatch) -> None:
    """The remote path differs from the local one only in how granules are described."""
    pipeline_path, granule = _synthetic_pipeline(tmp_path)
    logger = logging.getLogger("s2-remote-test")
    local = run_pipeline(PipelineConfig.from_file(pipeline_path), logger)
    local_set = local.outputs["result"].datasets[("test-site", "part-0000")]

    image_dir = granule / "IMG_DATA"
    hrefs = {
        "B02": str(next((image_dir / "R10m").glob("*_B02_10m.jp2"))),
        "B03": str(next((image_dir / "R20m").glob("*_B03_20m.jp2"))),
        "SCL": str(next((image_dir / "R20m").glob("*_SCL_20m.jp2"))),
    }
    # The synthetic raster sits at EPSG:32606 (500000, 1000000); cover it in WGS84.
    from rasterio.warp import transform_bounds

    footprint = box(*transform_bounds("EPSG:32606", "EPSG:4326", 499000, 999000, 501000, 1001000))
    scenes = pd.DataFrame(
        [_scene("item-1", "2024-06-15T12:00:00Z", footprint, hrefs)]
    )
    monkeypatch.setattr(s2_remote, "earth_search_scenes", lambda plan, **kwargs: scenes)
    remote_path = tmp_path / "remote.yaml"
    remote_path.write_text(
        pipeline_path.read_text().replace(
            "s2: {type: s2.l2a.local, data_dir: raw}", "s2: {type: s2.l2a.remote}"
        )
    )

    remote = run_pipeline(PipelineConfig.from_file(remote_path), logger)
    remote_set = remote.outputs["result"].datasets[("test-site", "part-0000")]

    np.testing.assert_array_equal(
        remote_set.compute(scheduler="single-threaded").reflectance.values,
        local_set.compute(scheduler="single-threaded").reflectance.values,
    )
    assert remote_set.granule_id.values.tolist() == ["item-1"]
    assert remote_set.quantification_value.values.tolist() == [10000]
