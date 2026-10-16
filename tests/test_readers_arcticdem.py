"""Tests for ArcticDEM tile records and the local/remote DEM reader."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from shapely.geometry import Point, box

from sentinel_py.cache import deterministic_cache_key
from sentinel_py.enums import SourceType
from sentinel_py.pipeline.config import PipelineConfigError
from sentinel_py.pipeline.grid import is_remote_path
from sentinel_py.pipeline.processors.base import get_default_registry
from sentinel_py.pipeline.readers import get_default_reader_registry
from sentinel_py.pipeline.readers.arcticdem import (
    DEMTile,
    local_dem_tiles,
    remote_dem_tiles,
    select_dem_tiles,
)


def _unit(geometry_wgs84) -> SimpleNamespace:
    """Return the AOI-unit attributes the tile selection reads."""
    return SimpleNamespace(
        aoi_id="site",
        component_id="point-0000",
        geometry=geometry_wgs84,
        geometry_wgs84=geometry_wgs84,
        grid=SimpleNamespace(crs="EPSG:4326"),
    )


def _manifest(*rows: tuple[str, str, str | None]) -> pd.DataFrame:
    """Build a PGC-style manifest from ``(url, footprint wkt, etag)`` rows."""
    return pd.DataFrame(
        [
            {"url": url, "tile_id": url, "geometry_wkt": footprint, "etag": etag}
            for url, footprint, etag in rows
        ]
    )


def test_local_tiles_are_sorted_and_fingerprinted_by_path(tmp_path: Path) -> None:
    for name in ("b.tif", "a.tif"):
        (tmp_path / name).touch()
    source = SimpleNamespace(data_dir=tmp_path, pattern="*.tif")

    tiles = local_dem_tiles(source)

    assert [tile.path.name for tile in tiles] == ["a.tif", "b.tif"]
    # The provenance key of local sources predates DEMTile and must not change.
    assert deterministic_cache_key([tile.fingerprint for tile in tiles]) == (
        deterministic_cache_key([str(tmp_path / "a.tif"), str(tmp_path / "b.tif")])
    )


def test_local_tiles_without_a_match_are_an_error(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="No DEM GeoTIFFs matched"):
        local_dem_tiles(SimpleNamespace(data_dir=tmp_path, pattern="*.tif"))


def test_tiles_with_a_footprint_are_selected_without_opening_files() -> None:
    near = DEMTile("https://example.invalid/near.tif", "near", box(0, 0, 1, 1))
    margin = DEMTile("https://example.invalid/margin.tif", "margin", box(1.02, 0, 2, 1))
    far = DEMTile("https://example.invalid/far.tif", "far", box(10, 10, 11, 11))

    selected = select_dem_tiles((near, margin, far), _unit(Point(0.99, 0.5)))

    # ``margin`` lies within the edge margin of the point; ``far`` does not.
    assert selected == (near, margin)


def test_remote_tiles_query_once_and_reuse_the_cache(tmp_path, monkeypatch) -> None:
    calls = []

    def fake_query(geometries, **kwargs):
        calls.append(geometries)
        return _manifest(
            ("https://example.invalid/b.tif", box(2, 2, 3, 3).wkt, "etag-b"),
            ("https://example.invalid/a.tif", box(0, 0, 1, 1).wkt, None),
        )

    monkeypatch.setattr("sentinel_py.download.pgc.query_arcticdem", fake_query)
    plan = SimpleNamespace(units=(_unit(Point(0.5, 0.5)), _unit(Point(2.5, 2.5))))

    first = remote_dem_tiles(plan, cache_dir=tmp_path)
    second = remote_dem_tiles(plan, cache_dir=tmp_path)

    assert first == second
    assert len(calls) == 1
    # One search covers every unit: the buffered bounds of all of them.
    west, south, east, north = calls[0][0].bounds
    assert west < 0.5 and south < 0.5 and east > 2.5 and north > 2.5
    assert [tile.path for tile in first] == [
        "https://example.invalid/a.tif",
        "https://example.invalid/b.tif",
    ]
    assert [tile.fingerprint for tile in first] == [
        "https://example.invalid/a.tif#",
        "https://example.invalid/b.tif#etag-b",
    ]


def test_remote_tiles_without_a_match_are_an_error(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        "sentinel_py.download.pgc.query_arcticdem",
        lambda geometries, **kwargs: _manifest(),
    )
    with pytest.raises(RuntimeError, match="no ArcticDEM tiles"):
        remote_dem_tiles(SimpleNamespace(units=(_unit(Point(0, 0)),)), cache_dir=tmp_path)


def test_remote_source_config_takes_no_directory(tmp_path: Path) -> None:
    reader = get_default_reader_registry().get(SourceType.dem_remote)

    source = reader.parse_config("dem", {"type": "dem.remote"}, tmp_path)

    assert source.data_dir is None
    assert source.source_type == SourceType.dem_remote
    with pytest.raises(PipelineConfigError, match="data_dir"):
        reader.parse_config("dem", {"type": "dem.remote", "data_dir": "x"}, tmp_path)


def test_local_source_config_is_unchanged(tmp_path: Path) -> None:
    reader = get_default_reader_registry().get(SourceType.dem_local)

    source = reader.parse_config("dem", {"type": "dem.local", "data_dir": "."}, tmp_path)

    assert source.data_dir == tmp_path.resolve()
    assert source.source_type == SourceType.dem_local
    with pytest.raises(PipelineConfigError):
        reader.parse_config("dem", {"type": "dem.local"}, tmp_path)


def test_dem_processor_accepts_local_and_remote_sources_only() -> None:
    processor = get_default_registry().get("dem")
    config = processor.config_model(source="dem", features=["slope"])

    def check(source_type):
        processor.validate(
            config,
            node_id="n",
            inputs={},
            sources={"dem": SimpleNamespace(source_type=source_type)},
            outputs=(),
            output_grid=SimpleNamespace(chunks=object()),
        )

    check(SourceType.dem_local)
    check(SourceType.dem_remote)
    with pytest.raises(ValueError, match="dem.local or dem.remote"):
        check(SourceType.s2_l2a_local)


def test_snap_gpt_requires_local_dem_tiles() -> None:
    processor = get_default_registry().get("snap_gpt")
    config = processor.config_model(source="s1", dem="dem")
    sources = {
        "s1": SimpleNamespace(source_type=SourceType.s1_grd_local),
        "dem": SimpleNamespace(source_type=SourceType.dem_remote),
    }

    with pytest.raises(ValueError, match="must be dem.local"):
        processor.validate(
            config,
            node_id="n",
            inputs={},
            sources=sources,
            outputs=(),
            output_grid=SimpleNamespace(
                crs="EPSG:3338", resolution=(20, 20), chunks=object()
            ),
        )


def test_remote_paths_are_recognised() -> None:
    assert is_remote_path("https://bucket.s3.us-west-2.amazonaws.com/tile.tif")
    assert is_remote_path("/vsis3/bucket/tile.tif")
    assert not is_remote_path(Path("/data/tile.tif"))


def test_remote_reads_skip_the_aws_credential_lookup() -> None:
    """Without this option every open waits ~2 s on the EC2 metadata probe."""
    from sentinel_py.pipeline.grid import REMOTE_GDAL_OPTIONS

    assert REMOTE_GDAL_OPTIONS["AWS_NO_SIGN_REQUEST"] == "YES"
