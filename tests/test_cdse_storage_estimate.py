from pathlib import Path

import pandas as pd

from sentinel_py.download.cdse import (
    StorageEstimator,
    _format_bytes,
    _scene_storage_bytes,
    _storage_progress_text,
    cdse_scene_catalog_path,
    prepare_cdse_download,
    update_cdse_scene_catalog,
)


def test_storage_estimator_projects_unresolved_scenes():
    estimator = StorageEstimator(total_scenes=4)

    projection = estimator.add_scene(footprint=100, additional=80)
    assert projection.projected_footprint == 400
    assert projection.projected_additional == 320

    projection = estimator.add_scene(footprint=300, additional=120)
    assert projection.resolved_scenes == 2
    assert projection.projected_footprint == 800
    assert projection.projected_additional == 400


def test_scene_storage_bytes_checks_disk_despite_valid_cached_size(tmp_path: Path):
    scene_name = "scene"
    valid_path = tmp_path / scene_name / "valid.jp2"
    valid_path.parent.mkdir()
    valid_path.write_bytes(b"x" * 25)

    images = [
        {
            "img_path_in_safedir": "missing.jp2",
            "s3_expected_size": 100,
            "local_actual_size": None,
        },
        {
            "img_path_in_safedir": "valid.jp2",
            "s3_expected_size": 25,
            "local_actual_size": None,
        },
        {
            "img_path_in_safedir": "trusted-cache.jp2",
            "s3_expected_size": 50,
            "local_actual_size": 50,
        },
    ]

    footprint, additional = _scene_storage_bytes(scene_name, images, tmp_path)

    assert footprint == 175
    assert additional == 150


def test_storage_display_marks_small_samples_as_early():
    projection = StorageEstimator(total_scenes=20).add_scene(1024**3, 512 * 1024**2)

    text = _storage_progress_text(projection, free_bytes=10 * 1024**3)

    assert "early sample 1/10" in text
    assert "dataset ~20.0 GiB total" in text
    assert "~10.0 GiB additional" in text
    assert "10.0 GiB free at start" in text


def test_storage_display_warns_when_estimate_exceeds_free_space():
    projection = StorageEstimator(total_scenes=2).add_scene(1024**3, 1024**3)

    text = _storage_progress_text(projection, free_bytes=512 * 1024**2)

    assert text.startswith("⚠ ")


def test_format_bytes_uses_iec_units():
    assert _format_bytes(0) == "0 B"
    assert _format_bytes(1536) == "1.5 KiB"


def test_cdse_scene_catalog_merges_queries_by_scene_name(tmp_path: Path):
    first = pd.DataFrame(
        {
            "Id": ["one", "two"],
            "Name": ["S2A_ONE.SAFE", "S2A_TWO.SAFE"],
            "S3Path": ["/one", "/two"],
            "ContentDate": ["2024-06-01", "2024-06-02"],
            "GeoFootprint": ["POLYGON ONE", "POLYGON TWO"],
            "query_id": ["first", "first"],
        }
    )
    second = pd.DataFrame(
        {
            "Id": ["two", "three"],
            "Name": ["S2A_TWO.SAFE", "S2A_THREE.SAFE"],
            "S3Path": ["/two", "/three"],
            "ContentDate": ["2024-06-02", "2024-06-03"],
            "GeoFootprint": ["POLYGON TWO", "POLYGON THREE"],
            "query_id": ["second", "second"],
        }
    )

    update_cdse_scene_catalog(tmp_path, first)
    path = update_cdse_scene_catalog(tmp_path, second)
    catalog = pd.read_parquet(path).sort_values("Id").reset_index(drop=True)

    assert path == cdse_scene_catalog_path(tmp_path)
    assert catalog["Id"].tolist() == ["one", "three", "two"]
    assert catalog.loc[catalog["Id"] == "two", "query_id"].item() == "second"


def test_prepare_cdse_download_resolves_sizes_without_downloading(
    tmp_path: Path,
    monkeypatch,
):
    scenes_cache = tmp_path / "cache" / "query" / "scenes.parquet"
    scenes_cache.parent.mkdir(parents=True)
    scene_name = "S2A_TEST_MSIL2A.SAFE"
    pd.DataFrame({"Name": [scene_name], "S3Path": ["/eodata/test"]}).to_parquet(
        scenes_cache, index=False
    )
    output_dir = tmp_path / "downloads"
    existing = output_dir / scene_name / "B04.jp2"
    existing.parent.mkdir(parents=True)
    existing.write_bytes(b"x" * 100)

    def fake_find_images(*args, **kwargs):
        return [
            {
                "safedir": scene_name,
                "s3_path": "/test",
                "band_name": "B04",
                "resolution_m": 20,
                "img_path_in_safedir": "B04.jp2",
                "s3_expected_size": 100,
                "local_actual_size": None,
                "asset_type": "image",
            },
            {
                "safedir": scene_name,
                "s3_path": "/test",
                "band_name": "MTD_MSIL2A",
                "resolution_m": 0,
                "img_path_in_safedir": "MTD_MSIL2A.xml",
                "s3_expected_size": 10,
                "local_actual_size": None,
                "asset_type": "metadata",
            },
            {
                "safedir": scene_name,
                "s3_path": "/test",
                "band_name": "MTD_TL",
                "resolution_m": 0,
                "img_path_in_safedir": "GRANULE/MTD_TL.xml",
                "s3_expected_size": 5,
                "local_actual_size": None,
                "asset_type": "metadata",
            },
        ]

    monkeypatch.setattr(
        "sentinel_py.download.cdse._find_s2_scene_images",
        fake_find_images,
    )

    summary = prepare_cdse_download(
        scenes_cache=scenes_cache,
        mission="S2",
        bands=["B04"],
        resolution=20,
        output_dir=output_dir,
        config_file=str(tmp_path / ".s5cfg"),
    )

    assert summary.asset_count == 3
    assert summary.known_total_bytes == 115
    assert summary.known_additional_bytes == 15
    assert summary.unknown_size_assets == 0
    assert (tmp_path / "cache" / "assets.parquet").exists()
