from pathlib import Path

import pandas as pd
import pytest

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


def _scene_images(scene_name: str, size: int) -> list[dict]:
    base = {"safedir": scene_name, "s3_path": "/t", "local_actual_size": None}
    return [
        {**base, "band_name": "B04", "resolution_m": 20,
         "img_path_in_safedir": "B04.jp2", "s3_expected_size": size,
         "asset_type": "image"},
        {**base, "band_name": "MTD_MSIL2A", "resolution_m": 0,
         "img_path_in_safedir": "MTD_MSIL2A.xml", "s3_expected_size": 10,
         "asset_type": "metadata"},
        {**base, "band_name": "MTD_TL", "resolution_m": 0,
         "img_path_in_safedir": "GRANULE/MTD_TL.xml", "s3_expected_size": 5,
         "asset_type": "metadata"},
    ]


def test_extrapolate_total_is_exact_for_full_sample_and_has_error_bar():
    from sentinel_py.download.preflight import extrapolate_total

    assert extrapolate_total([10.0, 10.0, 10.0], 3) == (30.0, 0.0)
    assert extrapolate_total([], 10) == (0.0, 0.0)

    total, half_width = extrapolate_total([8.0, 10.0, 12.0, 10.0], 100)
    assert total == pytest.approx(1000.0)
    assert 0 < half_width < 200

    constant_total, constant_ci = extrapolate_total([5.0] * 20, 1000)
    assert constant_total == pytest.approx(5000.0)
    assert constant_ci == 0.0


def test_prepare_cdse_download_lists_only_a_deterministic_sample(
    tmp_path: Path, monkeypatch
):
    scenes_cache = tmp_path / "cache" / "query" / "scenes.parquet"
    scenes_cache.parent.mkdir(parents=True)
    names = [f"S2A_SCENE{n:03d}_MSIL2A.SAFE" for n in range(50)]
    pd.DataFrame({"Name": names, "S3Path": [f"/eodata/{n}" for n in names]}).to_parquet(
        scenes_cache, index=False
    )
    listed: list[str] = []

    def fake_find(scene_name, *args, **kwargs):
        listed.append(scene_name)
        return _scene_images(scene_name, 100)

    monkeypatch.setattr("sentinel_py.download.cdse._find_s2_scene_images", fake_find)
    kwargs = dict(
        scenes_cache=scenes_cache, mission="S2", bands=["B04"], resolution=20,
        output_dir=tmp_path / "downloads", config_file=str(tmp_path / ".s5cfg"),
        sample_size=10,
    )

    summary = prepare_cdse_download(**kwargs)

    assert len(listed) == 10
    assert summary.is_estimate
    assert (summary.sampled_scenes, summary.total_scenes) == (10, 50)
    assert summary.asset_count == 150  # 3 assets per scene, extrapolated to 50
    assert summary.known_total_bytes == 50 * 115
    assert summary.total_ci95_bytes == 0  # identical scenes: no sampling variance

    # The same manifest chooses the same sample, which is now served from cache.
    first_sample = set(listed)
    listed.clear()
    again = prepare_cdse_download(**kwargs)
    assert listed == []
    assert again == summary
    assert len(first_sample) == 10


def test_estimate_always_prompts_but_exact_complete_download_does_not(monkeypatch):
    from sentinel_py.download.preflight import (
        DownloadStorageSummary,
        confirm_download,
    )

    prompts = []
    monkeypatch.setattr(
        "sentinel_py.download.preflight.typer.confirm",
        lambda *a, **k: prompts.append(1) or True,
    )

    confirm_download(
        assume_yes=False, storage=DownloadStorageSummary(10, 100, 0)
    )
    assert prompts == []

    confirm_download(
        assume_yes=False,
        storage=DownloadStorageSummary(10, 100, 0, is_estimate=True,
                                       sampled_scenes=5, total_scenes=50),
    )
    assert prompts == [1]
