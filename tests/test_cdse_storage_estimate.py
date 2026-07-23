from pathlib import Path

from sentinel_py.download.cdse import (
    StorageEstimator,
    _format_bytes,
    _scene_storage_bytes,
    _storage_progress_text,
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
