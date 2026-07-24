import logging
from pathlib import Path

from sentinel_py.download import cdse


def test_cached_size_does_not_hide_deleted_local_file(
    tmp_path: Path,
    monkeypatch,
):
    scene_name = "S2A_TEST.SAFE"
    relative_path = "GRANULE/TEST/IMG_DATA/R20m/TEST_B04_20m.jp2"
    expected_size = 4
    downloaded: list[tuple[str, Path]] = []
    planned: list[int] = []
    results: list[bool] = []

    def fake_download(
        uri: str,
        local_path: Path,
        *,
        logger,
        config_file: str,
        expected_size: int | None,
    ) -> bool:
        downloaded.append((uri, local_path))
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_bytes(b"test")
        return True

    monkeypatch.setattr(cdse, "download_s3_file", fake_download)

    result = cdse._download_scene_from_images(
        scene_name=scene_name,
        s3_path="/Sentinel-2/test/S2A_TEST.SAFE",
        images=[
            {
                "band_name": "B04",
                "resolution_m": 20,
                "img_path_in_safedir": relative_path,
                "s3_expected_size": expected_size,
                # This describes the previous run; the file is now absent.
                "local_actual_size": expected_size,
            }
        ],
        output_dir=tmp_path,
        config_file="unused-test-config",
        parallel_bands=1,
        logger=logging.getLogger("test_cdse_download_cache"),
        on_download_plan=planned.append,
        on_download_result=results.append,
    )

    expected_path = tmp_path / scene_name / relative_path
    assert downloaded == [
        (
            "s3://eodata/Sentinel-2/test/S2A_TEST.SAFE/" + relative_path,
            expected_path,
        )
    ]
    assert result.succeeded == ["B04@20m"]
    assert result.skipped == []
    assert result.updated_images[0]["local_actual_size"] == expected_size
    assert expected_path.read_bytes() == b"test"
    assert planned == [1]
    assert results == [True]
