import logging
import shlex
from datetime import date
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from sentinel_py.cli.main import app
from sentinel_py.download import cdse


runner = CliRunner()


def _write_aoi(path: Path) -> None:
    path.write_text(
        """
        {
          "type": "FeatureCollection",
          "features": [{
            "type": "Feature",
            "properties": {},
            "geometry": {
              "type": "Polygon",
              "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]
            }
          }]
        }
        """
    )


def _write_s5_config(path: Path) -> None:
    path.write_text(
        "[default]\n"
        "aws_access_key_id = test-access-key\n"
        "aws_secret_access_key = test-secret-key\n"
        "host_base = eodata.dataspace.copernicus.eu\n"
    )


def test_query_cache_key_includes_result_affecting_filters():
    base = {
        "aoi_wkt": "POLYGON EMPTY",
        "collection_name": "SENTINEL-2",
        "product_type": "S2MSI2A",
        "iso_windows": [("2024-01-01", "2024-01-31")],
    }

    first = cdse.query_cache_key(**base, cloud_thresh=10)
    second = cdse.query_cache_key(**base, cloud_thresh=20)
    third = cdse.query_cache_key(**base, cloud_thresh=10, rel_orbit_num=12)

    assert first != second
    assert first != third


def test_successful_empty_query_is_cached(tmp_path: Path, monkeypatch):
    aoi = tmp_path / "aoi.geojson"
    cache_dir = tmp_path / "cache"
    _write_aoi(aoi)

    class EmptySearcher:
        def query_by_filter(self, **kwargs):
            return None

        def execute_query(self):
            return pd.DataFrame()

    monkeypatch.setattr(cdse, "CopernicusDataSearcher", EmptySearcher)

    result = cdse.query_cdse(
        collection="SENTINEL-2",
        product="S2MSI2A",
        years=[2024],
        speriod=date(2000, 6, 1),
        eperiod=date(2000, 6, 30),
        aoi=aoi,
        crs="EPSG:4326",
        cache_dir=cache_dir,
        count=True,
    )

    cache_path = Path(result.attrs["cache_path"])
    assert result.empty
    assert result.columns.tolist() == cdse.SCENE_CACHE_COLUMNS
    assert cache_path.exists()
    assert pd.read_parquet(cache_path).columns.tolist() == cdse.SCENE_CACHE_COLUMNS


def test_failed_query_batch_is_not_cached_as_complete(tmp_path: Path, monkeypatch):
    aoi = tmp_path / "aoi.geojson"
    cache_dir = tmp_path / "cache"
    _write_aoi(aoi)

    class FailingSearcher:
        def query_by_filter(self, **kwargs):
            return None

        def execute_query(self):
            raise RuntimeError("temporary catalogue failure")

    monkeypatch.setattr(cdse, "CopernicusDataSearcher", FailingSearcher)
    monkeypatch.setattr(cdse.time, "sleep", lambda _: None)

    with pytest.raises(RuntimeError, match="CDSE query incomplete"):
        cdse.query_cdse(
            collection="SENTINEL-2",
            product="S2MSI2A",
            years=[2024],
            speriod=date(2000, 6, 1),
            eperiod=date(2000, 6, 30),
            aoi=aoi,
            crs="EPSG:4326",
            cache_dir=cache_dir,
            count=True,
        )

    assert not list(cache_dir.glob("*/scenes.parquet"))


def test_s2_discovery_lists_each_resolution_once_and_includes_metadata(monkeypatch):
    scene_path = "/Sentinel-2/test/S2A_TEST.SAFE"
    calls: list[str] = []

    def fake_s5cmd(command: str, *, config_file: str, **kwargs):
        calls.append(command)
        if "R20m/*.jp2" in command:
            return "\n".join(
                [
                    "2024/01/01 00:00:00 4 "
                    f"s3://eodata{scene_path}/GRANULE/G1/IMG_DATA/R20m/"
                    "G1_B04_20m.jp2",
                    "2024/01/01 00:00:00 5 "
                    f"s3://eodata{scene_path}/GRANULE/G1/IMG_DATA/R20m/"
                    "G1_B05_20m.jp2",
                    "2024/01/01 00:00:00 6 "
                    f"s3://eodata{scene_path}/GRANULE/G2/IMG_DATA/R20m/"
                    "G2_B04_20m.jp2",
                ]
            )
        if "MTD_MSIL2A.xml" in command:
            return (
                "2024/01/01 00:00:00 7 "
                f"s3://eodata{scene_path}/MTD_MSIL2A.xml"
            )
        if "MTD_TL.xml" in command:
            return (
                "2024/01/01 00:00:00 8 "
                f"s3://eodata{scene_path}/GRANULE/G1/MTD_TL.xml"
            )
        return ""

    monkeypatch.setattr(cdse, "run_s5cmd_with_config", fake_s5cmd)

    assets = cdse._find_s2_scene_images(
        "S2A_MSIL2A_TEST.SAFE",
        scene_path,
        [
            cdse.ResolvedBand("B04", 20),
            cdse.ResolvedBand("B05", 20),
        ],
        config_file="unused",
        logger=logging.getLogger("test_s2_asset_discovery"),
    )

    assert sum("R20m/*.jp2" in call for call in calls) == 1
    assert len(calls) == 3
    assert [asset["band_name"] for asset in assets].count("B04") == 2
    assert {asset["band_name"] for asset in assets} == {
        "B04",
        "B05",
        "MTD_MSIL2A",
        "MTD_TL",
    }
    assert all(
        not asset["img_path_in_safedir"].startswith("s3://") for asset in assets
    )


def test_download_is_atomic_and_retries_size_mismatch(tmp_path: Path, monkeypatch):
    destination = tmp_path / "asset.jp2"
    destination.write_bytes(b"old")
    attempts: list[Path] = []

    def fake_s5cmd(command: str, **kwargs):
        temporary = Path(shlex.split(command)[-1])
        attempts.append(temporary)
        temporary.write_bytes(b"bad" if len(attempts) == 1 else b"good")

    monkeypatch.setattr(cdse, "run_s5cmd_with_config", fake_s5cmd)
    monkeypatch.setattr(cdse.time, "sleep", lambda _: None)

    ok = cdse.download_s3_file(
        "s3://eodata/example.jp2",
        destination,
        logger=logging.getLogger("test_atomic_download"),
        expected_size=4,
        attempts=2,
    )

    assert ok
    assert len(attempts) == 2
    assert destination.read_bytes() == b"good"
    assert not destination.with_name(".asset.jp2.part").exists()


def test_download_state_is_scoped_to_output_directory(tmp_path: Path, monkeypatch):
    cache_root = tmp_path / "cache"
    query_dir = cache_root / "query"
    query_dir.mkdir(parents=True)
    scenes_cache = query_dir / "scenes.parquet"
    scene_name = "S2A_MSIL2A_TEST.SAFE"
    pd.DataFrame(
        [{"Name": scene_name, "S3Path": "/Sentinel-2/test/" + scene_name}]
    ).to_parquet(scenes_cache, index=False)

    assets = [
        {
            "safedir": scene_name,
            "s3_path": f"/Sentinel-2/test/{scene_name}",
            "band_name": band,
            "resolution_m": resolution,
            "img_path_in_safedir": rel_path,
            "s3_expected_size": 4,
            "local_actual_size": None,
            "asset_type": asset_type,
        }
        for band, resolution, rel_path, asset_type in [
            (
                "B04",
                20,
                "GRANULE/G1/IMG_DATA/R20m/G1_B04_20m.jp2",
                "image",
            ),
            ("MTD_MSIL2A", 0, "MTD_MSIL2A.xml", "metadata"),
            ("MTD_TL", 0, "GRANULE/G1/MTD_TL.xml", "metadata"),
        ]
    ]

    monkeypatch.setattr(cdse, "_find_s2_scene_images", lambda *args, **kwargs: assets)

    def fake_download_scene(**kwargs):
        result = cdse.DownloadResult(scene_name=kwargs["scene_name"])
        for image in kwargs["images"]:
            result.succeeded.append(image["band_name"])
            result.updated_images.append(
                {
                    **image,
                    "local_actual_size": image["s3_expected_size"],
                    "local_path": str(
                        kwargs["output_dir"]
                        / kwargs["scene_name"]
                        / image["img_path_in_safedir"]
                    ),
                    "download_status": "complete",
                }
            )
        return result

    monkeypatch.setattr(cdse, "_download_scene_from_images", fake_download_scene)
    output_dir = tmp_path / "downloads"

    results = cdse.resolve_and_download(
        scenes_cache=scenes_cache,
        mission="S2",
        bands=["B04"],
        resolution=20,
        output_dir=output_dir,
        config_file="unused",
        parallel_scenes=1,
        parallel_bands=1,
    )

    state_file = output_dir / ".sentinel-py" / "cdse_downloads.parquet"
    assert all(result.ok for result in results)
    assert state_file.exists()
    state = pd.read_parquet(state_file)
    assert len(state) == 3
    assert set(state["download_status"]) == {"complete"}


def test_download_cli_exits_nonzero_for_required_asset_failure(
    tmp_path: Path,
    monkeypatch,
):
    config = tmp_path / ".s5cfg"
    query = tmp_path / "scenes.parquet"
    _write_s5_config(config)
    query.write_bytes(b"unused")

    monkeypatch.setattr(
        cdse,
        "resolve_and_download",
        lambda **kwargs: [
            cdse.DownloadResult(
                scene_name="S2A_TEST.SAFE",
                failed=["MISSING:MTD_MSIL2A"],
            )
        ],
    )

    result = runner.invoke(
        app,
        [
            "cdse",
            "download",
            "--mission",
            "S2",
            "--bands",
            "B04",
            "--outdir",
            str(tmp_path),
            "--res",
            "20",
            "--config",
            str(config),
            "--query",
            str(query),
        ],
    )

    assert result.exit_code == 1
    assert "failed required asset" in result.stderr
