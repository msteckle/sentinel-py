import logging
import signal
import shlex
import subprocess
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

    first = cdse.query_cache_key(**base, filters=cdse.CDSEQueryFilters(cloud_thresh=10))
    second = cdse.query_cache_key(
        **base, filters=cdse.CDSEQueryFilters(cloud_thresh=20)
    )
    third = cdse.query_cache_key(
        **base, filters=cdse.CDSEQueryFilters(cloud_thresh=10, rel_orbit_num=12)
    )

    assert first != second
    assert first != third


def test_query_cache_key_is_stable_across_releases():
    """Recorded keys keep existing on-disk query caches reachable."""
    base = {
        "aoi_wkt": "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))",
        "collection_name": "SENTINEL-2",
        "product_type": "S2MSI2A",
        "iso_windows": [("2023-06-01", "2023-08-31")],
    }

    assert cdse.query_cache_key(**base) == "81d66dec267acbb0eaf846ea63b86b1d"
    assert (
        cdse.query_cache_key(
            **base,
            filters=cdse.CDSEQueryFilters(
                orbit="DESCENDING",
                cloud_thresh=20.0,
                rel_orbit_num=12,
                ops_mode="INS-NOBS",
                platform_serial_id="A",
                attrs={"a": 1},
                top=500,
                count=True,
            ),
        )
        == "efdc2a78125c829795a595d89d8d1694"
    )


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


def test_s2_standard_metadata_filters_are_sent_as_odata_attributes(
    tmp_path: Path,
    monkeypatch,
):
    aoi = tmp_path / "aoi.geojson"
    _write_aoi(aoi)
    calls = []

    class CapturingSearcher:
        def query_by_filter(self, **kwargs):
            calls.append(kwargs)

        def execute_query(self):
            return pd.DataFrame()

    monkeypatch.setattr(cdse, "CopernicusDataSearcher", CapturingSearcher)

    cdse.query_cdse(
        collection="SENTINEL-2",
        product="S2MSI2A",
        years=[2024],
        speriod=date(2000, 6, 1),
        eperiod=date(2000, 6, 30),
        aoi=aoi,
        crs="EPSG:4326",
        cache_dir=tmp_path / "cache",
        attrs={"tileId": "06WVT"},
        rel_orbit_num=12,
        ops_mode="INS-NOBS",
        platform_serial_id="C",
        count=True,
    )

    assert len(calls) == 1
    assert calls[0]["attributes"] == {
        "tileId": "06WVT",
        "relativeOrbitNumber": 12,
        "operationalMode": "INS-NOBS",
        "platformSerialIdentifier": "C",
    }
    assert calls[0]["relative_orbit_number"] is None
    assert calls[0]["operational_mode"] is None
    assert calls[0]["platform_serial_identifier"] is None


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


def _recursive_listing(scene_path: str) -> str:
    return "\n".join(
        [
            f"2024/01/01 00:00:00 7 s3://eodata{scene_path}/MTD_MSIL2A.xml",
            f"2024/01/01 00:00:00 9 s3://eodata{scene_path}/manifest.safe",
            "2024/01/01 00:00:00 8 L2A_G1/MTD_TL.xml",
            "2024/01/01 00:00:00 1 L2A_G1/QI_DATA/MSK_CLDPRB_20m.jp2",
            "2024/01/01 00:00:00 4 L2A_G1/IMG_DATA/R10m/G1_B04_10m.jp2",
            "2024/01/01 00:00:00 4 L2A_G1/IMG_DATA/R20m/G1_B04_20m.jp2",
            "2024/01/01 00:00:00 5 L2A_G1/IMG_DATA/R20m/G1_B05_20m.jp2",
            "2024/01/01 00:00:00 6 L2A_G2/IMG_DATA/R20m/G2_B04_20m.jp2",
            "2024/01/01 00:00:00 2 L2A_G1/IMG_DATA/R20m/G1_SCL_20m.jp2",
        ]
    )


def test_s2_discovery_uses_one_recursive_listing_and_caches_every_asset(monkeypatch):
    scene_path = "/Sentinel-2/test/S2A_TEST.SAFE"
    calls: list[str] = []

    def fake_s5cmd(command: str, *, config_file: str, **kwargs):
        calls.append(command)
        return _recursive_listing(scene_path)

    monkeypatch.setattr(cdse, "run_s5cmd_with_config", fake_s5cmd)
    logger = logging.getLogger("test_s2_asset_discovery")

    listing = cdse._list_s2_product_assets(
        "S2A_MSIL2A_TEST.SAFE", scene_path, "unused", logger
    )

    assert len(calls) == 1
    assert calls[0].endswith(f"{scene_path}/*\"")
    # Every recognized band and resolution is kept so other requests need no S3 call.
    assert {(a["band_name"], a["resolution_m"]) for a in listing} == {
        ("MTD_MSIL2A", 0),
        ("MTD_TL", 0),
        ("B04", 10),
        ("B04", 20),
        ("B05", 20),
        ("SCL", 20),
    }
    assert all(not a["img_path_in_safedir"].startswith("s3://") for a in listing)
    assert {
        a["img_path_in_safedir"] for a in listing if a["band_name"] == "MTD_TL"
    } == {"GRANULE/L2A_G1/MTD_TL.xml"}
    assert {
        a["img_path_in_safedir"]
        for a in listing
        if (a["band_name"], a["resolution_m"]) == ("B04", 20)
    } == {
        "GRANULE/L2A_G1/IMG_DATA/R20m/G1_B04_20m.jp2",
        "GRANULE/L2A_G2/IMG_DATA/R20m/G2_B04_20m.jp2",
    }

    selected = cdse._select_s2_assets(
        listing,
        [cdse.ResolvedBand("B04", 20), cdse.ResolvedBand("B05", 20)],
        "S2A_MSIL2A_TEST.SAFE",
    )
    assert {(a["band_name"], a["resolution_m"]) for a in selected} == {
        ("MTD_MSIL2A", 0),
        ("MTD_TL", 0),
        ("B04", 20),
        ("B05", 20),
    }


def test_s2_selection_reports_requested_assets_missing_from_listing(caplog):
    listing = [
        {"band_name": "B04", "resolution_m": 20},
        {"band_name": "MTD_MSIL2A", "resolution_m": 0},
        {"band_name": "MTD_TL", "resolution_m": 0},
    ]
    logger = logging.getLogger("test_s2_selection")
    with caplog.at_level(logging.WARNING, logger="test_s2_selection"):
        selected = cdse._select_s2_assets(
            listing,
            [cdse.ResolvedBand("B04", 20), cdse.ResolvedBand("B09", 60)],
            "S2A_MSIL2A_TEST.SAFE",
            logger,
        )
    assert len(selected) == 3
    assert "B09@60m: not found" in caplog.text


def test_s2_discovery_falls_back_to_directory_listings_when_recursive_unusable(
    monkeypatch,
):
    scene_path = "/Sentinel-2/test/S2A_TEST.SAFE"
    calls: list[str] = []

    def fake_s5cmd(command: str, *, config_file: str, **kwargs):
        calls.append(command)
        if command.endswith("/*\""):
            # Unusable shape: paths lack the GRANULE structure the downloader needs.
            return "2024/01/01 00:00:00 4 G1_B04_20m.jp2"
        if "R20m/*.jp2" in command:
            return "2024/01/01 00:00:00 4 L2A_G1/IMG_DATA/R20m/G1_B04_20m.jp2"
        if "MTD_MSIL2A.xml" in command:
            return f"2024/01/01 00:00:00 7 s3://eodata{scene_path}/MTD_MSIL2A.xml"
        if "MTD_TL.xml" in command:
            return "2024/01/01 00:00:00 8 L2A_G1/MTD_TL.xml"
        return ""

    monkeypatch.setattr(cdse, "run_s5cmd_with_config", fake_s5cmd)

    listing = cdse._list_s2_product_assets(
        "S2A_MSIL2A_TEST.SAFE",
        scene_path,
        "unused",
        logging.getLogger("test_s2_fallback"),
    )

    assert len(calls) == 6  # 1 recursive + 3 resolution directories + 2 metadata
    assert {a["band_name"] for a in listing} == {"B04", "MTD_MSIL2A", "MTD_TL"}
    assert [a["img_path_in_safedir"] for a in listing if a["band_name"] == "B04"] == [
        "GRANULE/L2A_G1/IMG_DATA/R20m/G1_B04_20m.jp2"
    ]


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


def test_interrupted_s5cmd_is_not_retried(tmp_path: Path, monkeypatch):
    destination = tmp_path / "asset.jp2"
    destination.write_bytes(b"existing")
    calls = 0

    def interrupted_s5cmd(command: str, **kwargs):
        nonlocal calls
        calls += 1
        temporary = Path(shlex.split(command)[-1])
        temporary.write_bytes(b"partial")
        raise subprocess.CalledProcessError(-signal.SIGINT, ["s5cmd"])

    monkeypatch.setattr(cdse, "run_s5cmd_with_config", interrupted_s5cmd)

    with pytest.raises(KeyboardInterrupt):
        cdse.download_s3_file(
            "s3://eodata/example.jp2",
            destination,
            logger=logging.getLogger("test_interrupted_download"),
            expected_size=20,
            attempts=3,
        )

    assert calls == 1
    assert destination.read_bytes() == b"existing"
    assert not destination.with_name(".asset.jp2.part").exists()


def test_download_state_is_scoped_to_output_directory(
    tmp_path: Path, monkeypatch, capsys
):
    monkeypatch.setenv("COLUMNS", "200")
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

    monkeypatch.setattr(
        cdse, "_list_s2_product_assets", lambda *args, **kwargs: assets
    )

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
    progress_output = capsys.readouterr().out
    assert "downloaded 3 · skipped 0 · failed 0 · cache updated 3" in progress_output


def test_download_cli_exits_nonzero_for_required_asset_failure(
    tmp_path: Path,
    monkeypatch,
):
    config = tmp_path / ".s5cfg"
    query = tmp_path / "scenes.parquet"
    _write_s5_config(config)
    pd.DataFrame(
        {
            "Name": ["S2A_TEST.SAFE"],
            "S3Path": ["/eodata/test"],
        }
    ).to_parquet(query, index=False)

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
    monkeypatch.setattr(
        cdse,
        "prepare_cdse_download",
        lambda **kwargs: cdse.DownloadStorageSummary(3, 300, 300),
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
            "--yes",
        ],
    )

    assert result.exit_code == 1
    assert "failed required asset" in result.stderr


def _download_state_fixture(tmp_path: Path, names: list[str], monkeypatch):
    cache_root = tmp_path / "cache"
    query_dir = cache_root / "query"
    query_dir.mkdir(parents=True)
    scenes_cache = query_dir / "scenes.parquet"
    pd.DataFrame(
        [{"Name": name, "S3Path": f"/Sentinel-2/test/{name}"} for name in names]
    ).to_parquet(scenes_cache, index=False)

    def listing(scene_name, *args, **kwargs):
        base = {"safedir": scene_name, "s3_path": f"/Sentinel-2/test/{scene_name}",
                "local_actual_size": None}
        return [
            {**base, "band_name": "B04", "resolution_m": 20, "s3_expected_size": 4,
             "img_path_in_safedir": "GRANULE/G1/IMG_DATA/R20m/G1_B04_20m.jp2",
             "asset_type": "image"},
            {**base, "band_name": "MTD_MSIL2A", "resolution_m": 0,
             "s3_expected_size": 4, "img_path_in_safedir": "MTD_MSIL2A.xml",
             "asset_type": "metadata"},
            {**base, "band_name": "MTD_TL", "resolution_m": 0, "s3_expected_size": 4,
             "img_path_in_safedir": "GRANULE/G1/MTD_TL.xml", "asset_type": "metadata"},
        ]

    monkeypatch.setattr(cdse, "_list_s2_product_assets", listing)
    return scenes_cache


def _complete_scene_download(kwargs) -> "cdse.DownloadResult":
    result = cdse.DownloadResult(scene_name=kwargs["scene_name"])
    for image in kwargs["images"]:
        result.succeeded.append(image["band_name"])
        result.updated_images.append(
            {**image, "local_actual_size": image["s3_expected_size"],
             "download_status": "complete"}
        )
    return result


def test_interrupted_download_keeps_state_and_resumes(tmp_path: Path, monkeypatch):
    names = [f"S2A_MSIL2A_SCENE{n}.SAFE" for n in range(3)]
    scenes_cache = _download_state_fixture(tmp_path, names, monkeypatch)
    output_dir = tmp_path / "downloads"
    common = dict(
        scenes_cache=scenes_cache, mission="S2", bands=["B04"], resolution=20,
        output_dir=output_dir, config_file="unused", parallel_scenes=1,
        parallel_bands=1,
    )

    def interrupted(**kwargs):
        if kwargs["scene_name"] == names[2]:
            raise KeyboardInterrupt
        return _complete_scene_download(kwargs)

    monkeypatch.setattr(cdse, "_download_scene_from_images", interrupted)
    with pytest.raises(KeyboardInterrupt):
        cdse.resolve_and_download(**common)

    # Rows buffered since the last flush were written even though the run aborted.
    downloads = cdse.REGISTRY["cdse_downloads"].store(output_dir)
    state = downloads.read()
    assert set(state["safedir"]) == set(names[:2])
    assert set(state["download_status"]) == {"complete"}
    assert downloads.part_paths()  # not compacted: the run did not finish

    # Resuming completes the remaining scene and compacts into the state file.
    monkeypatch.setattr(
        cdse, "_download_scene_from_images",
        lambda **kwargs: _complete_scene_download(kwargs),
    )
    results = cdse.resolve_and_download(**common)

    assert all(result.ok for result in results)
    assert downloads.part_paths() == []
    final = pd.read_parquet(downloads.path)
    assert set(final["safedir"]) == set(names)
    assert len(final) == 9


def test_s2_discovery_and_fingerprint_see_uncompacted_download_state(
    tmp_path: Path, monkeypatch
):
    from shapely.geometry import box

    from sentinel_py.cache import REGISTRY
    from sentinel_py.pipeline.readers import s2 as s2_reader

    data_dir = tmp_path / "raw"
    recorded = "S2A_MSIL2A_20240601T000000_N0510_R001_T06WVB_20240601T010000.SAFE"
    unrecorded = "S2A_MSIL2A_20240602T000000_N0510_R001_T06WVB_20240602T010000.SAFE"
    for safe in (recorded, unrecorded):
        image_dir = data_dir / safe / "GRANULE" / "L2A_T06WVB_A000001" / "IMG_DATA" / "R20m"
        image_dir.mkdir(parents=True)
        (image_dir / "T06WVB_B04_20m.jp2").write_bytes(b"x")
        (image_dir / "T06WVB_SCL_20m.jp2").write_bytes(b"x")
    monkeypatch.setattr(
        s2_reader, "read_granule_footprint", lambda *args, **kwargs: box(0, 0, 1, 1)
    )

    store = REGISTRY["cdse_downloads"].store(data_dir)
    assert store.fingerprint() is None

    # Only checkpoint parts exist (a download was interrupted before compaction).
    store.append(
        [{"safedir": recorded, "img_path_in_safedir": "MTD_MSIL2A.xml",
          "download_status": "complete"}]
    )
    parts_only = store.fingerprint()
    assert parts_only is not None

    granules = s2_reader.discover_s2_granules(data_dir, ("B04",), 20)
    # The state parts, not a directory glob, selected the SAFE to discover.
    assert [granule.safe_dir.name for granule in granules] == [recorded]

    store.compact()
    compacted = store.fingerprint()
    assert compacted != parts_only
    assert compacted == (
        str(store.path), store.path.stat().st_size, store.path.stat().st_mtime_ns
    )  # same shape a single state file always produced
    assert [g.safe_dir.name for g in s2_reader.discover_s2_granules(
        data_dir, ("B04",), 20)] == [recorded]
