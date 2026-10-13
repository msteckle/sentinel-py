"""ASF query and download behavior: manifest rows, verification, retries, and state."""

import hashlib
from pathlib import Path

import pandas as pd
import pytest
import requests

import sentinel_py.download.asf as asf_download_module
from sentinel_py.download.asf import (
    ASF_MANIFEST_COLUMNS,
    _existing_file_is_valid,
    _feature_to_row,
    download_asf,
    get_predominant_flightdir,
    query_asf,
)
from sentinel_py.enums import DownloadStatus


class EmptyASFResults:
    def geojson(self):
        return {"type": "FeatureCollection", "features": []}


class OneASFResult:
    def geojson(self):
        return {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {
                        "fileName": "one.zip",
                        "url": "https://example.test/one.zip",
                        "flightDirection": "ASCENDING",
                        "bytes": 1048576,
                        "md5sum": "abc123",
                    },
                }
            ],
        }


def test_query_asf_handles_no_results(monkeypatch):
    monkeypatch.setattr(
        "sentinel_py.download.asf.asf.search",
        lambda **kwargs: EmptyASFResults(),
    )

    manifest = query_asf(
        aoi_wkt="POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))",
        date_start="2024-01-01",
        date_end="2024-01-02",
        product_levels=["SLC"],
    )

    assert manifest.empty
    assert manifest.columns.tolist() == ASF_MANIFEST_COLUMNS


def test_query_asf_preserves_exact_size_and_checksum(monkeypatch):
    monkeypatch.setattr(
        "sentinel_py.download.asf.asf.search",
        lambda **kwargs: OneASFResult(),
    )

    manifest = query_asf(
        aoi_wkt="POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))",
        date_start="2024-01-01",
        date_end="2024-01-02",
        product_levels=["SLC"],
    )

    assert manifest.iloc[0]["expected_size"] == 1048576
    assert manifest.iloc[0]["sizeMB"] == 1.0
    assert manifest.iloc[0]["md5sum"] == "abc123"


def test_query_asf_translates_multipoint_to_cmr_point_or(monkeypatch):
    calls = []

    def fake_search(**kwargs):
        calls.append(kwargs)
        return EmptyASFResults()

    monkeypatch.setattr("sentinel_py.download.asf.asf.search", fake_search)

    query_asf(
        aoi_wkt="MULTIPOINT ((-150 68), (20 -30))",
        date_start="2024-01-01",
        date_end="2024-01-02",
        product_levels=["GRD_HD"],
        season=[1, 2],
    )

    assert len(calls) == 1
    assert "intersectsWith" not in calls[0]
    assert calls[0]["season"] == [1, 2]
    assert calls[0]["cmr_keywords"] == [
        ("point", "-150.0,68.0"),
        ("point", "20.0,-30.0"),
        ("options[point][or]", "true"),
    ]


def test_query_asf_batches_multipoint_for_cmr_condition_limit(monkeypatch):
    calls = []

    def fake_search(**kwargs):
        calls.append(kwargs)
        return EmptyASFResults()

    monkeypatch.setattr("sentinel_py.download.asf.asf.search", fake_search)
    monkeypatch.setattr("sentinel_py.download.asf.CMR_POINT_BATCH_SIZE", 2)

    query_asf(
        aoi_wkt="MULTIPOINT ((-150 68), (20 -30), (40 10))",
        date_start="2024-01-01",
        date_end="2024-01-02",
        product_levels=["GRD_HD"],
    )

    assert len(calls) == 2
    keyword_batches = [call["cmr_keywords"] for call in calls]
    assert [
        ("point", "-150.0,68.0"),
        ("point", "20.0,-30.0"),
        ("options[point][or]", "true"),
    ] in keyword_batches
    assert [
        ("point", "40.0,10.0"),
        ("options[point][or]", "true"),
    ] in keyword_batches


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        (
            {"product_levels": ["RTC_HIGH_RES"]},
            "Unsupported Sentinel-1 ASF product level",
        ),
        (
            {"product_levels": ["GRD_HD"], "beam_mode": "FBS"},
            "Unsupported Sentinel-1 ASF beam mode",
        ),
        (
            {"product_levels": ["GRD_HD"], "polarization": "quadrature"},
            "Unsupported Sentinel-1 ASF polarization",
        ),
    ],
)
def test_query_asf_rejects_options_from_other_missions(kwargs, message):
    with pytest.raises(ValueError, match=message):
        query_asf(
            aoi_wkt="POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))",
            date_start="2024-01-01",
            date_end="2024-01-02",
            **kwargs,
        )


def test_predominant_flight_direction_tie_selects_ascending():
    manifest = pd.DataFrame({"flightDirection": ["DESCENDING", "ASCENDING"]})

    assert get_predominant_flightdir(manifest) == "ASCENDING"


def test_download_asf_validates_size_persists_state_and_reports_progress(
    tmp_path: Path,
    monkeypatch,
    capsys,
):
    monkeypatch.setenv("COLUMNS", "200")
    monkeypatch.setattr(
        "sentinel_py.download.asf._build_asf_session",
        lambda *args: None,
    )
    products = pd.DataFrame(
        [
            {
                "granule": "one.zip",
                "url": "https://example.test/one.zip",
                "expected_size": 4,
                "sizeMB": 4 / (1024 * 1024),
                "md5sum": None,
            }
        ]
    )
    outdir = tmp_path / "downloads"
    download_calls = 0

    def fake_download_url(*, path, filename, **kwargs):
        nonlocal download_calls
        download_calls += 1
        (Path(path) / filename).write_bytes(b"data")

    monkeypatch.setattr(
        "sentinel_py.download.asf.asf_download_url",
        fake_download_url,
    )

    config = tmp_path / "unused.netrc"
    messages = download_asf(products, outdir, config_file=config, processes=1)

    target = outdir / "one.zip"
    state_file = outdir / ".sentinel-py" / "asf_downloads.parquet"
    assert target.read_bytes() == b"data"
    assert not (outdir / ".one.zip.part").exists()
    assert download_calls == 1
    assert "1 downloaded, 0 skipped, 0 failed" in messages[-2]
    state = pd.read_parquet(state_file)
    assert state.iloc[0]["status"] == "complete"
    assert state.iloc[0]["local_actual_size"] == 4
    progress_output = capsys.readouterr().out
    assert "Checking & downloading ASF products" in progress_output
    assert "downloaded 1 · skipped 0 · failed 0 · cache updated 1" in progress_output

    messages = download_asf(products, outdir, config_file=config, processes=1)

    assert download_calls == 1
    assert "0 downloaded, 1 skipped, 0 failed" in messages[-2]

    target.write_bytes(b"bad")
    messages = download_asf(products, outdir, config_file=config, processes=1)

    assert download_calls == 2
    assert target.read_bytes() == b"data"
    assert "1 downloaded, 0 skipped, 0 failed" in messages[-2]


def test_download_asf_validates_md5_for_new_and_existing_files(
    tmp_path: Path,
    monkeypatch,
):
    monkeypatch.setattr(
        "sentinel_py.download.asf._build_asf_session",
        lambda *args: None,
    )
    expected_bytes = b"correct"
    products = pd.DataFrame(
        [
            {
                "granule": "one.zip",
                "url": "https://example.test/one.zip",
                "expected_size": len(expected_bytes),
                "md5sum": hashlib.md5(expected_bytes).hexdigest(),
            }
        ]
    )
    outdir = tmp_path / "downloads"
    download_calls = 0

    def fake_download(*, path, filename, **kwargs):
        nonlocal download_calls
        download_calls += 1
        (Path(path) / filename).write_bytes(expected_bytes)

    monkeypatch.setattr(
        "sentinel_py.download.asf.asf_download_url",
        fake_download,
    )

    summary = download_asf(
        products,
        outdir,
        config_file=tmp_path / "unused.netrc",
        processes=1,
    )
    assert summary.failed == 0
    assert download_calls == 1
    state = pd.read_parquet(outdir / ".sentinel-py" / "asf_downloads.parquet")
    assert state.iloc[0]["local_mtime_ns"] == (outdir / "one.zip").stat().st_mtime_ns
    assert state.iloc[0]["fingerprint_version"] == 1

    # Simulate the short-lived legacy state format that rounded nanosecond mtimes
    # through float64. It should migrate without re-reading the ZIP.
    state["local_mtime_ns"] = state["local_mtime_ns"].astype(float)
    state = state.drop(columns=["fingerprint_version"])
    state.to_parquet(
        outdir / ".sentinel-py" / "asf_downloads.parquet",
        index=False,
    )

    original_file_md5 = asf_download_module._file_md5
    hash_calls = 0

    def counting_file_md5(path):
        nonlocal hash_calls
        hash_calls += 1
        return original_file_md5(path)

    monkeypatch.setattr(
        "sentinel_py.download.asf._file_md5",
        counting_file_md5,
    )
    summary = download_asf(
        products,
        outdir,
        config_file=tmp_path / "unused.netrc",
        processes=1,
    )
    assert summary.skipped == 1
    assert hash_calls == 0
    migrated_state = pd.read_parquet(outdir / ".sentinel-py" / "asf_downloads.parquet")
    assert (
        migrated_state.iloc[0]["local_mtime_ns"]
        == (outdir / "one.zip").stat().st_mtime_ns
    )
    assert migrated_state.iloc[0]["fingerprint_version"] == 1

    (outdir / "one.zip").write_bytes(b"badfile")
    summary = download_asf(
        products,
        outdir,
        config_file=tmp_path / "unused.netrc",
        processes=1,
    )
    assert summary.failed == 0
    assert download_calls == 2
    assert hash_calls == 2
    assert (outdir / "one.zip").read_bytes() == expected_bytes


def test_download_asf_retries_md5_mismatch_and_reports_failure(
    tmp_path: Path,
    monkeypatch,
):
    monkeypatch.setattr(
        "sentinel_py.download.asf._build_asf_session",
        lambda *args: None,
    )
    products = pd.DataFrame(
        [
            {
                "granule": "one.zip",
                "url": "https://example.test/one.zip",
                "expected_size": 4,
                "md5sum": hashlib.md5(b"good").hexdigest(),
            }
        ]
    )
    calls = 0

    def fake_download(*, path, filename, **kwargs):
        nonlocal calls
        calls += 1
        (Path(path) / filename).write_bytes(b"evil")

    monkeypatch.setattr(
        "sentinel_py.download.asf.asf_download_url",
        fake_download,
    )
    monkeypatch.setattr("sentinel_py.download.asf.time.sleep", lambda delay: None)

    summary = download_asf(
        products,
        tmp_path / "downloads",
        config_file=tmp_path / "unused.netrc",
        processes=1,
        retries=1,
    )

    assert calls == 2
    assert summary.failed == 1
    assert not (tmp_path / "downloads" / "one.zip").exists()
    state = pd.read_parquet(
        tmp_path / "downloads" / ".sentinel-py" / "asf_downloads.parquet"
    )
    assert "MD5 mismatch" in state.iloc[0]["error"]


def test_download_asf_batches_state_writes(tmp_path: Path, monkeypatch):
    outdir = tmp_path / "downloads"
    outdir.mkdir()
    products = []
    for index in range(101):
        filename = f"{index:03d}.zip"
        (outdir / filename).write_bytes(b"x")
        products.append(
            {
                "granule": filename,
                "url": f"https://example.test/{filename}",
                "expected_size": 1,
            }
        )

    from sentinel_py.cache import RowStore

    original_append = RowStore.append
    append_sizes: list[int] = []

    def counting_append(self, rows):
        frame = rows if isinstance(rows, pd.DataFrame) else pd.DataFrame(list(rows))
        append_sizes.append(len(frame))
        return original_append(self, rows)

    monkeypatch.setattr(RowStore, "append", counting_append)

    summary = download_asf(
        pd.DataFrame(products),
        outdir,
        config_file=tmp_path / "unused.netrc",
        processes=8,
    )

    assert summary.skipped == 101
    # One part per 100 buffered rows, plus the remainder.
    assert append_sizes == [100, 1]
    state = pd.read_parquet(outdir / ".sentinel-py" / "asf_downloads.parquet")
    assert len(state) == 101


def test_download_asf_does_not_replace_target_and_cleans_partial_file(
    tmp_path: Path,
    monkeypatch,
):
    monkeypatch.setattr(
        "sentinel_py.download.asf._build_asf_session",
        lambda *args: None,
    )
    products = pd.DataFrame(
        [
            {
                "granule": "one.zip",
                "url": "https://example.test/one.zip",
                "expected_size": 4,
            }
        ]
    )
    outdir = tmp_path / "downloads"
    outdir.mkdir()
    target = outdir / "one.zip"
    target.write_bytes(b"old")

    def fake_short_download(*, path, filename, **kwargs):
        (Path(path) / filename).write_bytes(b"xx")

    monkeypatch.setattr(
        "sentinel_py.download.asf.asf_download_url",
        fake_short_download,
    )

    messages = download_asf(
        products,
        outdir,
        config_file=tmp_path / "unused.netrc",
        processes=1,
    )

    assert target.read_bytes() == b"old"
    assert not (outdir / ".one.zip.part").exists()
    assert "0 downloaded, 0 skipped, 1 failed" in messages[-2]
    state = pd.read_parquet(outdir / ".sentinel-py" / "asf_downloads.parquet")
    assert state.iloc[0]["status"] == "failed"


def test_download_asf_retries_transient_failure_then_succeeds(
    tmp_path: Path,
    monkeypatch,
):
    monkeypatch.setattr(
        "sentinel_py.download.asf._build_asf_session",
        lambda *args: None,
    )
    products = pd.DataFrame(
        [
            {
                "granule": "one.zip",
                "url": "https://example.test/one.zip",
                "expected_size": 4,
            }
        ]
    )
    calls = 0
    sleeps = []

    def fake_download(*, path, filename, **kwargs):
        nonlocal calls
        calls += 1
        if calls < 3:
            raise requests.exceptions.ConnectionError("temporary DNS failure")
        (Path(path) / filename).write_bytes(b"data")

    monkeypatch.setattr(
        "sentinel_py.download.asf.asf_download_url",
        fake_download,
    )
    monkeypatch.setattr("sentinel_py.download.asf.time.sleep", sleeps.append)

    messages = download_asf(
        products,
        tmp_path / "downloads",
        config_file=tmp_path / "unused.netrc",
        processes=1,
        retries=3,
        retry_backoff=0.5,
    )

    assert calls == 3
    assert sleeps == [0.5, 1.0]
    assert (tmp_path / "downloads" / "one.zip").read_bytes() == b"data"
    assert "1 downloaded, 0 skipped, 0 failed" in messages[-2]


def test_download_asf_cleans_partial_file_on_keyboard_interrupt(
    tmp_path: Path,
    monkeypatch,
):
    monkeypatch.setattr(
        "sentinel_py.download.asf._build_asf_session",
        lambda *args: None,
    )
    products = pd.DataFrame(
        [
            {
                "granule": "one.zip",
                "url": "https://example.test/one.zip",
                "expected_size": 4,
            }
        ]
    )
    outdir = tmp_path / "downloads"
    outdir.mkdir()

    def fake_interrupted_download(*, path, filename, **kwargs):
        (Path(path) / filename).write_bytes(b"xx")
        raise KeyboardInterrupt

    monkeypatch.setattr(
        "sentinel_py.download.asf.asf_download_url",
        fake_interrupted_download,
    )

    with pytest.raises(KeyboardInterrupt):
        download_asf(
            products,
            outdir,
            config_file=tmp_path / "unused.netrc",
            processes=1,
        )

    assert not (outdir / ".one.zip.part").exists()
    assert not (outdir / "one.zip").exists()


def test_feature_to_row_derives_size_and_prefers_path_number():
    row = _feature_to_row(
        {
            "properties": {
                "fileName": "S1A_scene",
                "url": "https://example.test/S1A_scene.zip",
                "bytes": 2 * 1024 * 1024,
                "pathNumber": 42,
                "relativeOrbit": 7,
                "md5sum": "abc",
            }
        }
    )

    assert row["granule"] == "S1A_scene"
    assert row["expected_size"] == 2 * 1024 * 1024
    assert row["sizeMB"] == 2.0
    assert row["relativeOrbit"] == 42


def test_feature_to_row_keeps_reported_size_mb_without_bytes():
    row = _feature_to_row({"properties": {"url": "u", "sizeMB": 1.5}})

    assert row["expected_size"] is None
    assert row["sizeMB"] == 1.5


def test_existing_file_validity_checks_size_and_checksum(tmp_path: Path):
    target = tmp_path / "scene.zip"
    target.write_bytes(b"payload")
    digest = hashlib.md5(b"payload").hexdigest()

    ok, size, _ = _existing_file_is_valid(
        target, filename="scene.zip", expected_size=7, expected_md5=digest, cached={}
    )
    assert ok and size == 7
    assert not _existing_file_is_valid(
        target, filename="scene.zip", expected_size=8, expected_md5=None, cached={}
    )[0]
    assert not _existing_file_is_valid(
        target, filename="scene.zip", expected_size=7, expected_md5="0" * 32, cached={}
    )[0]


def test_existing_file_is_trusted_from_complete_state_without_manifest_size(
    tmp_path: Path,
):
    target = tmp_path / "scene.zip"
    target.write_bytes(b"payload")
    cached = {"status": DownloadStatus.complete, "local_actual_size": 7}

    assert _existing_file_is_valid(
        target, filename="scene.zip", expected_size=None, expected_md5=None, cached=cached
    )[0]
    assert not _existing_file_is_valid(
        target, filename="scene.zip", expected_size=None, expected_md5=None, cached={}
    )[0]
