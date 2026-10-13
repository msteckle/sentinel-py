"""Unit tests for the small ASF manifest and skip-decision helpers."""

from __future__ import annotations

import hashlib
from pathlib import Path

from sentinel_py.download.asf import _existing_file_is_valid, _feature_to_row
from sentinel_py.enums import DownloadStatus


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
