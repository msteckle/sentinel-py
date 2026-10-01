from __future__ import annotations

import pytest
import typer

from sentinel_py.download.preflight import DownloadStorageSummary, confirm_download


def test_confirm_download_skips_known_complete_download(monkeypatch) -> None:
    def fail_confirm(*args, **kwargs):
        raise AssertionError("confirmation should not be requested")

    monkeypatch.setattr("typer.confirm", fail_confirm)
    confirm_download(
        assume_yes=False,
        storage=DownloadStorageSummary(
            asset_count=10,
            known_total_bytes=100,
            known_additional_bytes=0,
            unknown_size_assets=0,
        ),
    )


def test_confirm_download_prompts_for_unknown_sizes(monkeypatch) -> None:
    prompted = False

    def confirm(*args, **kwargs):
        nonlocal prompted
        prompted = True
        return True

    monkeypatch.setattr("typer.confirm", confirm)
    confirm_download(
        assume_yes=False,
        storage=DownloadStorageSummary(
            asset_count=10,
            known_total_bytes=100,
            known_additional_bytes=0,
            unknown_size_assets=1,
        ),
    )

    assert prompted


def test_confirm_download_still_cancels_positive_download(monkeypatch) -> None:
    monkeypatch.setattr("typer.confirm", lambda *args, **kwargs: False)

    with pytest.raises(typer.Exit):
        confirm_download(
            assume_yes=False,
            storage=DownloadStorageSummary(
                asset_count=1,
                known_total_bytes=100,
                known_additional_bytes=100,
                unknown_size_assets=0,
            ),
        )
