"""Training-table preparation for the per-PFT random-forest processor."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from sentinel_py.enums import MissingCoverPolicy, PFTClass
from sentinel_py.pipeline.processors.pavc_gridded_rf.training import (
    file_digest,
    load_pft_training,
)

PFTS = ["graminoid", "water"]


def _write_tables(tmp_path: Path) -> tuple[Path, Path]:
    """Write a 5-plot training CSV and a PFT table with a flag and unmeasured values."""
    training = pd.DataFrame(
        {
            "site_visit_id": ["a", "b", "c", "d", "ghost"],
            "latitude": [70.0, 70.1, 70.2, 70.3, 70.4],
            "longitude": [-150.0, -150.1, -150.2, -150.3, -150.4],
        }
    )
    cover = pd.DataFrame(
        {
            "site_visit_id": ["a", "b", "c", "d", "unused"],
            "graminoid": [10.0, -9999, 30.0, -9999, 5.0],
            "water": [0.0, 5.0, -9999, -9999, 1.0],
            "qa_review_flag": ["no", "no", "yes", "no", "no"],
        }
    )
    training_path = tmp_path / "training.csv"
    cover_path = tmp_path / "cover.csv"
    training.to_csv(training_path, index=False)
    cover.to_csv(cover_path, index=False)
    return training_path, cover_path


def test_qa_flagged_and_all_missing_plots_are_dropped(tmp_path: Path) -> None:
    training, cover = _write_tables(tmp_path)

    result = load_pft_training(training, cover, pfts=PFTS)

    # "c" is flagged, "d" has no measured PFT, "ghost" has no cover row.
    assert result["graminoid"]["aoi_id"].tolist() == ["a", "b"]
    assert result["graminoid"]["cover"].tolist() == [10.0, 0.0]
    assert result["water"]["cover"].tolist() == [0.0, 5.0]
    assert {"latitude", "longitude"} <= set(result["water"].columns)


def test_qa_filter_can_be_disabled(tmp_path: Path) -> None:
    training, cover = _write_tables(tmp_path)

    result = load_pft_training(training, cover, pfts=PFTS, drop_qa_flagged=False)

    assert result["graminoid"]["aoi_id"].tolist() == ["a", "b", "c"]


def test_drop_policy_removes_unmeasured_plots_per_pft(tmp_path: Path) -> None:
    training, cover = _write_tables(tmp_path)

    result = load_pft_training(
        training, cover, pfts=PFTS, drop_qa_flagged=False, missing_cover="drop"
    )

    assert result["graminoid"]["aoi_id"].tolist() == ["a", "c"]
    assert result["water"]["aoi_id"].tolist() == ["a", "b"]


def test_too_few_rows_for_a_pft_is_an_error(tmp_path: Path) -> None:
    training, cover = _write_tables(tmp_path)

    with pytest.raises(ValueError, match="'graminoid' has 1 training row"):
        load_pft_training(
            training,
            cover,
            pfts=PFTS,
            missing_cover=MissingCoverPolicy.drop,
        )


def test_missing_columns_and_duplicate_ids_are_rejected(tmp_path: Path) -> None:
    training, cover = _write_tables(tmp_path)

    with pytest.raises(ValueError, match="missing columns: forb"):
        load_pft_training(training, cover, pfts=["forb"])

    pd.read_csv(cover).drop(columns="water").to_csv(cover, index=False)
    with pytest.raises(ValueError, match="PFT table .* missing columns: water"):
        load_pft_training(training, cover, pfts=PFTS)

    duplicated = pd.read_csv(training)
    duplicated.loc[1, "site_visit_id"] = "a"
    duplicated.to_csv(training, index=False)
    with pytest.raises(ValueError, match="repeats values of 'site_visit_id'"):
        load_pft_training(training, cover, pfts=["graminoid"])


def test_no_remaining_plots_is_an_error(tmp_path: Path) -> None:
    training, cover = _write_tables(tmp_path)
    pd.read_csv(training).assign(site_visit_id=list("vwxyz")).to_csv(
        training, index=False
    )

    with pytest.raises(ValueError, match="No training plots remain"):
        load_pft_training(training, cover, pfts=PFTS)


def test_file_digest_tracks_content(tmp_path: Path) -> None:
    path = tmp_path / "t.csv"
    path.write_text("a\n")
    first = file_digest(path)
    path.write_text("b\n")

    assert file_digest(path) != first


def test_pft_class_formats_as_its_value() -> None:
    assert f"{PFTClass.bareground}" == "bareground"
