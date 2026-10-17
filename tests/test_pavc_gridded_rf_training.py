"""PFT cover-table loading and feature digests for the per-PFT random-forest node."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from sentinel_py.enums import MissingCoverPolicy, PFTClass
from sentinel_py.pipeline.processors.pavc_gridded_rf.training import (
    features_digest,
    file_digest,
    load_pft_cover,
)

PFTS = ["graminoid", "water"]
PLOTS = ["a", "b", "c", "d", "ghost"]


def _write_cover(tmp_path: Path) -> Path:
    """Write a PFT table with a flagged plot and unmeasured (-9999) values."""
    cover = pd.DataFrame(
        {
            "site_visit_id": ["a", "b", "c", "d", "unused"],
            "graminoid": [10.0, -9999, 30.0, -9999, 5.0],
            "water": [0.0, 5.0, -9999, -9999, 1.0],
            "qa_review_flag": ["no", "no", "yes", "no", "no"],
        }
    )
    path = tmp_path / "cover.csv"
    cover.to_csv(path, index=False)
    return path


def test_qa_flagged_empty_and_unsampled_plots_are_dropped(tmp_path: Path) -> None:
    result = load_pft_cover(_write_cover(tmp_path), PLOTS, pfts=PFTS)

    # "c" is flagged, "d" has no measured PFT, "ghost" is not in the table and
    # "unused" was not sampled.
    assert result["graminoid"]["aoi_id"].tolist() == ["a", "b"]
    assert result["graminoid"]["cover"].tolist() == [10.0, 0.0]
    assert result["water"]["cover"].tolist() == [0.0, 5.0]
    assert list(result["water"].columns) == ["aoi_id", "cover"]


def test_qa_filter_can_be_disabled(tmp_path: Path) -> None:
    result = load_pft_cover(
        _write_cover(tmp_path), PLOTS, pfts=PFTS, drop_qa_flagged=False
    )

    assert result["graminoid"]["aoi_id"].tolist() == ["a", "b", "c"]


def test_drop_policy_removes_unmeasured_plots_per_pft(tmp_path: Path) -> None:
    result = load_pft_cover(
        _write_cover(tmp_path),
        PLOTS,
        pfts=PFTS,
        drop_qa_flagged=False,
        missing_cover="drop",
    )

    assert result["graminoid"]["aoi_id"].tolist() == ["a", "c"]
    assert result["water"]["aoi_id"].tolist() == ["a", "b"]


def test_too_few_rows_for_a_pft_is_an_error(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="'graminoid' has 1 training row"):
        load_pft_cover(
            _write_cover(tmp_path),
            PLOTS,
            pfts=PFTS,
            missing_cover=MissingCoverPolicy.drop,
        )


def test_missing_columns_and_duplicate_ids_are_rejected(tmp_path: Path) -> None:
    path = _write_cover(tmp_path)

    with pytest.raises(ValueError, match="missing columns: forb"):
        load_pft_cover(path, PLOTS, pfts=["forb"])

    duplicated = pd.read_csv(path)
    duplicated.loc[1, "site_visit_id"] = "a"
    duplicated.to_csv(path, index=False)
    with pytest.raises(ValueError, match="repeats values of 'site_visit_id'"):
        load_pft_cover(path, PLOTS, pfts=["graminoid"])


def test_no_remaining_plots_is_an_error(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="No training plots remain"):
        load_pft_cover(_write_cover(tmp_path), ["v", "w"], pfts=PFTS)


def test_file_digest_tracks_content(tmp_path: Path) -> None:
    path = tmp_path / "t.csv"
    path.write_text("a\n")
    first = file_digest(path)
    path.write_text("b\n")

    assert file_digest(path) != first


def test_features_digest_ignores_row_order_but_not_values() -> None:
    table = pd.DataFrame({"aoi_id": ["a", "b"], "x": [1.0, 2.0]})

    assert features_digest(table) == features_digest(table.iloc[::-1])
    assert features_digest(table) != features_digest(table.assign(x=[1.0, 2.5]))


def test_pft_class_formats_as_its_value() -> None:
    assert f"{PFTClass.bareground}" == "bareground"
