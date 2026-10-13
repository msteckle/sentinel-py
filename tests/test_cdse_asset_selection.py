"""AssetSelection decides when cached asset rows satisfy a CDSE download request."""

from __future__ import annotations

import pandas as pd

from sentinel_py.download.cdse import AssetSelection, ResolvedBand

L2A = "S2B_MSIL2A_20230601T000000_N0509_R001_T01XXX_20230601T010000.SAFE"
L1C = "S2A_MSIL1C_20230601T000000_N0509_R001_T01XXX_20230601T010000.SAFE"
BANDS = [ResolvedBand("B02", 10), ResolvedBand("B11", 20)]


def _assets(*rows: tuple[str, float | None]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["band_name", "resolution_m"])


def _s2() -> AssetSelection:
    return AssetSelection("S2", ["B02", "B11"], BANDS)


def test_l2a_selection_returns_requested_bands_and_metadata_only():
    assets = _assets(
        ("B02", 10), ("B11", 20), ("B03", 10),
        ("MTD_MSIL2A", None), ("MTD_TL", None),
    )

    selected = _s2().select(L2A, assets)

    assert selected is not None
    assert sorted(selected["band_name"]) == ["B02", "B11", "MTD_MSIL2A", "MTD_TL"]


def test_l2a_selection_is_incomplete_without_a_requested_band_or_metadata():
    missing_band = _assets(("B02", 10), ("MTD_MSIL2A", None), ("MTD_TL", None))
    missing_tile_metadata = _assets(("B02", 10), ("B11", 20), ("MTD_MSIL2A", None))
    wrong_resolution = _assets(
        ("B02", 20), ("B11", 20), ("MTD_MSIL2A", None), ("MTD_TL", None)
    )

    assert _s2().select(L2A, missing_band) is None
    assert _s2().select(L2A, missing_tile_metadata) is None
    assert _s2().select(L2A, wrong_resolution) is None


def test_l1c_assets_are_matched_at_resolution_zero():
    assets = _assets(
        ("B02", 0), ("B11", 0), ("MTD_MSIL1C", None), ("MTD_TL", None)
    )

    selected = _s2().select(L1C, assets)

    assert selected is not None
    assert len(selected) == 4
    # An L1C scene needs the L1C product metadata, not the L2A one.
    assert _s2().select(L1C, assets[assets["band_name"] != "MTD_MSIL1C"]) is None


def test_s1_selection_matches_names_case_insensitively():
    selection = AssetSelection("S1", ["vv"], None)
    assets = _assets(("VV", 0), ("VH", 0))

    selected = selection.select("S1A_IW_GRDH_scene.SAFE", assets)

    assert selected is not None
    assert selected["band_name"].tolist() == ["VV"]
    assert AssetSelection("S1", ["HH"], None).select("S1A_scene", assets) is None


def test_missing_or_empty_cache_is_incomplete():
    assert _s2().select(L2A, None) is None
    assert _s2().select(L2A, _assets()) is None
