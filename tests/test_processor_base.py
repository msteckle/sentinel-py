"""Shared processor base helpers: recipe IDs, single-input access, nodata masking."""

from __future__ import annotations

import numpy as np
import pytest

from sentinel_py.cache import deterministic_cache_key
from sentinel_py.pipeline.processors import get_default_registry
from sentinel_py.pipeline.processors.base import make_recipe_id, mask_nodata


def test_make_recipe_id_matches_the_historical_payload_for_chained_processors():
    legacy = deterministic_cache_key(
        {
            "processor": "s2.index",
            "version": 2,
            "indices": ("NDGI",),
            "source_recipe": "abc123",
        }
    )[:12]

    assert (
        make_recipe_id("s2.index", 2, indices=("NDGI",), source_recipe="abc123")
        == legacy
    )


def test_make_recipe_id_changes_with_version_and_parameters():
    base = make_recipe_id("composite", 2, period="all", aggregation="mean")

    assert base != make_recipe_id("composite", 3, period="all", aggregation="mean")
    assert base != make_recipe_id("composite", 2, period="1M", aggregation="mean")
    assert base != make_recipe_id("other", 2, period="all", aggregation="mean")
    assert len(base) == 12


def test_single_input_returns_the_only_upstream_value():
    processor = get_default_registry().get("composite")

    assert processor.single_input({"data": 7}) == 7
    with pytest.raises(ValueError, match="exactly one upstream input"):
        processor.single_input({})
    with pytest.raises(ValueError, match="exactly one upstream input"):
        processor.single_input({"a": 1, "b": 2})


def test_mask_nodata_replaces_the_sentinel_with_nan_and_ignores_none():
    xr = pytest.importorskip("xarray")
    values = xr.DataArray(np.array([1.0, 65535.0, 3.0], dtype=np.float32))

    masked = mask_nodata(values, 65535)

    assert np.isnan(masked.values[1])
    assert masked.values[[0, 2]].tolist() == [1.0, 3.0]
    assert mask_nodata(values, None) is values
