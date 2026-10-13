"""Tests for the shared pipeline component registry and its default instances."""

from __future__ import annotations

import pytest

from sentinel_py.pipeline.processors import get_default_registry
from sentinel_py.pipeline.readers.base import get_default_reader_registry
from sentinel_py.pipeline.writers import get_default_writer_registry


def test_default_registries_are_memoized():
    assert get_default_registry() is get_default_registry()
    assert get_default_reader_registry() is get_default_reader_registry()
    assert get_default_writer_registry() is get_default_writer_registry()


def test_default_registries_expose_builtin_names():
    assert set(get_default_registry().type_names) >= {
        "s2.preprocess",
        "composite",
        "s2.index",
        "dem",
    }
    assert set(get_default_reader_registry().source_types) == {
        "s1.grd.local",
        "s2.l2a.local",
        "dem.local",
    }
    writers = get_default_writer_registry()
    assert {writers.get(name).format_name for name in ("cog", "parquet", "xarray")} == {
        "cog",
        "parquet",
        "xarray",
    }


def test_registry_rejects_unknown_names_with_kind_in_message():
    with pytest.raises(KeyError, match="Unknown processor type"):
        get_default_registry().get("nope")
    with pytest.raises(KeyError, match="Unknown source reader type"):
        get_default_reader_registry().get("nope")
