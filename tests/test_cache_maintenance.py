"""Tests for cache inspection, compaction, and pruning."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from helpers import make_assets_store, make_point_sample, make_query
from sentinel_py.cache import (
    REGISTRY,
    compact_stores,
    delete_entries,
    list_entries,
    select_prunable,
)
from sentinel_py.enums import CacheKind, CachePruneTarget

@pytest.fixture
def cache_tree(tmp_path: Path) -> Path:
    """A home cache with old and new queries, a point sample, and a store."""
    root = tmp_path / "cache"
    make_query(root, "asf", "a" * 32, days=40)
    make_query(root, "asf", "b" * 32, days=1)
    make_query(root, "cdse", "c" * 32, days=60)
    make_point_sample(root, "d" * 32, days=45)
    make_assets_store(root)
    return root


def test_list_entries_discovers_queries_samples_and_stores(cache_tree: Path):
    entries = {(e.namespace, e.path.name): e for e in list_entries(cache_tree)}

    assert {namespace for namespace, _ in entries} == {
        "asf_queries",
        "cdse_queries",
        "point_samples",
        "cdse_assets",
    }
    old = entries[("asf_queries", "a" * 32)]
    assert old.prunable and old.kind is CacheKind.mutable
    assert old.rows == 2 and old.files == 2
    assert "2 products" in old.description and "collection=S1" in old.description
    assert entries[("point_samples", "d" * 32 + ".parquet")].rows == 3

    store = entries[("cdse_assets", "assets.parquet")]
    assert not store.prunable
    assert store.parts == 2 and store.rows == 6 and store.store is not None
    assert store.description == REGISTRY["cdse_assets"].description


def test_list_entries_finds_sidecars_only_in_named_directories(
    tmp_path: Path, cache_tree: Path
):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    REGISTRY["asf_downloads"].store(data_dir).append([{"url": "u", "status": "ok"}])
    elsewhere = tmp_path / "elsewhere"
    REGISTRY["pgc_downloads"].store(elsewhere).append([{"url": "u"}])

    without = {e.namespace for e in list_entries(cache_tree)}
    with_data = {e.namespace for e in list_entries(cache_tree, [data_dir])}

    assert "asf_downloads" not in without
    assert "asf_downloads" in with_data
    assert "pgc_downloads" not in with_data  # never discovered by scanning


def test_list_entries_tolerates_unreadable_tables(cache_tree: Path):
    broken = cache_tree / "pgc" / ("e" * 32)
    broken.mkdir(parents=True)
    (broken / "manifest.parquet").write_bytes(b"not parquet")
    (cache_tree / "asf" / "not-a-query-key").mkdir()

    entries = {e.path.name: e for e in list_entries(cache_tree)}

    assert entries["e" * 32].rows is None
    assert "not-a-query-key" not in entries


def test_compact_stores_dry_run_then_folds_parts(cache_tree: Path):
    entries = list_entries(cache_tree)
    store = next(e for e in entries if e.namespace == "cdse_assets").store

    dry = compact_stores(entries, dry_run=True)
    assert [(r.parts, r.folded) for r in dry] == [(2, 0)]
    assert len(store.part_paths()) == 2

    done = compact_stores(entries)
    assert [(r.namespace, r.parts, r.folded) for r in done] == [("cdse_assets", 2, 2)]
    assert store.part_paths() == []
    assert len(pd.read_parquet(store.path)) == 6
    assert compact_stores(list_entries(cache_tree)) == []


def test_select_prunable_filters_by_age_and_provider_and_skips_stores(
    cache_tree: Path,
):
    entries = list_entries(cache_tree)

    old = select_prunable(entries, older_than_days=30)
    assert {e.path.name for e in old} == {"a" * 32, "c" * 32, "d" * 32 + ".parquet"}
    assert all(e.prunable for e in old)

    asf_only = select_prunable(
        entries, older_than_days=30, providers=[CachePruneTarget.asf]
    )
    assert [e.path.name for e in asf_only] == ["a" * 32]
    samples_only = select_prunable(
        entries, older_than_days=0, providers=[CachePruneTarget.point_samples]
    )
    assert [e.namespace for e in samples_only] == ["point_samples"]

    everything = select_prunable(entries, older_than_days=0)
    assert "cdse_assets" not in {e.namespace for e in everything}
    assert len(everything) == 4


def test_delete_entries_removes_only_selected_entries(cache_tree: Path):
    entries = list_entries(cache_tree)
    selected = select_prunable(entries, older_than_days=30)

    result = delete_entries(selected, cache_tree)

    assert len(result.deleted) == 3 and not result.failed
    remaining = {e.path.name for e in list_entries(cache_tree)}
    assert remaining == {"b" * 32, "assets.parquet"}


def test_delete_entries_refuses_unsafe_targets(tmp_path: Path, cache_tree: Path):
    entries = list_entries(cache_tree)
    store_entry = next(e for e in entries if e.namespace == "cdse_assets")
    with pytest.raises(ValueError, match="non-prunable"):
        delete_entries([store_entry], cache_tree)

    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "keep.txt").write_text("keep")
    query = next(e for e in entries if e.path.name == "a" * 32)
    escaped = type(query)(**{**query.__dict__, "path": outside})
    link_target = cache_tree / "asf" / ("f" * 32)
    link_target.symlink_to(outside, target_is_directory=True)
    linked = type(query)(**{**query.__dict__, "path": link_target})

    result = delete_entries([escaped, linked], cache_tree)

    assert not result.deleted and len(result.failed) == 2
    assert (outside / "keep.txt").read_text() == "keep"
