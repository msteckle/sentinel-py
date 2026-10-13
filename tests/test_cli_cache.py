"""``sentinel-py cache`` list, compact, and prune commands."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from helpers import make_assets_store, make_point_sample, make_query, runner
from sentinel_py.cache import REGISTRY, PruneResult, list_entries
from sentinel_py.cli.main import app


@pytest.fixture
def cache_dir(tmp_path: Path) -> Path:
    """Home cache with one old and one recent query, a sample, and a store."""
    root = tmp_path / "cache"
    make_query(root, "asf", "a" * 32, days=40)
    make_query(root, "asf", "b" * 32, days=1)
    make_point_sample(root, "d" * 32, days=45)
    make_assets_store(root)
    return root


def test_cache_group_lists_its_commands():
    result = runner.invoke(app, ["cache", "--help"])

    assert result.exit_code == 0, result.output
    for command in ("list", "compact", "prune"):
        assert command in result.output


def test_list_prints_a_summary_and_entries(cache_dir: Path):
    summary = runner.invoke(app, ["cache", "list", "--cache-dir", str(cache_dir)])
    detail = runner.invoke(
        app, ["cache", "list", "--cache-dir", str(cache_dir), "--entries"]
    )

    assert summary.exit_code == 0, summary.output
    for namespace in ("asf_queries", "point_samples", "cdse_assets"):
        assert namespace in summary.output
    assert detail.exit_code == 0, detail.output
    assert "a" * 32 in detail.output and "assets.parquet" in detail.output


def test_list_reports_an_empty_cache(tmp_path: Path):
    result = runner.invoke(app, ["cache", "list", "--cache-dir", str(tmp_path / "none")])

    assert result.exit_code == 0, result.output
    assert "No cache entries found" in result.output


def test_list_includes_sidecars_for_named_data_directories(
    tmp_path: Path, cache_dir: Path
):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    REGISTRY["asf_downloads"].store(data_dir).append([{"url": "u"}])

    result = runner.invoke(
        app,
        ["cache", "list", "--cache-dir", str(cache_dir), "--data-dir", str(data_dir)],
    )

    assert result.exit_code == 0, result.output
    assert "asf_downloads" in result.output


def test_compact_dry_run_then_compacts(cache_dir: Path):
    store = REGISTRY["cdse_assets"].store(cache_dir / "cdse")
    args = ["cache", "compact", "--cache-dir", str(cache_dir)]

    dry = runner.invoke(app, [*args, "--dry-run"])
    assert dry.exit_code == 0, dry.output
    assert "Would compact 2 part(s)" in dry.output
    assert len(store.part_paths()) == 2

    real = runner.invoke(app, args)
    assert real.exit_code == 0, real.output
    assert "Compacted 2 part(s)" in real.output
    assert store.part_paths() == []
    assert len(pd.read_parquet(store.path)) == 6

    again = runner.invoke(app, args)
    assert again.exit_code == 0
    assert "Nothing to compact" in again.output


def test_prune_dry_run_deletes_nothing(cache_dir: Path):
    before = {e.path for e in list_entries(cache_dir)}

    result = runner.invoke(
        app,
        ["cache", "prune", "--older-than", "30", "--cache-dir", str(cache_dir),
         "--dry-run"],
    )

    assert result.exit_code == 0, result.output
    assert "Dry run: nothing was deleted." in result.output
    assert {e.path for e in list_entries(cache_dir)} == before


def test_prune_declined_prompt_deletes_nothing(cache_dir: Path):
    before = {e.path for e in list_entries(cache_dir)}

    result = runner.invoke(
        app,
        ["cache", "prune", "--older-than", "30", "--cache-dir", str(cache_dir)],
        input="n\n",
    )

    assert result.exit_code == 0, result.output
    assert "Prune cancelled." in result.output
    assert {e.path for e in list_entries(cache_dir)} == before


def test_prune_yes_deletes_only_old_queries_and_samples(cache_dir: Path):
    result = runner.invoke(
        app,
        ["cache", "prune", "--older-than", "30", "--cache-dir", str(cache_dir),
         "--yes"],
    )

    assert result.exit_code == 0, result.output
    assert "Deleted 2 entr(ies)" in result.output
    remaining = {e.path.name for e in list_entries(cache_dir)}
    assert remaining == {"b" * 32, "assets.parquet"}


def test_prune_provider_filter_and_nothing_matched(cache_dir: Path):
    only_samples = runner.invoke(
        app,
        ["cache", "prune", "--older-than", "0", "--provider", "point-samples",
         "--cache-dir", str(cache_dir), "--yes"],
    )
    assert only_samples.exit_code == 0, only_samples.output
    names = {e.path.name for e in list_entries(cache_dir)}
    assert {"a" * 32, "b" * 32} <= names and "d" * 32 + ".parquet" not in names

    nothing = runner.invoke(
        app,
        ["cache", "prune", "--older-than", "365", "--cache-dir", str(cache_dir),
         "--yes"],
    )
    assert nothing.exit_code == 0
    assert "Nothing to prune" in nothing.output


def test_prune_requires_older_than(cache_dir: Path):
    result = runner.invoke(app, ["cache", "prune", "--cache-dir", str(cache_dir)])

    assert result.exit_code != 0
    assert "--older-than" in result.output


def test_prune_exits_nonzero_when_a_deletion_fails(cache_dir: Path, monkeypatch):
    def failing(entries, cache_dir, **kwargs):
        entries = tuple(entries)
        return PruneResult((), tuple((entry, "permission denied") for entry in entries))

    monkeypatch.setattr("sentinel_py.cli.cache.prune.delete_entries", failing)

    result = runner.invoke(
        app,
        ["cache", "prune", "--older-than", "30", "--cache-dir", str(cache_dir),
         "--yes"],
    )

    assert result.exit_code == 1
    assert "permission denied" in result.output
