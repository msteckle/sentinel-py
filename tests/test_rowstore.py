"""Tests for the keyed-row store, the cache registry, and resumable CDSE preflight."""

from __future__ import annotations

import stat
import threading
from pathlib import Path

import pandas as pd
import pytest

from sentinel_py.cache import REGISTRY, CacheVersionError, RowStore
from sentinel_py.download.cdse import prepare_cdse_download


def _store(tmp_path: Path, **kwargs) -> RowStore:
    return RowStore(tmp_path / "table.parquet", ["id"], **kwargs)


def _rows(*pairs: tuple[str, int]) -> list[dict]:
    return [{"id": key, "value": value} for key, value in pairs]


def test_newest_row_wins_and_parts_override_legacy_base(tmp_path: Path):
    store = _store(tmp_path)
    pd.DataFrame(_rows(("a", 1), ("b", 1))).to_parquet(store.path, index=False)

    store.append(_rows(("b", 2), ("c", 2)))
    store.append(_rows(("c", 3)))

    table = store.read().sort_values("id").reset_index(drop=True)
    assert table["id"].tolist() == ["a", "b", "c"]
    assert table["value"].tolist() == [1, 2, 3]
    assert sorted(store.keys()["id"]) == ["a", "b", "c"]
    assert store.read(columns=["value"]).columns.tolist() == ["value"]


def test_empty_store_reads_empty_and_ignores_empty_appends(tmp_path: Path):
    store = _store(tmp_path)
    assert store.read().empty
    assert store.append([]) == 0
    assert store.part_paths() == []


def test_compact_folds_parts_into_base_and_replaces_read_only_base(tmp_path: Path):
    store = _store(tmp_path, read_only=True)
    store.append(_rows(("a", 1)))
    assert store.compact() == 1
    assert store.part_paths() == []
    assert not store.path.stat().st_mode & stat.S_IWUSR

    store.append(_rows(("a", 2), ("b", 1)))
    assert store.compact() == 1
    on_disk = pd.read_parquet(store.path).sort_values("id")
    assert on_disk["value"].tolist() == [2, 1]
    assert store.compact() == 0


def test_writer_flushes_in_batches_and_compacts_on_clean_exit(tmp_path: Path):
    store = _store(tmp_path)
    with store.writer(batch_rows=3, flush_seconds=3600, compact_on_close=False) as w:
        w.add(_rows(("a", 1), ("b", 1)))
        assert store.part_paths() == []
        w.add(_rows(("c", 1)))  # reaches the batch size: one part holding all three
        assert len(store.part_paths()) == 1
        w.add(_rows(("d", 1)))
    assert len(store.part_paths()) == 2

    with store.writer() as w:
        w.add(_rows(("e", 1)))
    assert store.part_paths() == []
    assert sorted(pd.read_parquet(store.path)["id"]) == ["a", "b", "c", "d", "e"]


def test_writer_keeps_completed_rows_when_interrupted(tmp_path: Path):
    store = _store(tmp_path)
    with pytest.raises(KeyboardInterrupt):
        with store.writer(batch_rows=1000) as w:
            w.add(_rows(("a", 1)))
            raise KeyboardInterrupt
    # Flushed on exit, but not compacted after an interruption.
    assert len(store.part_paths()) == 1
    assert store.read()["id"].tolist() == ["a"]


def test_concurrent_writers_do_not_lose_rows(tmp_path: Path):
    store = _store(tmp_path)

    def work(worker: int) -> None:
        for item in range(20):
            store.append(_rows((f"{worker}-{item}", worker)))

    threads = [threading.Thread(target=work, args=(n,)) for n in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(store.read()) == 80
    store.compact()
    assert len(pd.read_parquet(store.path)) == 80


def test_version_mismatch_is_rejected(tmp_path: Path):
    _store(tmp_path, version=1).append(_rows(("a", 1)))
    with pytest.raises(CacheVersionError):
        _store(tmp_path, version=2).read()


def test_registry_paths_match_module_constants(tmp_path: Path):
    from sentinel_py.download.s1_orbits import ORBIT_STATE_RELPATH
    from sentinel_py.pipeline.readers.s1 import CATALOG_RELPATH
    from sentinel_py.pipeline.writers.cog import cog_state_path

    assert ORBIT_STATE_RELPATH == Path(REGISTRY["s1_orbits"].relpath)
    assert CATALOG_RELPATH == Path(REGISTRY["s1_catalog"].relpath)
    assert cog_state_path(tmp_path) == REGISTRY["cog_outputs"].path(tmp_path)
    with pytest.raises(ValueError):
        REGISTRY["s1_orbits"].path()  # sidecars need an explicit root
    assert REGISTRY["cdse_assets"].store(tmp_path).key_columns == [
        "safedir",
        "img_path_in_safedir",
    ]


def _fake_images(scene: str) -> list[dict]:
    base = {"safedir": scene, "s3_path": "/t", "local_actual_size": None}
    return [
        {**base, "band_name": "B04", "resolution_m": 20,
         "img_path_in_safedir": "B04.jp2", "s3_expected_size": 100,
         "asset_type": "image"},
        {**base, "band_name": "MTD_MSIL2A", "resolution_m": 0,
         "img_path_in_safedir": "MTD_MSIL2A.xml", "s3_expected_size": 10,
         "asset_type": "metadata"},
        {**base, "band_name": "MTD_TL", "resolution_m": 0,
         "img_path_in_safedir": "GRANULE/MTD_TL.xml", "s3_expected_size": 5,
         "asset_type": "metadata"},
    ]


def test_prepare_cdse_download_checkpoints_and_resumes_after_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    scenes_cache = tmp_path / "cache" / "query" / "scenes.parquet"
    scenes_cache.parent.mkdir(parents=True)
    names = [f"S2A_SCENE{n}_MSIL2A.SAFE" for n in range(5)]
    pd.DataFrame({"Name": names, "S3Path": [f"/eodata/{n}" for n in names]}).to_parquet(
        scenes_cache, index=False
    )
    kwargs = dict(
        scenes_cache=scenes_cache, mission="S2", bands=["B04"], resolution=20,
        output_dir=tmp_path / "downloads", config_file=str(tmp_path / ".s5cfg"),
        parallel_scenes=1,
    )

    def failing(scene_name, *args, **kw):
        if scene_name == names[3]:
            raise RuntimeError("listing failed")
        return _fake_images(scene_name)

    monkeypatch.setattr("sentinel_py.download.cdse._list_s2_product_assets", failing)
    with pytest.raises(RuntimeError, match="listing failed"):
        prepare_cdse_download(**kwargs)

    # Scenes resolved before the failure were checkpointed despite the exception.
    store = REGISTRY["cdse_assets"].store(tmp_path / "cache")
    assert set(store.read()["safedir"]) == set(names[:3])

    listed: list[str] = []

    def recording(scene_name, *args, **kw):
        listed.append(scene_name)
        return _fake_images(scene_name)

    monkeypatch.setattr("sentinel_py.download.cdse._list_s2_product_assets", recording)
    summary = prepare_cdse_download(**kwargs)

    assert sorted(listed) == names[3:]
    assert summary.asset_count == 15
    assert set(store.read()["safedir"]) == set(names)
    assert store.part_paths() == []  # clean run compacts back into assets.parquet


def test_read_key_filters_limit_rows_and_reject_non_key_columns(tmp_path: Path):
    store = RowStore(tmp_path / "t.parquet", ["scene", "path"])
    store.append(
        [
            {"scene": "a", "path": "1", "value": 1},
            {"scene": "b", "path": "1", "value": 1},
            {"scene": "c", "path": "1", "value": 1},
        ]
    )
    store.append([{"scene": "b", "path": "1", "value": 2}])

    subset = store.read(key_filters={"scene": ["b", "c"]}).sort_values("scene")

    assert subset["scene"].tolist() == ["b", "c"]
    assert subset["value"].tolist() == [2, 1]  # newest row for "b" still wins
    assert store.read(key_filters={"scene": []}).empty
    with pytest.raises(ValueError, match="key columns"):
        store.read(key_filters={"value": [1]})
