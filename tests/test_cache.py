import hashlib
import json

import pandas as pd

from sentinel_py.cache import (
    DEFAULT_ASF_CACHE_DIR,
    DEFAULT_CACHE_DIR,
    DEFAULT_CDSE_CACHE_DIR,
    SENTINEL_PY_HOME,
    QueryCache,
    deterministic_cache_key,
    merge_state_rows,
    write_json_atomic,
    write_parquet_atomic,
)


def test_deterministic_cache_key_preserves_historical_format():
    payload = {"b": 2, "a": 1}
    expected = hashlib.md5(
        json.dumps(payload, sort_keys=True).encode()
    ).hexdigest()

    assert deterministic_cache_key(payload) == expected


def test_atomic_cache_writes_and_state_merge(tmp_path):
    json_path = tmp_path / "cache" / "query.json"
    parquet_path = tmp_path / "cache" / "state.parquet"

    write_json_atomic(json_path, {"value": 1})
    write_parquet_atomic(
        pd.DataFrame([{"url": "one", "status": "pending"}]),
        parquet_path,
    )
    existing = pd.read_parquet(parquet_path)
    merged = merge_state_rows(
        existing,
        [{"url": "one", "status": "complete"}],
        key_columns=["url"],
    )
    write_parquet_atomic(merged, parquet_path)

    assert json.loads(json_path.read_text()) == {"value": 1}
    assert pd.read_parquet(parquet_path).to_dict("records") == [
        {"url": "one", "status": "complete"}
    ]
    assert not list((tmp_path / "cache").glob("*.tmp"))


def test_query_cache_round_trips_table_and_info(tmp_path):
    payload = {"provider": "TEST", "years": [2023]}
    cache = QueryCache.for_payload(tmp_path, payload, "manifest.parquet")

    assert not cache.exists()
    assert cache.load_info() == {}

    table = pd.DataFrame([{"url": "one"}, {"url": "two"}])
    cache.save(table, {"num_products": 2, "when": "now"})

    reopened = QueryCache.for_payload(tmp_path, payload, "manifest.parquet")
    assert reopened.directory == cache.directory
    assert reopened.exists()
    assert reopened.load()["url"].tolist() == ["one", "two"]
    assert reopened.load_info()["num_products"] == 2
    assert reopened.info_path.name == "query_info.json"


def test_query_cache_keys_differ_by_payload(tmp_path):
    first = QueryCache.for_payload(tmp_path, {"a": 1}, "manifest.parquet")
    second = QueryCache.for_payload(tmp_path, {"a": 2}, "manifest.parquet")

    assert first.directory != second.directory


def test_default_provider_caches_share_sentinel_py_home():
    assert DEFAULT_CACHE_DIR == SENTINEL_PY_HOME / "cache"
    assert DEFAULT_CDSE_CACHE_DIR == DEFAULT_CACHE_DIR / "cdse"
    assert DEFAULT_ASF_CACHE_DIR == DEFAULT_CACHE_DIR / "asf"
