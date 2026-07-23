import hashlib
import json

import pandas as pd

from sentinel_py.cache import (
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
