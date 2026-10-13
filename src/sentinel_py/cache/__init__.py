"""Shared cache locations, atomic-write primitives, row stores, and the registry.

Everything historically importable from ``sentinel_py.cache`` is re-exported here.
"""

from sentinel_py.cache.base import (
    QueryCache,
    DEFAULT_ASF_CACHE_DIR,
    DEFAULT_CACHE_DIR,
    DEFAULT_CDSE_CACHE_DIR,
    DEFAULT_PGC_CACHE_DIR,
    DEFAULT_PIPELINE_CACHE_DIR,
    SENTINEL_PY_HOME,
    SENTINEL_PY_HOME_ENV,
    cache_directory,
    deterministic_cache_key,
    find_latest_cache_file,
    legacy_provider_cache_root,
    mark_cache_used,
    merge_parquet_cache,
    merge_state_rows,
    write_json_atomic,
    write_parquet_atomic,
)
from sentinel_py.cache.maintenance import (
    CacheEntry,
    CompactResult,
    PruneResult,
    compact_stores,
    delete_entries,
    list_entries,
    select_prunable,
)
from sentinel_py.cache.registry import (
    REGISTRY,
    CacheNamespace,
)
from sentinel_py.cache.rowstore import CacheVersionError, RowStore, RowWriter

__all__ = [
    "DEFAULT_ASF_CACHE_DIR",
    "DEFAULT_CACHE_DIR",
    "DEFAULT_CDSE_CACHE_DIR",
    "DEFAULT_PGC_CACHE_DIR",
    "DEFAULT_PIPELINE_CACHE_DIR",
    "REGISTRY",
    "QueryCache",
    "SENTINEL_PY_HOME",
    "SENTINEL_PY_HOME_ENV",
    "CacheEntry",
    "CacheNamespace",
    "CacheVersionError",
    "CompactResult",
    "PruneResult",
    "RowStore",
    "RowWriter",
    "cache_directory",
    "compact_stores",
    "delete_entries",
    "deterministic_cache_key",
    "find_latest_cache_file",
    "legacy_provider_cache_root",
    "list_entries",
    "mark_cache_used",
    "merge_parquet_cache",
    "merge_state_rows",
    "select_prunable",
    "write_json_atomic",
    "write_parquet_atomic",
]
