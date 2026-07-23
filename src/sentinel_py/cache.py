"""Shared helpers for deterministic query caches and persistent download state."""

from __future__ import annotations

import hashlib
import json
import os
import stat
import uuid
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

DEFAULT_ASF_CACHE_DIR = Path(".asf-cache")
DEFAULT_CDSE_CACHE_DIR = Path(".cdse-cache")


def deterministic_cache_key(payload: dict[str, Any]) -> str:
    """Return the stable MD5 key historically used by sentinel-py caches."""
    serialized = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.md5(serialized.encode()).hexdigest()


def cache_directory(cache_root: Path, cache_key: str) -> Path:
    """Return and create the directory for one deterministic query key."""
    directory = Path(cache_root) / cache_key
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def find_latest_cache_file(cache_root: Path, filename: str) -> Path | None:
    """Return the most recently used matching file from keyed cache directories."""
    candidates = sorted(
        Path(cache_root).glob(f"*/{filename}"),
        key=lambda path: path.stat().st_mtime,
    )
    return candidates[-1] if candidates else None


def mark_cache_used(path: Path) -> None:
    """Best-effort mtime update so latest-cache discovery follows the latest query."""
    try:
        path.touch(exist_ok=True)
    except OSError:
        pass


def _temporary_sibling(path: Path) -> Path:
    return path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")


def write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    """Atomically replace a JSON file so interruption cannot leave partial JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = _temporary_sibling(path)
    try:
        temporary.write_text(json.dumps(value, indent=2, default=str) + "\n")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def write_parquet_atomic(
    dataframe: pd.DataFrame,
    path: Path,
    *,
    index: bool = False,
    read_only: bool = False,
) -> None:
    """Atomically replace a Parquet file, optionally marking it read-only."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = _temporary_sibling(path)
    try:
        dataframe.to_parquet(temporary, index=index)
        os.replace(temporary, path)
        if read_only:
            path.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
    finally:
        temporary.unlink(missing_ok=True)


def write_csv_atomic(
    dataframe: pd.DataFrame,
    path: Path,
    *,
    index: bool = False,
) -> None:
    """Atomically replace a CSV file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = _temporary_sibling(path)
    try:
        dataframe.to_csv(temporary, index=index)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def merge_state_rows(
    existing: pd.DataFrame,
    new_rows: Iterable[dict[str, Any]],
    *,
    key_columns: list[str],
) -> pd.DataFrame:
    """Merge cache rows by key, keeping the latest row for each key."""
    new_dataframe = pd.DataFrame(list(new_rows))
    if new_dataframe.empty:
        return existing
    if existing.empty:
        return new_dataframe.reset_index(drop=True)

    all_columns = list(
        dict.fromkeys([*existing.columns.tolist(), *new_dataframe.columns.tolist()])
    )
    combined = pd.concat(
        [
            existing.reindex(columns=all_columns),
            new_dataframe.reindex(columns=all_columns),
        ],
        ignore_index=True,
    )
    return combined.drop_duplicates(subset=key_columns, keep="last").reset_index(
        drop=True
    )
