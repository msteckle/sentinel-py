"""Shared paths and helpers for reusable caches and persistent processing state."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import uuid
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from fcntl import LOCK_EX, LOCK_UN, flock
from pathlib import Path
from typing import Any, BinaryIO

import pandas as pd

SENTINEL_PY_HOME_ENV = "SENTINEL_PY_HOME"
SENTINEL_PY_HOME = Path(
    os.environ.get(SENTINEL_PY_HOME_ENV, Path.home() / ".sentinel-py")
).expanduser()
DEFAULT_CACHE_DIR = SENTINEL_PY_HOME / "cache"
DEFAULT_ASF_CACHE_DIR = DEFAULT_CACHE_DIR / "asf"
DEFAULT_CDSE_CACHE_DIR = DEFAULT_CACHE_DIR / "cdse"


def deterministic_cache_key(payload: dict[str, Any]) -> str:
    """Return the stable MD5 key historically used by sentinel-py caches."""
    serialized = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.md5(serialized.encode()).hexdigest()


def cache_directory(cache_root: Path, cache_key: str) -> Path:
    """Return one query directory, migrating its former working-directory cache."""
    cache_root = Path(cache_root)
    directory = cache_root / cache_key
    directory.mkdir(parents=True, exist_ok=True)

    # Copy a matching legacy query once so existing caches remain immediately usable.
    legacy_root = legacy_provider_cache_root(cache_root)
    legacy_directory = legacy_root / cache_key if legacy_root is not None else None
    if legacy_directory is not None and legacy_directory.is_dir():
        for name in ("manifest.parquet", "scenes.parquet", "query_info.json"):
            source = legacy_directory / name
            destination = directory / name
            if source.is_file() and not destination.exists():
                temporary = _temporary_sibling(destination)
                try:
                    shutil.copy2(source, temporary)
                    os.replace(temporary, destination)
                finally:
                    temporary.unlink(missing_ok=True)
    return directory


def find_latest_cache_file(cache_root: Path, filename: str) -> Path | None:
    """Return the most recently used matching file from keyed cache directories."""
    cache_root = Path(cache_root)
    candidates = sorted(
        cache_root.glob(f"*/{filename}"),
        key=lambda path: path.stat().st_mtime,
    )
    if not candidates:
        legacy_root = legacy_provider_cache_root(cache_root)
        legacy_candidates = (
            sorted(
                legacy_root.glob(f"*/{filename}"),
                key=lambda path: path.stat().st_mtime,
            )
            if legacy_root is not None
            else []
        )
        if legacy_candidates:
            latest = legacy_candidates[-1]
            migrated = cache_directory(cache_root, latest.parent.name) / filename
            candidates = [migrated] if migrated.is_file() else []
    return candidates[-1] if candidates else None


def legacy_provider_cache_root(cache_root: Path) -> Path | None:
    """Return the former working-directory cache for a default provider cache."""
    cache_root = Path(cache_root).expanduser()
    if cache_root == DEFAULT_ASF_CACHE_DIR:
        return Path(".asf-cache")
    if cache_root == DEFAULT_CDSE_CACHE_DIR:
        return Path(".cdse-cache")
    return None


def mark_cache_used(path: Path) -> None:
    """Best-effort mtime update so latest-cache discovery follows the latest query."""
    try:
        path.touch(exist_ok=True)
    except OSError:
        pass


def _temporary_sibling(path: Path) -> Path:
    return path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")


@contextmanager
def _cache_file_lock(path: Path) -> Iterator[BinaryIO]:
    """Serialize read-modify-write updates to one shared cache file."""
    lock_path = path.with_name(f".{path.name}.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+b") as lock_stream:
        flock(lock_stream.fileno(), LOCK_EX)
        try:
            yield lock_stream
        finally:
            flock(lock_stream.fileno(), LOCK_UN)


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


def merge_parquet_cache(
    path: Path,
    rows: pd.DataFrame,
    *,
    key_columns: list[str],
    read_only: bool = False,
) -> pd.DataFrame:
    """Merge dataframe rows into a keyed Parquet cache and write it atomically.

    Parameters
    ----------
    path
        Parquet cache to create or update.
    rows
        New cache rows. Empty frames leave an existing cache unchanged.
    key_columns
        Columns that uniquely identify a row; newer values replace older values.
    read_only
        Mark the completed cache file read-only after writing.

    Returns
    -------
    pandas.DataFrame
        The complete merged cache, or the existing cache when ``rows`` is empty.

    Examples
    --------
    ``merge_parquet_cache(path, scenes, key_columns=["Id"])``
    """
    path = Path(path)
    with _cache_file_lock(path):
        existing = pd.read_parquet(path) if path.is_file() else pd.DataFrame()
        if rows.empty:
            return existing

        # Reuse the same deterministic merge behavior as output-scoped state files.
        merged = merge_state_rows(
            existing,
            (
                {str(key): value for key, value in row.items()}
                for row in rows.to_dict("records")
            ),
            key_columns=key_columns,
        )
        write_parquet_atomic(merged, path, index=False, read_only=read_only)
        return merged
