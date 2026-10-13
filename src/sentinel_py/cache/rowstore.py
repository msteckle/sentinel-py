"""Keyed-row Parquet tables stored as a base file plus immutable part files.

A :class:`RowStore` replaces the "read the whole file, merge, rewrite it" pattern
for shared caches and state tables. Updates are appended as small immutable part
files, so interruption loses at most one buffered batch, concurrent writers never
rewrite each other's data, and cost per update does not grow with table size.
:meth:`RowStore.compact` folds the parts back into the base file, which keeps the
historical single-file layout readable by older code.
"""

from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from collections.abc import Collection, Iterable
from pathlib import Path
from typing import Any

import pandas as pd

from sentinel_py.cache.base import (
    _cache_file_lock,
    write_json_atomic,
    write_parquet_atomic,
)

PART_PREFIX = "part-"
META_NAME = "_meta.json"
_READ_RETRIES = 3

########################################################################################
# Exceptions
########################################################################################


class CacheVersionError(RuntimeError):
    """Raised when a store on disk was written with another schema version."""


########################################################################################
# Row store
########################################################################################


class RowStore:
    """A keyed-row table made of a base Parquet file and immutable part files.

    Rows are identified by ``key_columns``; when a key appears more than once, the
    row from the newest part wins (parts are ordered by name, which embeds a
    nanosecond timestamp). The base file is read first, so any part overrides it.

    Parameters
    ----------
    path : pathlib.Path
        The base Parquet file. Parts live in ``<path>.parts/``.
    key_columns : list[str]
        Columns that uniquely identify a row.
    version : int, default 1
        Schema/semantics version recorded beside the parts. Opening parts written
        with a different version raises :class:`CacheVersionError`.
    read_only : bool, default False
        Mark the base file and part files read-only after writing.
    logger : logging.Logger, optional
        Logger for compaction decisions.

    Notes
    -----
    Several workers may call :meth:`append` concurrently. Mutable tables should
    still have a single logical owner, because "newest wins" is decided by part
    name and node clocks may differ slightly on a shared filesystem.

    Examples
    --------
    >>> store = RowStore(Path("assets.parquet"), ["safedir", "path"])  # doctest: +SKIP
    >>> with store.writer(batch_rows=1000) as writer:  # doctest: +SKIP
    ...     writer.add([{"safedir": "a", "path": "b"}])
    """

    def __init__(
        self,
        path: Path,
        key_columns: list[str],
        *,
        version: int = 1,
        read_only: bool = False,
        logger: logging.Logger | None = None,
    ) -> None:
        if not key_columns:
            raise ValueError("A RowStore requires at least one key column")
        self.path = Path(path)
        self.key_columns = list(key_columns)
        self.version = version
        self.read_only = read_only
        self.logger = logger or logging.getLogger(__name__)

    @property
    def parts_dir(self) -> Path:
        """Return the directory holding this store's part files."""
        return self.path.with_name(f"{self.path.name}.parts")

    def part_paths(self) -> list[Path]:
        """Return existing part files, oldest first."""
        if not self.parts_dir.is_dir():
            return []
        return sorted(self.parts_dir.glob(f"{PART_PREFIX}*.parquet"))

    def _check_version(self) -> None:
        """Raise when existing parts were written with a different version."""
        meta = self.parts_dir / META_NAME
        if not meta.is_file():
            return
        try:
            recorded = json.loads(meta.read_text()).get("version")
        except (OSError, ValueError):
            return
        if recorded != self.version:
            raise CacheVersionError(
                f"{self.parts_dir} was written with cache version {recorded}, "
                f"but version {self.version} is required"
            )

    def _read_files(
        self,
        files: list[Path],
        columns: list[str] | None,
        key_filters: dict[str, Collection[Any]] | None = None,
    ) -> pd.DataFrame:
        """Read and deduplicate files in order, newest row per key winning."""
        wanted = None
        if columns is not None:
            wanted = list(dict.fromkeys([*self.key_columns, *columns]))
        filters = None
        if key_filters:
            unknown = set(key_filters) - set(self.key_columns)
            if unknown:
                raise ValueError(
                    "key_filters may only use key columns (all versions of a key "
                    f"share them); got: {', '.join(sorted(unknown))}"
                )
            if any(len(values) == 0 for values in key_filters.values()):
                return pd.DataFrame(columns=wanted or [])
            filters = [(name, "in", list(values)) for name, values in key_filters.items()]
        frames = []
        for file in files:
            frame = pd.read_parquet(file, columns=wanted, filters=filters)
            if not frame.empty:
                frames.append(frame)
        if not frames:
            return pd.DataFrame(columns=wanted or [])
        combined = frames[0] if len(frames) == 1 else pd.concat(frames, ignore_index=True)
        combined = combined.drop_duplicates(subset=self.key_columns, keep="last")
        combined = combined.reset_index(drop=True)
        return combined if columns is None else combined[list(columns)]

    def read(
        self,
        columns: list[str] | None = None,
        *,
        key_filters: dict[str, Collection[Any]] | None = None,
    ) -> pd.DataFrame:
        """Return the deduplicated table.

        Parameters
        ----------
        columns : list[str], optional
            Columns to return; reading fewer columns is much cheaper. All columns
            are returned when omitted.
        key_filters : dict[str, Collection], optional
            Keep only rows whose key column is in the given values, e.g.
            ``{"safedir": scene_names}``. Only key columns are allowed because
            every version of a row shares them, so filtering cannot resurrect a
            superseded row.

        Returns
        -------
        pandas.DataFrame
            One row per key; empty when the store has no data.
        """
        self._check_version()
        for attempt in range(_READ_RETRIES):
            parts = self.part_paths()
            files = ([self.path] if self.path.is_file() else []) + parts
            try:
                return self._read_files(files, columns, key_filters)
            except FileNotFoundError:
                # A concurrent compaction folded parts into the base file.
                if attempt == _READ_RETRIES - 1:
                    raise
        raise AssertionError("unreachable")  # pragma: no cover

    def fingerprint(self) -> tuple | None:
        """Return a hashable description of the store's current files.

        The base file entry has the same ``(path, size, mtime_ns)`` shape that
        callers used before parts existed, so a compacted store yields the same
        value as the single file did. Part files are appended only while some
        exist, so state held in uncompacted parts still changes the fingerprint.

        Returns
        -------
        tuple | None
            ``None`` when the store has no files.
        """
        entries = []
        if self.path.is_file():
            stat = self.path.stat()
            entries.append((str(self.path), stat.st_size, stat.st_mtime_ns))
        for part in self.part_paths():
            try:
                stat = part.stat()
            except FileNotFoundError:  # compacted concurrently
                continue
            entries.append((part.name, stat.st_size, stat.st_mtime_ns))
        if not entries:
            return None
        return entries[0] if len(entries) == 1 else tuple(entries)

    def keys(self) -> pd.DataFrame:
        """Return only the key columns of every stored row."""
        return self.read(columns=self.key_columns)

    def append(self, rows: pd.DataFrame | Iterable[dict[str, Any]]) -> int:
        """Atomically write rows as one new part file.

        Parameters
        ----------
        rows : pandas.DataFrame | Iterable[dict]
            Rows to add. Rows passed in one call always land in the same part.

        Returns
        -------
        int
            Number of rows written.
        """
        frame = rows if isinstance(rows, pd.DataFrame) else pd.DataFrame(list(rows))
        if frame.empty:
            return 0
        self._check_version()
        self.parts_dir.mkdir(parents=True, exist_ok=True)
        meta = self.parts_dir / META_NAME
        if not meta.exists():
            write_json_atomic(
                meta, {"version": self.version, "key_columns": self.key_columns}
            )
        name = f"{PART_PREFIX}{time.time_ns():020d}-{uuid.uuid4().hex[:8]}.parquet"
        write_parquet_atomic(
            frame, self.parts_dir / name, index=False, read_only=self.read_only
        )
        return len(frame)

    def compact(self) -> int:
        """Fold all current parts into the base file and remove them.

        Parts written while compaction runs are not in the snapshot and survive.

        Returns
        -------
        int
            Number of part files folded into the base file.
        """
        with _cache_file_lock(self.path):
            parts = self.part_paths()
            if not parts:
                return 0
            files = ([self.path] if self.path.is_file() else []) + parts
            merged = self._read_files(files, None)
            write_parquet_atomic(
                merged, self.path, index=False, read_only=self.read_only
            )
            for part in parts:
                part.unlink(missing_ok=True)
            self.logger.info(
                "Compacted %d part(s) into %s (%d rows)", len(parts), self.path, len(merged)
            )
            return len(parts)

    def writer(
        self,
        *,
        batch_rows: int = 5000,
        flush_seconds: float = 60.0,
        compact_on_close: bool = True,
    ) -> RowWriter:
        """Return a buffered writer that checkpoints rows in batches.

        Parameters
        ----------
        batch_rows : int, default 5000
            Flush once this many rows are buffered.
        flush_seconds : float, default 60.0
            Flush when this much time passed since the last flush.
        compact_on_close : bool, default True
            Compact parts into the base file after a clean exit.
        """
        return RowWriter(
            self,
            batch_rows=batch_rows,
            flush_seconds=flush_seconds,
            compact_on_close=compact_on_close,
        )


########################################################################################
# Buffered writer
########################################################################################


class RowWriter:
    """Buffer rows and flush them to a :class:`RowStore` in atomic batches.

    Use as a context manager. Buffered rows are flushed on exit even when the body
    raises (including ``KeyboardInterrupt``), so an interrupted run keeps all work
    completed before the interruption. The writer is safe to share between threads.
    """

    def __init__(
        self,
        store: RowStore,
        *,
        batch_rows: int,
        flush_seconds: float,
        compact_on_close: bool,
    ) -> None:
        self.store = store
        self.batch_rows = batch_rows
        self.flush_seconds = flush_seconds
        self.compact_on_close = compact_on_close
        self.rows_written = 0
        self._buffer: list[dict[str, Any]] = []
        self._lock = threading.Lock()
        self._last_flush = time.monotonic()

    def add(self, rows: pd.DataFrame | Iterable[dict[str, Any]]) -> None:
        """Buffer rows; rows from one call are never split across parts."""
        records = (
            rows.to_dict("records") if isinstance(rows, pd.DataFrame) else list(rows)
        )
        if not records:
            return
        with self._lock:
            self._buffer.extend(records)
            due = (
                len(self._buffer) >= self.batch_rows
                or time.monotonic() - self._last_flush >= self.flush_seconds
            )
            if due:
                self._flush_locked()

    def flush(self) -> int:
        """Write any buffered rows as one part and return how many were written."""
        with self._lock:
            return self._flush_locked()

    def _flush_locked(self) -> int:
        """Write the buffered rows as one part; the caller must hold the lock."""
        buffered, self._buffer = self._buffer, []
        self._last_flush = time.monotonic()
        if not buffered:
            return 0
        try:
            written = self.store.append(buffered)
        except BaseException:
            # Keep the rows so a retrying caller does not lose them.
            self._buffer = buffered + self._buffer
            raise
        self.rows_written += written
        return written

    def __enter__(self) -> RowWriter:
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        try:
            self.flush()
        finally:
            if exc_type is None and self.compact_on_close:
                self.store.compact()
