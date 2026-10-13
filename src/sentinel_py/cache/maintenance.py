"""Inspect, compact, and prune the sentinel-py caches.

The cache layout is fixed and small: keyed query directories under each provider's
cache root, cached point-sample tables under the pipeline cache root, and keyed-row
stores (:class:`~sentinel_py.cache.rowstore.RowStore`) declared in
:data:`~sentinel_py.cache.registry.REGISTRY`, either in the home cache or beside
user data in a ``.sentinel-py`` sidecar directory.

Only query directories and point-sample files are ever deleted. Stores can be
inspected and compacted but are never removed, and sidecars are only examined in
directories the caller names explicitly; nothing is discovered by walking a tree.
"""

from __future__ import annotations

import json
import logging
import re
import shutil
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from sentinel_py.cache.registry import REGISTRY
from sentinel_py.cache.rowstore import RowStore
from sentinel_py.enums import CacheKind, CacheLocation, CachePruneTarget

QUERY_KEY_PATTERN = re.compile(r"^[0-9a-f]{32}$")
QUERY_TABLE_NAMES = ("manifest.parquet", "scenes.parquet")
POINT_SAMPLES_NAMESPACE = "point_samples"

LOGGER = logging.getLogger(__name__)

########################################################################################
# Entry model
########################################################################################


@dataclass(frozen=True)
class CacheEntry:
    """One inspectable cache item: a query directory, point-sample file, or store.

    Attributes
    ----------
    namespace : str
        ``<provider>_queries`` for query directories, otherwise a registry name.
    kind : CacheKind
        Invalidation behavior of the namespace.
    path : pathlib.Path
        Directory (query), file (point sample), or base file (store).
    size_bytes : int
        Total size of every file belonging to the entry, including store parts.
    files : int
        Number of files making up the entry.
    rows : int | None
        Row count from Parquet footers, or ``None`` when no table could be read. For
        a store with uncompacted parts this can exceed the deduplicated row count.
    parts : int
        Uncompacted part files of a store; zero for other entries.
    last_used : datetime.datetime
        Most recent modification time among the entry's files.
    prunable : bool
        Whether ``cache prune`` may delete the entry.
    description : str
        Short human-readable summary.
    store : RowStore | None
        The opened store for keyed-row entries.
    """

    namespace: str
    kind: CacheKind
    path: Path
    size_bytes: int
    files: int
    rows: int | None
    parts: int
    last_used: datetime
    prunable: bool
    description: str
    store: RowStore | None = None


@dataclass(frozen=True)
class CompactResult:
    """Outcome of compacting one store."""

    namespace: str
    path: Path
    parts: int
    folded: int


@dataclass(frozen=True)
class PruneResult:
    """Outcome of deleting prunable entries."""

    deleted: tuple[CacheEntry, ...]
    failed: tuple[tuple[CacheEntry, str], ...]


########################################################################################
# Inspection helpers
########################################################################################


def _parquet_rows(paths: Iterable[Path]) -> int | None:
    """Sum Parquet footer row counts, or ``None`` if any file is unreadable."""
    total = 0
    for path in paths:
        try:
            total += pq.ParquetFile(path).metadata.num_rows
        except (OSError, pa.ArrowException):
            return None
    return total


def _file_totals(paths: Sequence[Path]) -> tuple[int, int, datetime]:
    """Return the combined size, file count, and newest mtime of existing files."""
    size = 0
    files = 0
    newest = 0.0
    for path in paths:
        try:
            stat = path.stat()
        except OSError:
            continue
        size += stat.st_size
        files += 1
        newest = max(newest, stat.st_mtime)
    return size, files, datetime.fromtimestamp(newest)


def _query_description(info_path: Path) -> str:
    """Summarize a query directory from its ``query_info.json``."""
    try:
        info = json.loads(info_path.read_text())
    except (OSError, ValueError):
        return ""
    parts: list[str] = []
    for key in ("num_scenes", "num_products", "num_tiles"):
        if key in info:
            parts.append(f"{info[key]} {key.removeprefix('num_')}")
            break
    for key in ("collection", "product", "years", "period"):
        if info.get(key) not in (None, "", []):
            parts.append(f"{key}={info[key]}")
    return ", ".join(parts)


########################################################################################
# Discovery
########################################################################################


def _query_entries(provider: CachePruneTarget, provider_dir: Path) -> list[CacheEntry]:
    """Return one entry per keyed query directory under a provider cache root."""
    entries: list[CacheEntry] = []
    if not provider_dir.is_dir():
        return entries
    for directory in sorted(provider_dir.iterdir()):
        if not (directory.is_dir() and QUERY_KEY_PATTERN.match(directory.name)):
            continue
        tables = [
            directory / name for name in QUERY_TABLE_NAMES if (directory / name).exists()
        ]
        if not tables:
            continue
        members = [path for path in directory.iterdir() if path.is_file()]
        size, files, _ = _file_totals(members)
        _, _, last_used = _file_totals(tables)
        entries.append(
            CacheEntry(
                namespace=f"{provider.value}_queries",
                kind=CacheKind.mutable,
                path=directory,
                size_bytes=size,
                files=files,
                rows=_parquet_rows(tables),
                parts=0,
                last_used=last_used,
                prunable=True,
                description=_query_description(directory / "query_info.json"),
            )
        )
    return entries


def _point_sample_entries(samples_dir: Path) -> list[CacheEntry]:
    """Return one entry per cached point-sample table (``<reader>/<key>.parquet``)."""
    entries: list[CacheEntry] = []
    if not samples_dir.is_dir():
        return entries
    for path in sorted(samples_dir.glob("*/*.parquet")):
        size, files, last_used = _file_totals([path])
        entries.append(
            CacheEntry(
                namespace=POINT_SAMPLES_NAMESPACE,
                kind=CacheKind.derived,
                path=path,
                size_bytes=size,
                files=files,
                rows=_parquet_rows([path]),
                parts=0,
                last_used=last_used,
                prunable=True,
                description=f"reader={path.parent.name}",
            )
        )
    return entries


def _store_entry(namespace: str, kind: CacheKind, store: RowStore) -> CacheEntry | None:
    """Describe a store, or return ``None`` when it has neither base nor parts."""
    base = [store.path] if store.path.is_file() else []
    parts = store.part_paths()
    if not base and not parts:
        return None
    members = [*base, *parts]
    size, files, last_used = _file_totals(members)
    return CacheEntry(
        namespace=namespace,
        kind=kind,
        path=store.path,
        size_bytes=size,
        files=files,
        rows=_parquet_rows(members),
        parts=len(parts),
        last_used=last_used,
        prunable=False,
        description=REGISTRY[namespace].description,
        store=store,
    )


def list_entries(
    cache_dir: Path, data_dirs: Sequence[Path] = ()
) -> list[CacheEntry]:
    """Enumerate cache entries in the home cache and the named sidecar directories.

    Parameters
    ----------
    cache_dir : pathlib.Path
        sentinel-py cache root containing ``asf/``, ``cdse/``, ``pgc/`` and
        ``pipeline/``.
    data_dirs : Sequence[pathlib.Path], optional
        Data or output directories whose ``.sentinel-py`` sidecar state should be
        included. Only that one directory is examined; nothing is searched.

    Returns
    -------
    list[CacheEntry]
        Entries ordered by namespace and path. Unreadable tables are reported with
        ``rows=None`` instead of raising.
    """
    cache_dir = Path(cache_dir)
    entries: list[CacheEntry] = []
    for provider in (CachePruneTarget.asf, CachePruneTarget.cdse, CachePruneTarget.pgc):
        entries.extend(_query_entries(provider, cache_dir / provider.value))

    for name, namespace in REGISTRY.items():
        if namespace.location is CacheLocation.home and namespace.default_root:
            root = cache_dir / namespace.default_root.name
            if namespace.key_columns:
                entry = _store_entry(name, namespace.kind, namespace.store(root))
                if entry is not None:
                    entries.append(entry)
            elif name == POINT_SAMPLES_NAMESPACE:
                entries.extend(_point_sample_entries(namespace.path(root)))

    for data_dir in data_dirs:
        for name, namespace in REGISTRY.items():
            if namespace.location is CacheLocation.home or not namespace.key_columns:
                continue
            entry = _store_entry(name, namespace.kind, namespace.store(Path(data_dir)))
            if entry is not None:
                entries.append(entry)
    return sorted(entries, key=lambda entry: (entry.namespace, str(entry.path)))


########################################################################################
# Compaction
########################################################################################


def compact_stores(
    entries: Iterable[CacheEntry],
    *,
    dry_run: bool = False,
    logger: logging.Logger | None = None,
) -> list[CompactResult]:
    """Fold uncompacted parts of every store entry into its base file.

    Parameters
    ----------
    entries : Iterable[CacheEntry]
        Entries from :func:`list_entries`; non-store entries are ignored.
    dry_run : bool, default False
        Report what would be folded without touching any file.
    logger : logging.Logger, optional
        Logger for compaction records; defaults to the module logger.

    Returns
    -------
    list[CompactResult]
        One result per store that has parts. ``folded`` is zero for a dry run.
    """
    logger = logger or LOGGER
    results: list[CompactResult] = []
    for entry in entries:
        if entry.store is None or entry.parts == 0:
            continue
        folded = 0 if dry_run else entry.store.compact()
        if folded:
            logger.info("Compacted %d part(s) into %s", folded, entry.path)
        results.append(CompactResult(entry.namespace, entry.path, entry.parts, folded))
    return results


########################################################################################
# Pruning
########################################################################################


def select_prunable(
    entries: Iterable[CacheEntry],
    *,
    older_than_days: int,
    providers: Iterable[CachePruneTarget] = (),
    now: datetime | None = None,
) -> list[CacheEntry]:
    """Choose prunable entries last used more than ``older_than_days`` ago.

    Parameters
    ----------
    entries : Iterable[CacheEntry]
        Entries from :func:`list_entries`.
    older_than_days : int
        Keep entries used within this many days; ``0`` selects every prunable entry.
    providers : Iterable[CachePruneTarget], optional
        Restrict the selection to these areas; all prunable areas when empty.
    now : datetime.datetime, optional
        Reference time (defaults to the current time).

    Returns
    -------
    list[CacheEntry]
        Entries safe to delete. Stores and sidecars are never selected.
    """
    cutoff = (now or datetime.now()) - timedelta(days=older_than_days)
    wanted = {
        POINT_SAMPLES_NAMESPACE
        if provider is CachePruneTarget.point_samples
        else f"{provider.value}_queries"
        for provider in providers
    }
    return [
        entry
        for entry in entries
        if entry.prunable
        and entry.last_used <= cutoff
        and (not wanted or entry.namespace in wanted)
    ]


def delete_entries(
    entries: Iterable[CacheEntry],
    cache_dir: Path,
    *,
    logger: logging.Logger | None = None,
) -> PruneResult:
    """Delete prunable entries that lie inside ``cache_dir``.

    Parameters
    ----------
    entries : Iterable[CacheEntry]
        Entries to delete, normally from :func:`select_prunable`.
    cache_dir : pathlib.Path
        Cache root every deleted path must resolve inside.
    logger : logging.Logger, optional
        Logger for deletion records; defaults to the module logger.

    Returns
    -------
    PruneResult
        Deleted entries and, for each entry that could not be deleted, why.

    Raises
    ------
    ValueError
        If an entry is not marked prunable.
    """
    logger = logger or LOGGER
    root = Path(cache_dir).resolve()
    deleted: list[CacheEntry] = []
    failed: list[tuple[CacheEntry, str]] = []
    for entry in entries:
        if not entry.prunable:
            raise ValueError(f"Refusing to delete non-prunable cache entry: {entry.path}")
        try:
            if entry.path.is_symlink():
                raise OSError("refusing to delete a symbolic link")
            if not entry.path.resolve().is_relative_to(root):
                raise OSError(f"path is outside the cache root {root}")
            if entry.path.is_dir():
                shutil.rmtree(entry.path)
            else:
                entry.path.unlink()
        except OSError as error:
            logger.error("Could not delete %s: %s", entry.path, error)
            failed.append((entry, str(error)))
        else:
            logger.info("Deleted cache entry %s", entry.path)
            deleted.append(entry)
    return PruneResult(tuple(deleted), tuple(failed))
